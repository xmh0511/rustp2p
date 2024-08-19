use crate::error::RecvError;
use crate::pipe::config::TcpPipeConfig;
use crate::route::{Index, RouteKey};
use crate::socket::{create_tcp0, create_tcp_listener, LocalInterface};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use dashmap::DashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

pub struct TcpPipe<D, E> {
    route_idle_time: Duration,
    tcp_listener: TcpListener,
    connect_receiver: Receiver<(RouteKey, OwnedReadHalf, WriteHalfBox<E>)>,
    tcp_pipe_writer: TcpPipeWriter<E>,
    write_half_collect: Arc<WriteHalfCollect<E>>,
    decoder: D,
}

impl<D, E> TcpPipe<D, E> {
    /// Construct a `TCP` pipe with the specified configuration
    pub fn new(config: TcpPipeConfig<D, E>) -> anyhow::Result<TcpPipe<D, E>> {
        config.check()?;
        let address: SocketAddr = if config.use_v6 {
            format!("[::]:{}", config.tcp_port).parse().unwrap()
        } else {
            format!("0.0.0.0:{}", config.tcp_port).parse().unwrap()
        };

        let tcp_listener = create_tcp_listener(address)?;
        let local_addr = tcp_listener.local_addr()?;
        let tcp_listener = TcpListener::from_std(tcp_listener)?;
        let (connect_sender, connect_receiver) = tokio::sync::mpsc::channel(64);
        let write_half_collect = Arc::new(WriteHalfCollect::new(config.tcp_multiplexing_limit));
        let tcp_pipe_writer = TcpPipeWriter {
            socket_layer: Arc::new(SocketLayer::new(
                local_addr,
                config.tcp_multiplexing_limit,
                write_half_collect.clone(),
                connect_sender,
                config.default_interface,
                config.encoder,
            )),
        };
        Ok(TcpPipe {
            route_idle_time: config.route_idle_time,
            tcp_listener,
            connect_receiver,
            tcp_pipe_writer,
            write_half_collect,
            decoder: config.decoder,
        })
    }
    #[inline]
    pub fn writer_ref(&self) -> TcpPipeWriterRef<'_, E> {
        TcpPipeWriterRef {
            shadow: &self.tcp_pipe_writer,
        }
    }
}

impl<D: Decoder, E: Encoder> TcpPipe<D, E> {
    /// Accept `TCP` pipelines from this kind pipe
    pub async fn accept(&mut self) -> anyhow::Result<TcpPipeLine<D, E>> {
        tokio::select! {
            rs=self.connect_receiver.recv()=>{
                let (route_key,read_half,write_half) = rs.context("connect_receiver done")?;
                Ok(TcpPipeLine::new(self.route_idle_time,route_key,read_half,write_half,self.write_half_collect.clone(),self.decoder.clone()))
            }
            rs=self.tcp_listener.accept()=>{
                let (tcp_stream,_addr) = rs?;
                let route_key = tcp_stream.route_key()?;
                let (read_half,write_half) = tcp_stream.into_split();
                let write_half = WriteHalfBox::new(write_half,self.tcp_pipe_writer.encoder.clone());
                self.write_half_collect.add_write_half(route_key,0, write_half.clone());
                Ok(TcpPipeLine::new(self.route_idle_time,route_key,read_half,write_half,self.write_half_collect.clone(),self.decoder.clone()))
            }
        }
    }
}

pub struct TcpPipeLine<D, E> {
    route_idle_time: Duration,
    tcp_read: OwnedReadHalf,
    tcp_write: WriteHalfBox<E>,
    stream_owned: StreamOwned<E>,
    decoder: D,
}

impl<D, E> TcpPipeLine<D, E> {
    pub(crate) fn new(
        route_idle_time: Duration,
        route_key: RouteKey,
        tcp_read: OwnedReadHalf,
        tcp_write: WriteHalfBox<E>,
        write_half_collect: Arc<WriteHalfCollect<E>>,
        decoder: D,
    ) -> Self {
        let stream_owned = StreamOwned {
            route_key,
            write_half_collect,
        };
        Self {
            route_idle_time,
            tcp_read,
            tcp_write,
            stream_owned,
            decoder,
        }
    }
    #[inline]
    pub fn route_key(&self) -> RouteKey {
        self.stream_owned.route_key
    }
    /// Close this pipeline
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let mut guard = self.tcp_write.lock().await;
        if let Some((write, _)) = guard.as_mut() {
            write.shutdown().await?;
        }
        Ok(())
    }
    pub async fn readable(&self) -> anyhow::Result<()> {
        self.tcp_read.readable().await?;
        Ok(())
    }
}
impl<D, E> TcpPipeLine<D, E> {
    pub async fn into_raw(self) -> anyhow::Result<(OwnedWriteHalf, OwnedReadHalf)> {
        let option = self.tcp_write.lock().await.take();
        if let Some((write_half, _)) = option {
            Ok((write_half, self.tcp_read))
        } else {
            Err(anyhow!("miss"))
        }
    }
}
impl<D: Decoder, E: Encoder> TcpPipeLine<D, E> {
    /// Writing `buf` to the target denoted by `route_key` via this pipeline
    pub async fn send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<usize> {
        if &self.stream_owned.route_key != route_key {
            Err(anyhow!("mismatch"))?
        }
        let mut guard = self.tcp_write.lock().await;
        if let Some((write, encoder)) = guard.as_mut() {
            let len = encoder.encode(write, buf).await?;
            Ok(len)
        } else {
            Err(anyhow!("miss"))
        }
    }

    pub(crate) async fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match tokio::time::timeout(
            self.route_idle_time,
            self.decoder.decode(&mut self.tcp_read, buf),
        )
        .await
        {
            Ok(rs) => rs,
            Err(_) => Err(io::Error::from(io::ErrorKind::TimedOut)),
        }
    }
    /// Receive bytes from this pipeline, which the configured Decoder pre-processes
    /// `usize` in the `Ok` branch indicates how many bytes are received
    /// `RouteKey` in the `Ok` branch denotes the source where these bytes are received from
    pub async fn recv_from(&mut self, buf: &mut [u8]) -> Result<(usize, RouteKey), RecvError> {
        let len = self.recv(buf).await?;
        Ok((len, self.route_key()))
    }
}
struct StreamOwned<E> {
    route_key: RouteKey,
    write_half_collect: Arc<WriteHalfCollect<E>>,
}

impl<E> Drop for StreamOwned<E> {
    fn drop(&mut self) {
        self.write_half_collect.remove(&self.route_key);
    }
}

pub struct WriteHalfCollect<E> {
    tcp_multiplexing_limit: usize,
    addr_mapping: DashMap<SocketAddr, Vec<usize>>,
    write_half_map: DashMap<usize, WriteHalfBox<E>>,
}

impl<E> WriteHalfCollect<E> {
    fn new(tcp_multiplexing_limit: usize) -> Self {
        Self {
            tcp_multiplexing_limit,
            addr_mapping: Default::default(),
            write_half_map: Default::default(),
        }
    }
}

pub(crate) struct WriteHalfBox<E> {
    write_half: Arc<Mutex<Option<(OwnedWriteHalf, E)>>>,
}

impl<E> Deref for WriteHalfBox<E> {
    type Target = Mutex<Option<(OwnedWriteHalf, E)>>;

    fn deref(&self) -> &Self::Target {
        &self.write_half
    }
}

impl<E> Clone for WriteHalfBox<E> {
    fn clone(&self) -> Self {
        Self {
            write_half: self.write_half.clone(),
        }
    }
}

impl<E> WriteHalfBox<E> {
    pub(crate) fn new(write_half: OwnedWriteHalf, encoder: E) -> WriteHalfBox<E> {
        Self {
            write_half: Arc::new(Mutex::new(Some((write_half, encoder)))),
        }
    }
}

impl<E> WriteHalfCollect<E> {
    pub(crate) fn add_write_half(
        &self,
        route_key: RouteKey,
        index: usize,
        write_half: WriteHalfBox<E>,
    ) {
        assert!(index < self.tcp_multiplexing_limit);
        let index_usize = route_key.index_usize();
        let _ref = self
            .addr_mapping
            .entry(route_key.addr())
            .and_modify(|v| {
                v[index] = index_usize;
            })
            .or_insert_with(|| {
                let mut v = vec![0; self.tcp_multiplexing_limit];
                v[index] = index_usize;
                v
            });
        self.write_half_map.insert(index_usize, write_half);
    }
    pub(crate) fn remove(&self, route_key: &RouteKey) {
        let index_usize = route_key.index_usize();
        let mut option = self.addr_mapping.get_mut(&route_key.addr());
        if let Some(index_vec) = option.as_mut() {
            index_vec.retain(|v| *v != index_usize)
        }
        self.write_half_map.remove(&index_usize);
    }
    pub(crate) fn get(&self, index: &usize) -> Option<WriteHalfBox<E>> {
        self.write_half_map.get(index).map(|v| v.value().clone())
    }

    pub(crate) fn get_one_route_key(&self, addr: &SocketAddr) -> Option<RouteKey> {
        if let Some(v) = self.addr_mapping.get(addr) {
            if let Some(index_usize) = v.first() {
                return Some(RouteKey::new(Index::Tcp(*index_usize), *addr));
            }
        }
        None
    }
    pub(crate) fn get_limit_route_key(&self, index: usize, addr: &SocketAddr) -> Option<RouteKey> {
        if let Some(v) = self.addr_mapping.get(addr) {
            assert_eq!(v.len(), self.tcp_multiplexing_limit);
            let index_usize = v[index];
            if index_usize == 0 {
                return None;
            }
            return Some(RouteKey::new(Index::Tcp(index_usize), *addr));
        }
        None
    }
}

pub struct SocketLayer<E> {
    lock: Mutex<()>,
    local_addr: SocketAddr,
    tcp_multiplexing_limit: usize,
    write_half_collect: Arc<WriteHalfCollect<E>>,
    connect_sender: Sender<(RouteKey, OwnedReadHalf, WriteHalfBox<E>)>,
    default_interface: Option<LocalInterface>,
    encoder: E,
}

impl<E> SocketLayer<E> {
    pub(crate) fn new(
        local_addr: SocketAddr,
        tcp_multiplexing_limit: usize,
        write_half_collect: Arc<WriteHalfCollect<E>>,
        connect_sender: Sender<(RouteKey, OwnedReadHalf, WriteHalfBox<E>)>,
        default_interface: Option<LocalInterface>,
        encoder: E,
    ) -> Self {
        Self {
            local_addr,
            lock: Default::default(),
            tcp_multiplexing_limit,
            write_half_collect,
            connect_sender,
            default_interface,
            encoder,
        }
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<E: Encoder> SocketLayer<E> {
    /// Multiple connections can be initiated to the target address.
    pub async fn multi_connect(&self, addr: SocketAddr, index: usize) -> anyhow::Result<RouteKey> {
        if index >= self.tcp_multiplexing_limit {
            return Err(anyhow!(
                "index[{}] cannot exceed tcp_multiplexing_limit[{}]",
                index,
                self.tcp_multiplexing_limit
            ));
        }
        let _guard = self.lock.lock().await;
        if let Some(route_key) = self.write_half_collect.get_limit_route_key(index, &addr) {
            return Ok(route_key);
        }
        self.connect0(0, addr, index).await
    }
    /// Initiate a connection.
    pub async fn connect(&self, addr: SocketAddr) -> anyhow::Result<RouteKey> {
        let _guard = self.lock.lock().await;
        if let Some(route_key) = self.write_half_collect.get_one_route_key(&addr) {
            return Ok(route_key);
        }
        self.connect0(0, addr, 0).await
    }
    /// Reuse the bound port to initiate a connection, which can be used to penetrate NAT1 network type.
    pub async fn connect_reuse_port(&self, addr: SocketAddr) -> anyhow::Result<RouteKey> {
        let _guard = self.lock.lock().await;
        if let Some(route_key) = self.write_half_collect.get_one_route_key(&addr) {
            return Ok(route_key);
        }
        self.connect0(self.local_addr.port(), addr, 0).await
    }
    async fn connect0(
        &self,
        bind_port: u16,
        addr: SocketAddr,
        index: usize,
    ) -> anyhow::Result<RouteKey> {
        let socket = create_tcp0(addr.is_ipv4(), bind_port, self.default_interface.as_ref())?;
        let stream = socket.connect(addr).await?;
        let route_key = stream.route_key()?;
        let (read_half, write_half) = stream.into_split();
        let write_half = WriteHalfBox::new(write_half, self.encoder.clone());

        if let Err(e) = self
            .connect_sender
            .send((route_key, read_half, write_half.clone()))
            .await
        {
            Err(anyhow!("{e}"))?
        }
        self.write_half_collect
            .add_write_half(route_key, index, write_half);
        Ok(route_key)
    }
}

impl<E: Encoder> TcpPipeWriter<E> {
    pub async fn send_to_addr_multi<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<usize> {
        let route_key = self.multi_connect(addr.into(), 0).await?;
        self.send_to(buf, &route_key).await
    }
    /// Reuse the bound port to initiate a connection, which can be used to penetrate NAT1 network type.
    pub async fn send_to_addr_reuse_port<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<usize> {
        let route_key = self.connect_reuse_port(addr.into()).await?;
        self.send_to(buf, &route_key).await
    }
    pub async fn send_to_addr<A: Into<SocketAddr>>(
        &self,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<usize> {
        let route_key = self.connect(addr.into()).await?;
        self.send_to(buf, &route_key).await
    }
    /// Writing `buf` to the target denoted by `route_key`
    pub async fn send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<usize> {
        match route_key.index() {
            Index::Tcp(index) => {
                let write_half = self
                    .write_half_collect
                    .get(&index)
                    .with_context(|| format!("not found {route_key:?}"))?;
                let mut guard = write_half.lock().await;
                if let Some((write_half, encoder)) = guard.as_mut() {
                    let len = encoder.encode(write_half, buf).await?;
                    Ok(len)
                } else {
                    Err(anyhow!("miss"))
                }
            }
            _ => Err(anyhow!("invalid protocol")),
        }
    }
    pub async fn get(
        &self,
        addr: SocketAddr,
        index: usize,
    ) -> anyhow::Result<TcpPipeWriterIndex<'_, E>> {
        let route_key = self.multi_connect(addr, index).await?;
        let write_half = self
            .write_half_collect
            .get(&route_key.index_usize())
            .with_context(|| format!("not found with index={index}"))?;

        Ok(TcpPipeWriterIndex {
            shadow: write_half,
            marker: Default::default(),
        })
    }
}

#[derive(Clone)]
pub struct TcpPipeWriter<E> {
    socket_layer: Arc<SocketLayer<E>>,
}

impl<E> Deref for TcpPipeWriter<E> {
    type Target = Arc<SocketLayer<E>>;

    fn deref(&self) -> &Self::Target {
        &self.socket_layer
    }
}

pub struct TcpPipeWriterRef<'a, E> {
    shadow: &'a Arc<SocketLayer<E>>,
}

impl<'a, E> Clone for TcpPipeWriterRef<'a, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, E> Copy for TcpPipeWriterRef<'a, E> {}

impl<'a, E> TcpPipeWriterRef<'a, E> {
    pub fn to_owned(&self) -> TcpPipeWriter<E> {
        TcpPipeWriter {
            socket_layer: self.shadow.clone(),
        }
    }
}

impl<'a, E> Deref for TcpPipeWriterRef<'a, E> {
    type Target = Arc<SocketLayer<E>>;

    fn deref(&self) -> &Self::Target {
        self.shadow
    }
}

pub struct TcpPipeWriterIndex<'a, E: Encoder> {
    shadow: WriteHalfBox<E>,
    marker: PhantomData<&'a ()>,
}

impl<'a, E: Encoder> TcpPipeWriterIndex<'a, E> {
    pub async fn send(&self, buf: &[u8]) -> anyhow::Result<usize> {
        let mut guard = self.shadow.lock().await;
        if let Some((write_half, encoder)) = guard.as_mut() {
            let len = encoder.encode(write_half, buf).await?;
            Ok(len)
        } else {
            Err(anyhow!("miss"))?
        }
    }
}

pub trait TcpStreamIndex {
    fn route_key(&self) -> anyhow::Result<RouteKey>;
    fn index(&self) -> Index;
}

impl TcpStreamIndex for TcpStream {
    fn route_key(&self) -> anyhow::Result<RouteKey> {
        let addr = self.peer_addr()?;

        Ok(RouteKey::new(self.index(), addr))
    }

    fn index(&self) -> Index {
        #[cfg(windows)]
        use std::os::windows::io::AsRawSocket;
        #[cfg(windows)]
        let index = self.as_raw_socket() as usize;
        #[cfg(unix)]
        use std::os::fd::{AsFd, AsRawFd};
        #[cfg(unix)]
        let index = self.as_fd().as_raw_fd() as usize;
        Index::Tcp(index)
    }
}

/// The default byte encoder/decoder; using this is no different from directly using a TCP stream.
#[derive(Copy, Clone, Default)]
pub struct BytesCodec;

/// Fixed-length prefix encoder/decoder.
#[derive(Copy, Clone, Default)]
pub struct LengthPrefixedCodec;

#[async_trait]
impl Decoder for BytesCodec {
    async fn decode(&mut self, read: &mut OwnedReadHalf, src: &mut [u8]) -> io::Result<usize> {
        let len = read.read(src).await?;
        Ok(len)
    }
}

#[async_trait]
impl Encoder for BytesCodec {
    async fn encode(&mut self, write: &mut OwnedWriteHalf, data: &[u8]) -> io::Result<usize> {
        write.write_all(data).await?;
        Ok(data.len())
    }
}
#[async_trait]
impl Decoder for LengthPrefixedCodec {
    async fn decode(&mut self, read: &mut OwnedReadHalf, src: &mut [u8]) -> io::Result<usize> {
        let mut head = [0; 4];
        read.read_exact(&mut head).await?;
        let len = u32::from_be_bytes(head) as usize;
        read.read_exact(&mut src[..len]).await?;
        Ok(len)
    }
}

#[async_trait]
impl Encoder for LengthPrefixedCodec {
    async fn encode(&mut self, write: &mut OwnedWriteHalf, data: &[u8]) -> io::Result<usize> {
        let head: [u8; 4] = (data.len() as u32).to_be_bytes();
        write.write_all(&head).await?;
        write.write_all(data).await?;
        Ok(data.len())
    }
}

#[async_trait]
pub trait Decoder: Clone {
    async fn decode(&mut self, read: &mut OwnedReadHalf, src: &mut [u8]) -> io::Result<usize>;
}

#[async_trait]
pub trait Encoder: Clone + Send + Sync {
    async fn encode(&mut self, write: &mut OwnedWriteHalf, data: &[u8]) -> io::Result<usize>;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::io;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

    use crate::pipe::config::TcpPipeConfig;
    use crate::pipe::tcp_pipe::{Decoder, Encoder, TcpPipe};

    #[tokio::test]
    pub async fn create_tcp_pipe() {
        let config: TcpPipeConfig = TcpPipeConfig::default();
        let tcp_pipe = TcpPipe::new(config).unwrap();
        drop(tcp_pipe)
    }

    #[tokio::test]
    pub async fn create_codec_tcp_pipe() {
        let config = TcpPipeConfig::<MyCodeC, MyCodeC>::default();
        let tcp_pipe = TcpPipe::new(config).unwrap();
        drop(tcp_pipe)
    }

    #[derive(Clone, Default)]
    struct MyCodeC;

    #[async_trait]
    impl Decoder for MyCodeC {
        async fn decode(&mut self, read: &mut OwnedReadHalf, src: &mut [u8]) -> io::Result<usize> {
            let mut head = [0; 2];
            read.read_exact(&mut head).await?;
            let len = u16::from_be_bytes(head) as usize;
            read.read_exact(&mut src[..len]).await?;
            Ok(len)
        }
    }

    #[async_trait]
    impl Encoder for MyCodeC {
        async fn encode(&mut self, write: &mut OwnedWriteHalf, data: &[u8]) -> io::Result<usize> {
            let head: [u8; 2] = (data.len() as u16).to_be_bytes();
            write.write_all(&head).await?;
            write.write_all(data).await?;
            Ok(data.len())
        }
    }
}

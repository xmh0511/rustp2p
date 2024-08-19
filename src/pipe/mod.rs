use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

pub mod config;
pub mod extensible_pipe;
pub mod tcp_pipe;
pub mod udp_pipe;
pub const DEFAULT_ADDRESS_V4: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
pub const DEFAULT_ADDRESS_V6: SocketAddr =
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0));

use std::hash::Hash;

use anyhow::anyhow;

use crate::error::RecvError;
use crate::idle::IdleRouteManager;
use crate::pipe::config::PipeConfig;
use crate::pipe::extensible_pipe::{
    ExtensiblePipe, ExtensiblePipeLine, ExtensiblePipeWriter, ExtensiblePipeWriterRef,
};
use crate::pipe::tcp_pipe::{Decoder, Encoder, TcpPipe, TcpPipeLine, TcpPipeWriter};
use crate::pipe::udp_pipe::{UdpPipe, UdpPipeLine, UdpPipeWriter, UdpPipeWriterRef};
use crate::punch::Puncher;
use crate::route::route_table::RouteTable;
use crate::route::{ConnectProtocol, RouteKey};

use tcp_pipe::TcpPipeWriterRef;

pub type PipeComponent<PeerID, D, E> = (
    Pipe<PeerID, D, E>,
    Puncher<PeerID, E>,
    IdleRouteManager<PeerID>,
);

pub fn pipe<PeerID: Hash + Eq + Clone, D, E>(
    config: PipeConfig<D, E>,
) -> anyhow::Result<PipeComponent<PeerID, D, E>> {
    let route_table = RouteTable::new(config.first_latency, config.multi_pipeline);
    let udp_pipe = if let Some(mut udp_pipe_config) = config.udp_pipe_config {
        udp_pipe_config.main_pipeline_num = config.multi_pipeline;
        Some(UdpPipe::new(udp_pipe_config)?)
    } else {
        None
    };
    let tcp_pipe = if let Some(mut tcp_pipe_config) = config.tcp_pipe_config {
        tcp_pipe_config.tcp_multiplexing_limit = config.multi_pipeline;
        Some(TcpPipe::new(tcp_pipe_config)?)
    } else {
        None
    };
    let extensible_pipe = if config.enable_extend {
        Some(ExtensiblePipe::new())
    } else {
        None
    };
    let pipe = Pipe {
        route_table: route_table.clone(),
        udp_pipe,
        tcp_pipe,
        extensible_pipe,
    };
    let puncher = Puncher::from(&pipe);
    Ok((
        pipe,
        puncher,
        IdleRouteManager::new(config.route_idle_time, route_table),
    ))
}

pub struct Pipe<PeerID, D, E> {
    route_table: RouteTable<PeerID>,
    udp_pipe: Option<UdpPipe>,
    tcp_pipe: Option<TcpPipe<D, E>>,
    extensible_pipe: Option<ExtensiblePipe>,
}

pub enum PipeLine<D, E> {
    Udp(UdpPipeLine),
    Tcp(TcpPipeLine<D, E>),
    Extend(ExtensiblePipeLine),
}

#[derive(Clone)]
pub struct PipeWriter<PeerID, E> {
    route_table: RouteTable<PeerID>,
    udp_pipe_writer: Option<UdpPipeWriter>,
    tcp_pipe_writer: Option<TcpPipeWriter<E>>,
    extensible_pipe_writer: Option<ExtensiblePipeWriter>,
}

pub struct PipeWriterRef<'a, PeerID, E> {
    route_table: &'a RouteTable<PeerID>,
    udp_pipe_writer: Option<UdpPipeWriterRef<'a>>,
    tcp_pipe_writer: Option<TcpPipeWriterRef<'a, E>>,
    extensible_pipe_writer: Option<ExtensiblePipeWriterRef<'a>>,
}

impl<PeerID, D: Decoder, E: Encoder> Pipe<PeerID, D, E> {
    pub async fn accept(&mut self) -> anyhow::Result<PipeLine<D, E>> {
        tokio::select! {
            rs=accept_udp(self.udp_pipe.as_mut())=>{
                 rs
            }
            rs=accept_tcp(self.tcp_pipe.as_mut())=>{
                rs
            }
            rs=accept_extend(self.extensible_pipe.as_mut())=>{
                rs
            }
        }
    }
}
async fn accept_tcp<D: Decoder, E: Encoder>(
    tcp: Option<&mut TcpPipe<D, E>>,
) -> anyhow::Result<PipeLine<D, E>> {
    if let Some(tcp_pipe) = tcp {
        Ok(PipeLine::Tcp(tcp_pipe.accept().await?))
    } else {
        futures::future::pending().await
    }
}
async fn accept_udp<D, E>(udp: Option<&mut UdpPipe>) -> anyhow::Result<PipeLine<D, E>> {
    if let Some(udp_pipe) = udp {
        Ok(PipeLine::Udp(udp_pipe.accept().await?))
    } else {
        futures::future::pending().await
    }
}
async fn accept_extend<D, E>(
    extend: Option<&mut ExtensiblePipe>,
) -> anyhow::Result<PipeLine<D, E>> {
    if let Some(extend) = extend {
        Ok(PipeLine::Extend(extend.accept().await?))
    } else {
        futures::future::pending().await
    }
}

impl<PeerID, D, E> Pipe<PeerID, D, E> {
    pub fn udp_pipe_ref(&mut self) -> Option<&mut UdpPipe> {
        self.udp_pipe.as_mut()
    }
    pub fn tcp_pipe_ref(&mut self) -> Option<&mut TcpPipe<D, E>> {
        self.tcp_pipe.as_mut()
    }

    pub fn route_table(&self) -> &RouteTable<PeerID> {
        &self.route_table
    }
    pub fn writer_ref(&self) -> PipeWriterRef<PeerID, E> {
        PipeWriterRef {
            route_table: &self.route_table,
            udp_pipe_writer: self.udp_pipe.as_ref().map(|v| v.writer_ref()),
            tcp_pipe_writer: self.tcp_pipe.as_ref().map(|v| v.writer_ref()),
            extensible_pipe_writer: self.extensible_pipe.as_ref().map(|v| v.writer_ref()),
        }
    }
}

impl<'a, PeerID, E> PipeWriterRef<'a, PeerID, E> {
    pub fn to_owned(&self) -> PipeWriter<PeerID, E> {
        PipeWriter {
            route_table: self.route_table.clone(),
            udp_pipe_writer: self.udp_pipe_writer.as_ref().map(|v| v.to_owned()),
            tcp_pipe_writer: self.tcp_pipe_writer.as_ref().map(|v| v.to_owned()),
            extensible_pipe_writer: self.extensible_pipe_writer.as_ref().map(|v| v.to_owned()),
        }
    }
    pub fn tcp_pipe_writer_ref(&self) -> Option<TcpPipeWriterRef<'_, E>> {
        self.tcp_pipe_writer
    }
    pub fn udp_pipe_writer_ref(&self) -> Option<UdpPipeWriterRef<'_>> {
        self.udp_pipe_writer
    }
    pub fn extensible_pipe_writer_ref(&self) -> Option<ExtensiblePipeWriterRef<'_>> {
        self.extensible_pipe_writer
    }
}

impl<PeerID, E> PipeWriter<PeerID, E> {
    pub fn udp_pipe_writer(&self) -> Option<&UdpPipeWriter> {
        self.udp_pipe_writer.as_ref()
    }
    pub fn tcp_pipe_writer(&self) -> Option<&TcpPipeWriter<E>> {
        self.tcp_pipe_writer.as_ref()
    }
    pub fn extensible_pipe_writer(&self) -> Option<&ExtensiblePipeWriter> {
        self.extensible_pipe_writer.as_ref()
    }
}

impl<PeerID, E: Encoder> PipeWriter<PeerID, E> {
    pub async fn send_to(&self, buf: &[u8], route_key: &RouteKey) -> anyhow::Result<usize> {
        match route_key.protocol() {
            ConnectProtocol::UDP => {
                if let Some(w) = self.udp_pipe_writer.as_ref() {
                    return w.send_to(buf, route_key).await;
                }
            }
            ConnectProtocol::TCP => {
                if let Some(w) = self.tcp_pipe_writer.as_ref() {
                    return w.send_to(buf, route_key).await;
                }
            }
            ConnectProtocol::WS => {}
            ConnectProtocol::WSS => {}
            ConnectProtocol::Extend => {
                if let Some(w) = self.extensible_pipe_writer.as_ref() {
                    return w.send_to(buf, route_key).await;
                }
            }
        }
        Err(anyhow!("unimplemented"))
    }
    pub async fn send_to_addr<A: Into<SocketAddr>>(
        &self,
        connect_protocol: ConnectProtocol,
        buf: &[u8],
        addr: A,
    ) -> anyhow::Result<usize> {
        match connect_protocol {
            ConnectProtocol::UDP => {
                if let Some(w) = self.udp_pipe_writer.as_ref() {
                    return w.send_to_addr(buf, addr).await;
                }
            }
            ConnectProtocol::TCP => {
                if let Some(w) = self.tcp_pipe_writer.as_ref() {
                    return w.send_to_addr(buf, addr).await;
                }
            }
            ConnectProtocol::WS => {}
            ConnectProtocol::WSS => {}
            ConnectProtocol::Extend => {}
        }
        Err(anyhow!("unimplemented"))
    }
}

impl<PeerID: Hash + Eq, E: Encoder> PipeWriter<PeerID, E> {
    pub async fn send_to_id(&self, buf: &[u8], peer_id: &PeerID) -> anyhow::Result<usize> {
        let route = self.route_table.get_route_by_id(peer_id)?;
        self.send_to(buf, &route.route_key()).await
    }
}

impl<D: Decoder, E: Encoder> PipeLine<D, E> {
    pub async fn recv_from(&mut self, buf: &mut [u8]) -> Result<(usize, RouteKey), RecvError> {
        match self {
            PipeLine::Udp(line) => line.recv_from(buf).await,
            PipeLine::Tcp(line) => line.recv_from(buf).await,
            PipeLine::Extend(line) => line.recv_from(buf).await,
        }
    }
}

impl<D, E> PipeLine<D, E> {
    pub fn protocol(&self) -> ConnectProtocol {
        match self {
            PipeLine::Udp(_) => ConnectProtocol::UDP,
            PipeLine::Tcp(_) => ConnectProtocol::TCP,
            PipeLine::Extend(_) => ConnectProtocol::Extend,
        }
    }
}

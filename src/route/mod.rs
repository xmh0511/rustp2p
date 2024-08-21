use std::net::SocketAddr;

pub mod route_table;

pub const DEFAULT_RT: i64 = 9999;

#[derive(Copy, Clone, Debug)]
pub struct Route {
    index: Index,
    addr: SocketAddr,
    metric: u8,
    rt: i64,
}
impl Route {
    pub fn from(route_key: RouteKey, metric: u8, rt: i64) -> Self {
        Self {
            index: route_key.index,
            addr: route_key.addr,
            metric,
            rt,
        }
    }
    pub fn from_default_rt(route_key: RouteKey, metric: u8) -> Self {
        Self {
            index: route_key.index,
            addr: route_key.addr,
            metric,
            rt: DEFAULT_RT,
        }
    }
    pub fn route_key(&self) -> RouteKey {
        RouteKey {
            index: self.index,
            addr: self.addr,
        }
    }
    pub fn sort_key(&self) -> RouteSortKey {
        RouteSortKey {
            metric: self.metric,
            rt: self.rt,
        }
    }
    pub fn is_p2p(&self) -> bool {
        self.metric <= 1
    }
    pub fn is_relay(&self) -> bool {
        self.metric > 1
    }
}

impl From<(RouteKey, u8)> for Route {
    fn from((key, metric): (RouteKey, u8)) -> Self {
        Route::from_default_rt(key, metric)
    }
}

use crate::pipe::udp_pipe::UDPIndex;
#[non_exhaustive]
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub enum Index {
    Udp(UDPIndex),
    Tcp(usize),
    Extend(usize),
}
impl Index {
    pub fn index(&self) -> usize {
        match self {
            Index::Udp(index) => index.index(),
            Index::Tcp(index) => *index,
            Index::Extend(index) => *index,
        }
    }
    pub fn protocol(&self) -> ConnectProtocol {
        match self {
            Index::Tcp(_) => ConnectProtocol::TCP,
            Index::Udp(_) => ConnectProtocol::UDP,
            Index::Extend(_) => ConnectProtocol::Extend,
        }
    }
}
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct RouteKey {
    index: Index,
    addr: SocketAddr,
}
impl RouteKey {
    pub(crate) const fn new(index: Index, addr: SocketAddr) -> Self {
        Self { index, addr }
    }
    #[inline]
    pub fn protocol(&self) -> ConnectProtocol {
        self.index.protocol()
    }
    #[inline]
    pub fn index(&self) -> Index {
        self.index
    }
    #[inline]
    pub fn index_usize(&self) -> usize {
        self.index.index()
    }
    #[inline]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct RouteSortKey {
    metric: u8,
    rt: i64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ConnectProtocol {
    UDP,
    TCP,
    Extend,
}
impl ConnectProtocol {
    #[inline]
    pub fn is_tcp(&self) -> bool {
        self == &ConnectProtocol::TCP
    }
    #[inline]
    pub fn is_udp(&self) -> bool {
        self == &ConnectProtocol::UDP
    }
}

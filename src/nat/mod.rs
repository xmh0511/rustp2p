use crate::extend::addr::is_ipv6_global;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum NatType {
    Cone,
    Symmetric,
}
impl Default for NatType {
    fn default() -> Self {
        NatType::Cone
    }
}
impl NatType {
    #[inline]
    pub fn is_cone(&self) -> bool {
        self == &NatType::Cone
    }
    #[inline]
    pub fn is_symmetric(&self) -> bool {
        self == &NatType::Symmetric
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NatInfo {
    // nat类型
    pub nat_type: NatType,
    // 公网IP集合
    pub public_ips: Vec<Ipv4Addr>,
    // stun服务发现的端口
    pub public_ports: Vec<u16>,
    // 映射的地址
    pub mapping_tcp_addr: Vec<SocketAddr>,
    pub mapping_udp_addr: Vec<SocketAddr>,
    // 公网端口预测范围，对称网络打洞时有用
    pub public_port_range: u16,
    // 本地IP
    pub local_ipv4: Ipv4Addr,
    // 公网ipv6
    pub ipv6: Option<Ipv6Addr>,
    // 本机监听的udp端口
    pub local_udp_ports: Vec<u16>,
    // 本机监听的tcp端口
    pub local_tcp_port: u16,
    // tcp映射出去的端口 nat1或者没有nat时有用
    pub public_tcp_port: u16,
}
impl NatInfo {
    pub fn ipv6_addr(&self) -> Vec<SocketAddr> {
        if let Some(ipv6) = self.ipv6 {
            if is_ipv6_global(&ipv6) {
                return self
                    .local_udp_ports
                    .iter()
                    .map(|&port| SocketAddrV6::new(ipv6, port, 0, 0).into())
                    .collect();
            }
        }
        vec![]
    }
    pub fn ipv6_tcp_addr(&self) -> Option<SocketAddr> {
        if let Some(ipv6) = self.ipv6 {
            Some(SocketAddrV6::new(ipv6, self.local_tcp_port, 0, 0).into())
        } else {
            None
        }
    }
    pub fn public_ipv4_addr(&self) -> Vec<SocketAddr> {
        if self.public_ips.is_empty() || self.public_ports.is_empty() {
            return vec![];
        }
        if self.public_ips.len() == 1 {
            let ip = self.public_ips[0];
            return self
                .public_ports
                .iter()
                .map(|&port| SocketAddrV4::new(ip, port).into())
                .collect();
        }
        let port = self.public_ports[0];
        self.public_ips
            .iter()
            .map(|&ip| SocketAddrV4::new(ip, port).into())
            .collect()
    }
    pub fn local_ipv4_addrs(&self) -> Vec<SocketAddr> {
        if self.local_ipv4.is_unspecified()
            || self.local_ipv4.is_multicast()
            || self.local_ipv4.is_broadcast()
        {
            return vec![];
        }
        self.local_udp_ports
            .iter()
            .map(|&port| SocketAddrV4::new(self.local_ipv4, port).into())
            .collect()
    }
    pub fn local_ipv4_tcp(&self) -> Option<SocketAddr> {
        if self.local_ipv4.is_unspecified()
            || self.local_ipv4.is_multicast()
            || self.local_ipv4.is_broadcast()
            || self.local_tcp_port == 0
        {
            return None;
        }
        Some(SocketAddrV4::new(self.local_ipv4, self.local_tcp_port).into())
    }
    pub fn public_ipv4_tcp(&self) -> Vec<SocketAddr> {
        if self.public_tcp_port == 0 {
            return vec![];
        }
        self.public_ips
            .iter()
            .map(|&ip| SocketAddrV4::new(ip, self.public_tcp_port).into())
            .collect()
    }
}

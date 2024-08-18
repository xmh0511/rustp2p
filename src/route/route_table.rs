use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_utils::atomic::AtomicCell;
use parking_lot::RwLock;

use crate::route::{Route, RouteKey, DEFAULT_RT};

pub struct RouteTable<PeerID> {
    pub(crate) route_table:
        Arc<RwLock<HashMap<PeerID, (AtomicUsize, Vec<(Route, AtomicCell<Instant>)>)>>>,
    first_latency: bool,
    channel_num: usize,
}
impl<PeerID> Clone for RouteTable<PeerID> {
    fn clone(&self) -> Self {
        Self {
            route_table: self.route_table.clone(),
            first_latency: self.first_latency,
            channel_num: self.channel_num,
        }
    }
}
impl<PeerID> RouteTable<PeerID> {
    pub fn new(first_latency: bool, channel_num: usize) -> RouteTable<PeerID> {
        Self {
            route_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            first_latency,
            channel_num,
        }
    }
}
impl<PeerID: Hash + Eq> RouteTable<PeerID> {
    pub fn get_route_by_id(&self, id: &PeerID) -> io::Result<Route> {
        if let Some((count, v)) = self.route_table.read().get(id) {
            if self.first_latency {
                if let Some((route, _)) = v.first() {
                    return Ok(*route);
                }
            } else {
                let len = v.len();
                if len != 0 {
                    let index = count.fetch_add(1, Ordering::Relaxed);
                    let route = &v[index % len].0;
                    // 尝试跳过默认rt的路由(一般是刚加入的)，这有助于提升稳定性
                    if route.rt != DEFAULT_RT {
                        return Ok(*route);
                    }
                    for (route, _) in v {
                        if route.rt != DEFAULT_RT {
                            return Ok(*route);
                        }
                    }
                    return Ok(v[0].0);
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "route not found"))
    }
}
impl<PeerID: Hash + Eq + Clone> RouteTable<PeerID> {
    pub fn add_route_if_absent(&self, id: PeerID, route: Route) -> bool {
        self.add_route_(id, route, true)
    }
    pub fn add_route<R: Into<Route>>(&self, id: PeerID, route: R) -> bool {
        self.add_route_(id, route.into(), false)
    }
    fn add_route_(&self, id: PeerID, route: Route, only_if_absent: bool) -> bool {
        let key = route.route_key();
        if only_if_absent {
            if let Some((_, list)) = self.route_table.read().get(&id) {
                for (x, _) in list {
                    if x.route_key() == key {
                        return true;
                    }
                }
            }
        }
        let mut route_table = self.route_table.write();
        let (_, list) = route_table
            .entry(id)
            .or_insert_with(|| (AtomicUsize::new(0), Vec::with_capacity(4)));
        let mut exist = false;
        for (x, time) in list.iter_mut() {
            if x.metric < route.metric && !self.first_latency {
                //非优先延迟的情况下 不能比当前的路径更长
                return false;
            }
            if x.route_key() == key {
                if only_if_absent {
                    return true;
                }
                x.metric = route.metric;
                x.rt = route.rt;
                exist = true;
                time.store(Instant::now());
                break;
            }
        }
        if exist {
            list.sort_by_key(|(k, _)| k.rt);
        } else {
            if !self.first_latency {
                if route.is_p2p() {
                    //非优先延迟的情况下 添加了直连的则排除非直连的
                    list.retain(|(k, _)| k.is_p2p());
                }
            };
            list.sort_by_key(|(k, _)| k.rt);
            list.push((route, AtomicCell::new(Instant::now())));
        }
        return true;
    }

    pub fn route(&self, id: &PeerID) -> Option<Vec<Route>> {
        if let Some((_, v)) = self.route_table.read().get(id) {
            Some(v.iter().map(|(i, _)| *i).collect())
        } else {
            None
        }
    }
    pub fn route_one(&self, id: &PeerID) -> Option<Route> {
        if let Some((_, v)) = self.route_table.read().get(id) {
            v.first().map(|(i, _)| *i)
        } else {
            None
        }
    }
    pub fn route_one_p2p(&self, id: &PeerID) -> Option<Route> {
        if let Some((_, v)) = self.route_table.read().get(id) {
            for (i, _) in v {
                if i.is_p2p() {
                    return Some(*i);
                }
            }
        }
        None
    }
    pub fn route_to_id(&self, route_key: &RouteKey) -> Option<PeerID> {
        let table = self.route_table.read();
        for (k, (_, v)) in table.iter() {
            for (route, _) in v {
                if &route.route_key() == route_key && route.is_p2p() {
                    return Some(k.clone());
                }
            }
        }
        None
    }
    pub fn need_punch(&self, id: &PeerID) -> bool {
        if let Some((_, v)) = self.route_table.read().get(id) {
            //p2p的通道数符合要求
            return v.iter().filter(|(k, _)| k.is_p2p()).count() < self.channel_num;
        }
        true
    }
    pub fn no_need_punch(&self, id: &PeerID) -> bool {
        !self.need_punch(id)
    }
    pub fn p2p_num(&self, id: &PeerID) -> usize {
        if let Some((_, v)) = self.route_table.read().get(id) {
            v.iter().filter(|(k, _)| k.is_p2p()).count()
        } else {
            0
        }
    }
    pub fn relay_num(&self, id: &PeerID) -> usize {
        if let Some((_, v)) = self.route_table.read().get(id) {
            v.iter().filter(|(k, _)| k.is_relay()).count()
        } else {
            0
        }
    }
    /// Return all routes
    pub fn route_table(&self) -> Vec<(PeerID, Vec<Route>)> {
        let table = self.route_table.read();
        table
            .iter()
            .map(|(k, (_, v))| (k.clone(), v.iter().map(|(i, _)| i.clone()).collect()))
            .collect()
    }
    /// Return all P2P routes
    pub fn route_table_p2p(&self) -> Vec<(PeerID, Route)> {
        let table = self.route_table.read();
        let mut list = Vec::with_capacity(8);
        for (id, (_, routes)) in table.iter() {
            for (route, _) in routes.iter() {
                if route.is_p2p() {
                    list.push((id.clone(), *route));
                    break;
                }
            }
        }
        list
    }
    /// Return to the first route
    pub fn route_table_one(&self) -> Vec<(PeerID, Route)> {
        let mut list = Vec::with_capacity(8);
        let table = self.route_table.read();
        for (k, (_, v)) in table.iter() {
            if let Some((route, _)) = v.first() {
                list.push((k.clone(), *route));
            }
        }
        list
    }
    /// Remove specified route
    pub fn remove_route(&self, id: &PeerID, route_key: &RouteKey) {
        let mut write_guard = self.route_table.write();
        if let Some((_, routes)) = write_guard.get_mut(id) {
            routes.retain(|(x, _)| &x.route_key() != route_key);
            if routes.is_empty() {
                write_guard.remove(id);
            }
        }
    }
    pub fn remove_all(&self, id: &PeerID) {
        let mut write_guard = self.route_table.write();
        write_guard.remove(id);
    }
    /// Update the usage time of the route,
    /// routes that have not received data for a long time will be excluded
    pub fn update_read_time(&self, id: &PeerID, route_key: &RouteKey) -> bool {
        if let Some((_, routes)) = self.route_table.read().get(id) {
            for (route, time) in routes {
                if &route.route_key() == route_key {
                    time.store(Instant::now());
                    return true;
                }
            }
        }
        false
    }
    pub fn oldest_route(&self) -> Option<(PeerID, Route, Instant)> {
        let guard = self.route_table.read();
        if guard.is_empty() {
            return None;
        }
        let mut option: Option<(PeerID, Route, Instant)> = None;
        for (peer_id, (_, routes)) in guard.iter() {
            for (route, time) in routes {
                let instant = time.load();
                if let Some((t_peer_id, t_route, t_instant)) = &mut option {
                    if *t_instant > instant {
                        *t_peer_id = peer_id.clone();
                        *t_route = *route;
                        *t_instant = instant;
                    }
                } else {
                    option.replace((peer_id.clone(), *route, instant));
                }
            }
        }
        option
    }
}

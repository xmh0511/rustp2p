use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RecvError {
    #[error("io")]
    Io(#[from] io::Error),
    #[error("eof")]
    Eof,
}
#[derive(Error, Debug)]
pub enum SendError {
    #[error("io")]
    Io(#[from] io::Error),
    #[error("route not found")]
    RouteNotFound,
    #[error("invalid protocol")]
    InvalidProtocol,
    #[error("index out of bounds:len is {len} but the index is {index}")]
    IndexOutOfBounds { len: usize, index: usize },
}

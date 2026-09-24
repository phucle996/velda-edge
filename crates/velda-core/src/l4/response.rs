use std::net::SocketAddr;

#[derive(Debug)]
pub enum L4Action {
    Forward,
    Close,
    Reject,
}

#[derive(Debug)]
pub struct L4Response {
    pub action: L4Action,
    pub target: Option<SocketAddr>,
}

use acktor::{ActorId, Address};

use super::Session;

/// A session handle which can be used to identify a session in a node by either its actor
/// address, index, or label.
#[derive(Debug, Clone)]
pub enum SessionHandle {
    Address(Address<Session>),
    Index(ActorId),
    Label(String),
}

impl From<Address<Session>> for SessionHandle {
    fn from(addr: Address<Session>) -> Self {
        Self::Address(addr)
    }
}

impl From<ActorId> for SessionHandle {
    fn from(index: ActorId) -> Self {
        Self::Index(index)
    }
}

impl From<String> for SessionHandle {
    fn from(label: String) -> Self {
        Self::Label(label)
    }
}

impl From<&str> for SessionHandle {
    fn from(label: &str) -> Self {
        Self::Label(label.to_string())
    }
}

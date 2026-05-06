use acktor::{Actor, ActorId, Address, Message, Recipient, SenderInfo};

/// An actor reference which can be used to identify an actor in a node by either its index or
/// label.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActorRef {
    Index(ActorId),
    Label(String),
}

impl<A> From<Address<A>> for ActorRef
where
    A: Actor,
{
    fn from(addr: Address<A>) -> Self {
        Self::Index(addr.index())
    }
}

impl<M> From<Recipient<M>> for ActorRef
where
    M: Message,
{
    fn from(recipient: Recipient<M>) -> Self {
        Self::Index(recipient.index())
    }
}

impl From<ActorId> for ActorRef {
    fn from(index: ActorId) -> Self {
        Self::Index(index)
    }
}

impl From<u64> for ActorRef {
    fn from(index: u64) -> Self {
        Self::Index(ActorId::new(index))
    }
}

impl From<String> for ActorRef {
    fn from(label: String) -> Self {
        Self::Label(label)
    }
}

impl From<&str> for ActorRef {
    fn from(label: &str) -> Self {
        Self::Label(label.to_string())
    }
}

use acktor::ActorId;

/// An actor reference which can be used to identify an actor in a node by either its index or
/// label.
#[derive(Debug, Clone)]
pub enum ActorRef {
    Index(ActorId),
    Label(String),
}

impl From<ActorId> for ActorRef {
    fn from(index: ActorId) -> Self {
        Self::Index(index)
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

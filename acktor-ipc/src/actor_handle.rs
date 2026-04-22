use acktor::ActorId;

/// An actor handle which can be used to identify an actor in a node by either its index
/// or label.
#[derive(Debug, Clone)]
pub enum ActorHandle {
    Index(ActorId),
    Label(String),
}

impl From<ActorId> for ActorHandle {
    fn from(index: ActorId) -> Self {
        Self::Index(index)
    }
}

impl From<String> for ActorHandle {
    fn from(label: String) -> Self {
        Self::Label(label)
    }
}

impl From<&str> for ActorHandle {
    fn from(label: &str) -> Self {
        Self::Label(label.to_string())
    }
}

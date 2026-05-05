use std::fmt::{self, Debug, Display};
#[cfg(feature = "ipc")]
use std::num::NonZeroU64;

/// Actor index.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId {
    index: u64,
    session: u64,
}

impl Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.session == 0 {
            write!(f, "{}", self.index)
        } else {
            write!(f, "{}@{}", self.index, self.session)
        }
    }
}

impl Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl ActorId {
    /// Constructs a new `ActorId` from an local index.
    pub const fn new(index: u64) -> Self {
        Self { index, session: 0 }
    }

    /// Returns the local index part of this `ActorId`.
    pub const fn as_local(&self) -> u64 {
        self.index
    }

    /// Constructs a new `ActorId` from a local index and a non-zero remote session index.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub const fn new_remote(index: u64, session: NonZeroU64) -> Self {
        Self {
            index,
            session: session.get(),
        }
    }

    /// Returns true if this `ActorId` is a remote index.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub const fn is_remote(&self) -> bool {
        self.session != 0
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_local_id() {
        let id = ActorId::new(42);
        assert_eq!(id.as_local(), 42);
        assert_eq!(format!("{:?}", id), "42");
        assert_eq!(format!("{}", id), "42");
        assert!(ActorId::new(1) < ActorId::new(2));
        assert_eq!(ActorId::new(1), ActorId::new(1));
    }

    #[cfg(feature = "ipc")]
    #[test]
    fn test_remote_id() {
        let remote = ActorId::new_remote(3, std::num::NonZeroU64::new(5).unwrap());
        assert!(remote.is_remote());
        assert_eq!(remote.as_local(), 3);
        assert_eq!(format!("{:?}", remote), "3@5");
        assert!(!ActorId::new(3).is_remote());
    }
}

use std::fmt::{self, Debug, Display};
#[cfg(feature = "ipc")]
use std::num::NonZeroU64;

/// Actor index type.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId {
    index: u64,
    epoch: u64,
}

impl Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.epoch == 0 {
            write!(f, "{}", self.index)
        } else {
            write!(f, "{}@{}", self.index, self.epoch)
        }
    }
}

impl Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl ActorId {
    /// Constructs a new `ActorId` from a u64.
    pub const fn new(index: u64) -> Self {
        Self { index, epoch: 0 }
    }

    /// Returns the index part of this `ActorId` as a u64.
    pub const fn as_u64(&self) -> u64 {
        self.index
    }

    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    /// Constructs a new `ActorId` from a u64 and a non-zero remote id.
    pub const fn new_remote(index: u64, remote_index: NonZeroU64) -> Self {
        Self {
            index,
            epoch: remote_index.get(),
        }
    }

    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    /// Returns true if this `ActorId` is a remote id.
    pub const fn is_remote(&self) -> bool {
        self.epoch != 0
    }
}

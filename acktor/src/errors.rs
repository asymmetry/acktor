//! Error types used by this crate.

use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};

use thiserror::Error;

mod report;
pub use report::ErrorReport;

/// Error returned when sending a message.
pub enum SendError<M> {
    Closed(M),
    Full(M),
    Timeout(M),
    Other(Box<dyn StdError + Send + Sync>, M),
}

impl<M> Debug for SendError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => fmt.write_str("Closed(..)"),
            SendError::Full(_) => fmt.write_str("Full(..)"),
            SendError::Timeout(_) => fmt.write_str("Timeout(..)"),
            SendError::Other(transparent, _) => Debug::fmt(transparent, fmt),
        }
    }
}

impl<M> Display for SendError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SendError::Closed(_) => fmt.write_str("sending on a closed channel"),
            SendError::Full(_) => fmt.write_str("sending on a full channel"),
            SendError::Timeout(_) => fmt.write_str("timed out waiting on sending"),
            SendError::Other(transparent, _) => Display::fmt(transparent, fmt),
        }
    }
}

impl<M> StdError for SendError<M> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SendError::Closed(_) => Option::None,
            SendError::Full(_) => Option::None,
            SendError::Timeout(_) => Option::None,
            SendError::Other(transparent, _) => transparent.source(),
        }
    }
}

impl<M> From<tokio::sync::mpsc::error::SendError<M>> for SendError<M> {
    fn from(e: tokio::sync::mpsc::error::SendError<M>) -> Self {
        Self::Closed(e.0)
    }
}

impl<M> From<tokio::sync::mpsc::error::TrySendError<M>> for SendError<M> {
    fn from(e: tokio::sync::mpsc::error::TrySendError<M>) -> Self {
        match e {
            tokio::sync::mpsc::error::TrySendError::Closed(m) => Self::Closed(m),
            tokio::sync::mpsc::error::TrySendError::Full(m) => Self::Full(m),
        }
    }
}

impl<M> From<tokio::sync::mpsc::error::SendTimeoutError<M>> for SendError<M> {
    fn from(e: tokio::sync::mpsc::error::SendTimeoutError<M>) -> Self {
        match e {
            tokio::sync::mpsc::error::SendTimeoutError::Closed(m) => Self::Closed(m),
            tokio::sync::mpsc::error::SendTimeoutError::Timeout(m) => Self::Timeout(m),
        }
    }
}

/// Error returned when receiving a message.
#[derive(Debug, Error)]
pub enum RecvError {
    #[error("receiving on a closed channel")]
    Closed,

    #[error("receiving on an empty channel")]
    Empty,

    #[error("timed out waiting on receiving")]
    Timeout,

    #[error(transparent)]
    Other(Box<dyn StdError + Send + Sync>),
}

impl From<tokio::sync::mpsc::error::TryRecvError> for RecvError {
    fn from(e: tokio::sync::mpsc::error::TryRecvError) -> Self {
        match e {
            tokio::sync::mpsc::error::TryRecvError::Disconnected => Self::Closed,
            tokio::sync::mpsc::error::TryRecvError::Empty => Self::Empty,
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for RecvError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Closed
    }
}

impl From<tokio::sync::oneshot::error::TryRecvError> for RecvError {
    fn from(e: tokio::sync::oneshot::error::TryRecvError) -> Self {
        match e {
            tokio::sync::oneshot::error::TryRecvError::Closed => Self::Closed,
            tokio::sync::oneshot::error::TryRecvError::Empty => Self::Empty,
        }
    }
}

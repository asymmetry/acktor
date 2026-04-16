//! Error types used by this crate.

use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::pin::Pin;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::channel::oneshot::Receiver;
use crate::envelope::DefaultEnvelopeProxy;
use crate::message::Message;

mod report;
pub use report::ErrorReport;

pub type SendResult<M, EP = DefaultEnvelopeProxy<M>> =
    Result<Receiver<<M as Message<EP>>::Result>, SendError<M>>;

pub type SendResultFuture<'a, M, EP = DefaultEnvelopeProxy<M>> =
    Pin<Box<dyn Future<Output = SendResult<M, EP>> + Send + 'a>>;

pub type DoSendResult<M> = Result<(), SendError<M>>;

pub type DoSendResultFuture<'a, M> = Pin<Box<dyn Future<Output = DoSendResult<M>> + Send + 'a>>;

/// Error returned when sending a message.
pub enum SendError<M> {
    Closed(M),
    Full(M),
    Timeout(M),
    Other(Box<dyn std::error::Error + Send + Sync>, M),
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

impl<M> std::error::Error for SendError<M> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SendError::Closed(_) => Option::None,
            SendError::Full(_) => Option::None,
            SendError::Timeout(_) => Option::None,
            SendError::Other(transparent, _) => transparent.source(),
        }
    }
}

impl<M> From<mpsc::error::SendError<M>> for SendError<M> {
    fn from(e: mpsc::error::SendError<M>) -> Self {
        Self::Closed(e.0)
    }
}

impl<M> From<mpsc::error::TrySendError<M>> for SendError<M> {
    fn from(e: mpsc::error::TrySendError<M>) -> Self {
        match e {
            mpsc::error::TrySendError::Closed(m) => Self::Closed(m),
            mpsc::error::TrySendError::Full(m) => Self::Full(m),
        }
    }
}

impl<M> From<mpsc::error::SendTimeoutError<M>> for SendError<M> {
    fn from(e: mpsc::error::SendTimeoutError<M>) -> Self {
        match e {
            mpsc::error::SendTimeoutError::Closed(m) => Self::Closed(m),
            mpsc::error::SendTimeoutError::Timeout(m) => Self::Timeout(m),
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
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl From<mpsc::error::TryRecvError> for RecvError {
    fn from(e: mpsc::error::TryRecvError) -> Self {
        match e {
            mpsc::error::TryRecvError::Disconnected => Self::Closed,
            mpsc::error::TryRecvError::Empty => Self::Empty,
        }
    }
}

impl From<oneshot::error::RecvError> for RecvError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::Closed
    }
}

impl From<oneshot::error::TryRecvError> for RecvError {
    fn from(e: oneshot::error::TryRecvError) -> Self {
        match e {
            oneshot::error::TryRecvError::Closed => Self::Closed,
            oneshot::error::TryRecvError::Empty => Self::Empty,
        }
    }
}

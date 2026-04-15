//! Error types used by this crate.

use thiserror::Error;

use crate::channel::oneshot::Receiver;
use crate::envelope::DefaultEnvelopeProxy;
use crate::message::Message;

mod report;
pub use report::ErrorReport;

pub type SendResult<M, EP = DefaultEnvelopeProxy<M>> =
    Result<Receiver<<M as Message<EP>>::Result>, SendError<M>>;
pub type DoSendResult<M> = Result<(), SendError<M>>;

/// Error returned when sending a message through an address, recipient, or sender.
///
/// The message is returned inside the error variant so callers can recover ownership
/// and retry or handle it differently.
#[derive(Error)]
pub enum SendError<M> {
    /// The channel has been closed.
    #[error("channel is closed")]
    Closed(M),
    /// The channel is full.
    #[error("channel is full")]
    Full(M),
    /// Any other failure with a descriptive message.
    #[error("external send error")]
    Other {
        message: Option<M>,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl<M> std::fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
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

/// Error returned when receiving a message from a channel.
#[derive(Error)]
pub enum RecvError {
    /// The channel is closed and no more messages can be received.
    #[error("channel is closed")]
    Closed,
    /// The channel is currently empty.
    #[error("channel is empty")]
    Empty,
    /// Any other failure with a descriptive message.
    #[error("external recv error")]
    Other {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl std::fmt::Debug for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
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

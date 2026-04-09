//! Error types used by this crate.

use std::io;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

pub use crate::codec::errors::{DecodeError, EncodeError};

/// Error type used by [`Node`][crate::node::Node].
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("could not connect to the remote endpoint")]
    ConnectFailed(#[from] io::Error),

    #[error("could not create a new session")]
    CreateSessionFailed(#[source] SessionError),

    #[error("could not find the session {0}")]
    SessionNotFound(String),

    #[error("could not create the remote actor")]
    CreateRemoteActorFailed(#[source] SessionError),

    #[error("could not find the actor in the remote process")]
    RemoteActorNotFound(#[source] SessionError),

    #[error("could not send local message")]
    SendMessageError(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not receive local message")]
    ReceiveMessageError(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl<T> From<mpsc::error::SendError<T>> for NodeError
where
    T: Send + Sync + 'static,
{
    fn from(source: mpsc::error::SendError<T>) -> Self {
        Self::SendMessageError(Box::new(source))
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for NodeError
where
    T: Send + Sync + 'static,
{
    fn from(source: mpsc::error::TrySendError<T>) -> Self {
        Self::SendMessageError(Box::new(source))
    }
}

impl From<oneshot::error::RecvError> for NodeError {
    fn from(source: oneshot::error::RecvError) -> Self {
        Self::ReceiveMessageError(Box::new(source))
    }
}

impl From<oneshot::error::TryRecvError> for NodeError {
    fn from(source: oneshot::error::TryRecvError) -> Self {
        Self::ReceiveMessageError(Box::new(source))
    }
}

/// Error type used by [`Node`][crate::node::Node] to represent session related errors.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not encode the remote message")]
    EncodeError(#[from] EncodeError),

    #[error("could not decode the remote message")]
    DecodeError(#[from] DecodeError),

    #[error("could not forward the inbound remote message to any local actor")]
    ForwardInboundMessageFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not send the outbound remote message to the remote node")]
    SendOutboundMessageFailed(#[from] io::Error),

    #[error("invalid node message reply tag: {0}")]
    InvalidNodeMessageReplyTag(u64),

    #[error("invalid actor message reply tag: {0}")]
    InvalidActorMessageReplyTag(u64),

    #[error("{0}")]
    RemoteNodeError(String),

    #[error("could not send local message")]
    SendMessageError(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not receive local message")]
    ReceiveMessageError(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl<T> From<mpsc::error::SendError<T>> for SessionError
where
    T: Send + Sync + 'static,
{
    fn from(source: mpsc::error::SendError<T>) -> Self {
        Self::SendMessageError(Box::new(source))
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for SessionError
where
    T: Send + Sync + 'static,
{
    fn from(source: mpsc::error::TrySendError<T>) -> Self {
        Self::SendMessageError(Box::new(source))
    }
}

impl From<oneshot::error::RecvError> for SessionError {
    fn from(source: oneshot::error::RecvError) -> Self {
        Self::ReceiveMessageError(Box::new(source))
    }
}

impl From<oneshot::error::TryRecvError> for SessionError {
    fn from(source: oneshot::error::TryRecvError) -> Self {
        Self::ReceiveMessageError(Box::new(source))
    }
}

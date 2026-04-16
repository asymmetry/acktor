//! Error types used by this crate.

use std::io;

use thiserror::Error;

use acktor::{RecvError, SendError};

pub use crate::codec::errors::{DecodeError, EncodeError};
pub use crate::double_map::{KeyConflictError, TryReserveError};

/// Error type used by [`Node`][crate::node::Node].
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("could not connect to the remote endpoint")]
    ConnectFailed(#[from] io::Error),

    #[error("could not create a new session")]
    CreateSessionFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not find the session {0}")]
    SessionNotFound(String),

    #[error("could not create the remote actor")]
    CreateRemoteActorFailed(#[source] SessionError),

    #[error("could not find the actor in the remote process")]
    RemoteActorNotFound(#[source] SessionError),

    #[error("could not send message")]
    SendError(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not receive message")]
    RecvError(#[from] RecvError),
}

impl<T> From<SendError<T>> for NodeError
where
    T: Send + Sync + 'static,
{
    fn from(source: SendError<T>) -> Self {
        Self::SendError(source.into())
    }
}

/// Error type used by [`Node`][crate::node::Node] to represent session related errors.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("could not encode the remote message")]
    EncodeError(#[from] EncodeError),

    #[error("could not decode the remote message")]
    DecodeError(#[from] DecodeError),

    #[error("could not forward the inbound remote message to any actor")]
    ForwardInboundMessageFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not send the outbound remote message to the remote node")]
    SendOutboundMessageFailed(#[source] io::Error),

    #[error("invalid node message reply tag: {0}")]
    InvalidNodeMessageReplyTag(u64),

    #[error("invalid actor message reply tag: {0}")]
    InvalidActorMessageReplyTag(u64),

    #[error("could not create the actor")]
    CreateActorFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not find the actor {0}")]
    ActorNotFound(String),

    #[error("{0}")]
    RemoteActorError(String),

    #[error(transparent)]
    IoError(io::Error),

    #[error("could not send message")]
    SendError(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("could not receive message")]
    RecvError(#[from] RecvError),
}

impl<T> From<SendError<T>> for SessionError
where
    T: Send + Sync + 'static,
{
    fn from(source: SendError<T>) -> Self {
        Self::SendError(source.into())
    }
}

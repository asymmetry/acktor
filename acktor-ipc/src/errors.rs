//! Error types used by this crate.

use std::error::Error as StdError;
use std::io;

use thiserror::Error;

use acktor::{RecvError, SendError};

pub use crate::codec::{DecodeError, EncodeError};
pub use crate::double_map::{KeyConflictError, TryReserveError};
pub use crate::remote_message::ToRemoteMessageRecipientError;

pub type BoxError = Box<dyn StdError + Send + Sync>;

/// Error type used by [`Node`][crate::node::Node].
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("could not connect to the remote endpoint")]
    ConnectFailed(#[from] io::Error),

    #[error("could not create a new session")]
    CreateSessionFailed(#[source] Box<dyn StdError + Send + Sync>),

    #[error("could not find the session {0}")]
    SessionNotFound(String),

    #[error("could not create the remote actor")]
    CreateRemoteActorFailed(#[source] SessionError),

    #[error("could not find the actor in the remote process")]
    RemoteActorNotFound(#[source] SessionError),

    #[error("could not send message")]
    SendError(#[source] Box<dyn StdError + Send + Sync>),

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
    #[error("could not encode the outbound remote message")]
    EncodeError(#[from] EncodeError),

    #[error("could not decode the inbound remote message")]
    DecodeError(#[from] DecodeError),

    #[error("could not forward the inbound remote message to any actor")]
    ForwardInboundMessageFailed(#[source] Box<dyn StdError + Send + Sync>),

    #[error("could not send the outbound remote message to the remote node")]
    SendOutboundMessageFailed(#[source] io::Error),

    #[error("invalid node message reply tag: {0}")]
    InvalidNodeMessageReplyTag(u64),

    #[error(
        "could not forward the node message reply, whoever is waiting for it closed the channel"
    )]
    ForwardNodeMessageReplyFailed,

    #[error("invalid actor message reply tag: {0}")]
    InvalidActorMessageReplyTag(u64),

    #[error(
        "could not forward the actor message reply, whoever is waiting for it closed the channel"
    )]
    ForwardActorMessageReplyFailed,

    #[error("could not create the actor on behalf of the remote peer")]
    RemoteActorFactoryError(#[source] Box<dyn StdError + Send + Sync>),

    #[error("could not find the actor {0}")]
    ActorNotFound(String),

    #[error("{0}")]
    RemotePeerError(String),

    #[error(transparent)]
    IoError(io::Error),

    #[error("could not send message")]
    SendError(#[source] Box<dyn StdError + Send + Sync>),

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

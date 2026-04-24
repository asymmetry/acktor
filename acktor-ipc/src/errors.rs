//! Error types used by this crate.

use std::io;

use thiserror::Error;

use acktor::{BoxError, RecvError, SendError};

pub use crate::codec::{DecodeError, EncodeError};
pub use crate::double_map::{KeyConflictError, TryReserveError};
pub use crate::remote_message::ToRemoteMessageRecipientError;

/// Error type used by [`Node`][crate::node::Node].
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("could not connect to the remote endpoint")]
    ConnectFailed(#[from] io::Error),

    #[error("could not create a new session")]
    CreateSessionFailed(#[source] BoxError),

    #[error("could not find the session {0}")]
    SessionNotFound(String),

    #[error("could not create the remote actor")]
    CreateRemoteActorFailed(#[source] SessionError),

    #[error("could not find the actor in the remote process")]
    RemoteActorNotFound(#[source] SessionError),

    #[error("could not send message")]
    SendError(#[source] BoxError),

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

    #[error("could not send the outbound remote message to the remote node")]
    SendOutboundMessageFailed(#[source] io::Error),

    #[error("could not forward the inbound remote message to any actor")]
    ForwardInboundMessageFailed(#[source] BoxError),

    #[error("invalid node message response tag: {0}")]
    InvalidNodeMsgResTxTag(u64),

    #[error("could not forward the node message response to the original sender")]
    ForwardNodeMsgResFailed,

    #[error("invalid actor message response tag: {0}")]
    InvalidActorMsgResTxTag(u64),

    #[error("could not forward the actor message response")]
    ForwardActorMessageResFailed,

    #[error("could not create the actor on behalf of the remote peer")]
    RemoteActorFactoryError(#[source] BoxError),

    #[error("could not find actor {0} in the current process")]
    ActorNotFound(String),

    #[error("could not handle inbound remote message")]
    HandleInboundMessageFailed(#[source] BoxError),

    #[error(
        "no response received from the remote peer after {} seconds",
        crate::session::RESPONSE_TIMEOUT.as_secs()
    )]
    ResponseTimeout,

    #[error("remote actor returned an error:{0}")]
    RemotePeerError(String),

    #[error(transparent)]
    IoError(io::Error),

    #[error("could not send message")]
    SendError(#[source] BoxError),

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

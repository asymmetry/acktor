//! Remote message which can be sent over an IPC channel.

use std::fmt::{self, Debug};

use bytes::Bytes;
use thiserror::Error;

use acktor::{Actor, ActorState, Address, Message, Recipient, Sender, channel::oneshot};

use crate::codec::DecodeContext;
use crate::remote_address::RemoteAddress;

/// The kind of a remote message.
pub enum RemoteMessageKind {
    /// No response is expected for this message.
    DoSend,
    /// The sender expects a response for this message.
    Send(oneshot::Sender<Bytes>),
}

impl Debug for RemoteMessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DoSend => f.write_str("DoSend"),
            Self::Send(_) => f.write_str("Send"),
        }
    }
}

/// A unified remote message used for communication with remote actors.
///
/// This is used both for outbound messages (sent by an actor in the current process to an actor
/// in a peer process through [`Session`][crate::session::Session]) and for inbound messages
/// (received from an IPC session and forwarded to an actor in the current process for
/// processing).
#[derive(Message)]
#[result_type(())]
pub struct RemoteMessage {
    pub actor_id: u64,
    pub kind: RemoteMessageKind,
    pub message: Bytes,
    pub decode_context: Option<DecodeContext>,
}

impl Debug for RemoteMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteMessage")
            .field("actor_id", &self.actor_id)
            .field("kind", &self.kind)
            .field("message", &format_args!("Bytes({})", self.message.len()))
            .finish()
    }
}

impl RemoteMessage {
    /// Constructs a new [`RemoteMessage`] with [`DoSend`][RemoteMessageKind::DoSend] kind.
    pub fn do_send(actor_id: u64, message: Bytes) -> Self {
        Self {
            actor_id,
            kind: RemoteMessageKind::DoSend,
            message,
            decode_context: None,
        }
    }

    /// Constructs a new [`RemoteMessage`] with [`Send`][RemoteMessageKind::Send] kind.
    pub fn send(actor_id: u64, message: Bytes, tx: oneshot::Sender<Bytes>) -> Self {
        Self {
            actor_id,
            kind: RemoteMessageKind::Send(tx),
            message,
            decode_context: None,
        }
    }

    /// Sets the decode context on this message.
    pub fn with_context(mut self, context: DecodeContext) -> Self {
        self.decode_context = Some(context);
        self
    }
}

#[derive(Debug, Error)]
pub enum ToRemoteMessageRecipientError {
    #[error(
        "actor does not opt-in the `type-erased-recipient-hook` feature, see the docs of \
         `RemoteActor` for details"
    )]
    MissingHook,

    #[error("could not downcast the type-erased recipient to Recipient<RemoteMessage>")]
    DowncastFailed,
}

pub trait ToRemoteMessageRecipient {
    fn to_remote_message_recipient(
        &self,
    ) -> Result<Recipient<RemoteMessage>, ToRemoteMessageRecipientError>;
}

impl<A> ToRemoteMessageRecipient for Address<A>
where
    A: Actor,
{
    fn to_remote_message_recipient(
        &self,
    ) -> Result<Recipient<RemoteMessage>, ToRemoteMessageRecipientError> {
        Ok(*self
            .type_erased_recipient()
            .ok_or(ToRemoteMessageRecipientError::MissingHook)?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|_| ToRemoteMessageRecipientError::DowncastFailed)?)
    }
}

impl<M> ToRemoteMessageRecipient for Recipient<M>
where
    M: Message,
{
    fn to_remote_message_recipient(
        &self,
    ) -> Result<Recipient<RemoteMessage>, ToRemoteMessageRecipientError> {
        Ok(*self
            .type_erased_recipient()
            .ok_or(ToRemoteMessageRecipientError::MissingHook)?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|_| ToRemoteMessageRecipientError::DowncastFailed)?)
    }
}

/// A message which is used to report actor status to a supervisor from a remote node.
#[derive(Debug, Message)]
#[result_type(())]
pub enum RemoteSupervisionEvent {
    /// Warning, the actor could resume by itself.
    Warn(RemoteAddress, String),
    /// Actor terminated with or without error.
    Terminated(RemoteAddress, Option<String>),
    /// Actor panicked with the given panic info.
    Panicked(RemoteAddress, String),
    /// Actor state changed.
    State(RemoteAddress, ActorState),
}

//! Remote message which can be sent over an IPC channel.
//!

use std::fmt::{self, Debug};

use bytes::Bytes;
use thiserror::Error;

use acktor::{
    Actor, ActorState, Address, Message, Recipient, Sender, channel::oneshot, utils::ShortName,
};

use crate::codec::DecodeContext;
use crate::remote_address::RemoteAddress;

/// A unified remote message used for communication with remote actors.
///
/// This is used both for outbound messages (sent by an actor in the current process to an actor
/// in a remote process through [`Session`][crate::session::Session]) and for inbound messages
/// (received by a [`Session`][crate::session::Session] and forwarded to an actor in the current
/// process for handling).
#[derive(Message)]
#[result_type(())]
pub struct RemoteMessage {
    pub actor_id: u64,
    pub message_id: u64,
    pub message: Bytes,
    pub result_tx: Option<oneshot::Sender<Bytes>>,
    pub decode_context: Option<DecodeContext>,
}

impl Debug for RemoteMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteMessage")
            .field("actor_id", &self.actor_id)
            .field("message_id", &self.message_id)
            .field("message", &format_args!("Bytes({})", self.message.len()))
            .field(
                "result_tx",
                &match self.result_tx {
                    Some(_) => format_args!("Send"),
                    None => format_args!("DoSend"),
                },
            )
            .finish()
    }
}

impl RemoteMessage {
    /// Constructs a new [`RemoteMessage`] which does not expect a response.
    pub fn do_send(actor_id: u64, message_id: u64, message: Bytes) -> Self {
        Self {
            actor_id,
            message_id,
            message,
            result_tx: None,
            decode_context: None,
        }
    }

    /// Constructs a new [`RemoteMessage`] which expects a response.
    pub fn send(
        actor_id: u64,
        message_id: u64,
        message: Bytes,
        tx: oneshot::Sender<Bytes>,
    ) -> Self {
        Self {
            actor_id,
            message_id,
            message,
            result_tx: Some(tx),
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
        "`{0}` does not opt-in the `type-erased-recipient-hook` feature, see the docs of \
         `RemoteActor` for details"
    )]
    MissingHook(String),

    #[error("{0}")]
    DowncastFailed(String),
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
            .ok_or_else(|| {
                ToRemoteMessageRecipientError::MissingHook(ShortName::of::<A>().to_string())
            })?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|(_, e)| ToRemoteMessageRecipientError::DowncastFailed(e))?)
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
            .ok_or_else(|| {
                ToRemoteMessageRecipientError::MissingHook(
                    ShortName::of::<Recipient<M>>().to_string(),
                )
            })?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|(_, e)| ToRemoteMessageRecipientError::DowncastFailed(e))?)
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

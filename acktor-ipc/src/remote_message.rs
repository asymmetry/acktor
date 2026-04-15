//! Remote message which can be sent over an IPC channel.

use std::fmt::{self, Debug};

use bytes::Bytes;

use acktor::{Actor, ActorState, Address, Message, Recipient, Sender, channel::oneshot};

use crate::codec::DecodeContext;
use crate::remote_address::RemoteAddress;

pub use acktor::{Signal as RemoteSignal, cron::CronSignal as RemoteCronSignal};

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
            RemoteMessageKind::DoSend => f.write_str("DoSend"),
            RemoteMessageKind::Send(_) => f.write_str("Send"),
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
    pub message: Bytes,
    pub kind: RemoteMessageKind,
    pub decode_context: Option<DecodeContext>,
}

impl Debug for RemoteMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteMessage")
            .field("actor_id", &self.actor_id)
            .field("message", &format_args!("Bytes({})", self.message.len()))
            .field("kind", &self.kind)
            .field(
                "decode_context",
                &format_args!(
                    "{}",
                    match &self.decode_context {
                        Some(_) => "Some(..)",
                        None => "None",
                    }
                ),
            )
            .finish()
    }
}

impl RemoteMessage {
    /// Constructs a new [`DoSend`][RemoteMessageKind::DoSend][`RemoteMessage`].
    pub fn do_send(actor_id: u64, message: Bytes) -> Self {
        Self {
            actor_id,
            message,
            kind: RemoteMessageKind::DoSend,
            decode_context: None,
        }
    }

    /// Constructs a new [`Send`][RemoteMessageKind::Send][`RemoteMessage`].
    pub fn send(actor_id: u64, message: Bytes, tx: oneshot::Sender<Bytes>) -> Self {
        Self {
            actor_id,
            message,
            kind: RemoteMessageKind::Send(tx),
            decode_context: None,
        }
    }

    /// Sets the decode context on this message.
    pub fn with_context(mut self, context: DecodeContext) -> Self {
        self.decode_context = Some(context);
        self
    }
}

pub trait RecipientExt {
    fn to_recipient_remote_message(&self) -> Result<Recipient<RemoteMessage>, String>;
}

impl<A> RecipientExt for Address<A>
where
    A: Actor,
{
    fn to_recipient_remote_message(&self) -> Result<Recipient<RemoteMessage>, String> {
        Ok(*self
            .erased_recipient()
            .ok_or_else(|| "actor does not opt in the erased_recipient hook".to_string())?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|_| "could not downcast to Recipient<RemoteMessage>".to_string())?)
    }
}

impl<M> RecipientExt for Recipient<M>
where
    M: Message,
{
    fn to_recipient_remote_message(&self) -> Result<Recipient<RemoteMessage>, String> {
        Ok(*self
            .erased_recipient()
            .ok_or_else(|| "actor does not opt in the erased_recipient hook".to_string())?
            .downcast::<Recipient<RemoteMessage>>()
            .map_err(|_| "could not downcast to Recipient<RemoteMessage>".to_string())?)
    }
}

/// A message which is used to set/unset a supervisor from a remote node.
#[derive(Debug, Message)]
#[result_type(())]
pub enum RemoteSupervisor {
    /// Set a supervisor.
    Set(RemoteAddress),
    /// Unset a supervisor.
    Unset,
}

/// A message which is used to register/unregister an observer from a remote node.
#[derive(Debug, Message)]
#[result_type(())]
pub enum RemoteObserver {
    /// Register an observer.
    Register(RemoteAddress),
    /// Unregister an observer.
    Unregister(RemoteAddress),
}

/// A message which is used to report actor status to a supervisor from a remote node.
#[derive(Debug, Message)]
#[result_type(())]
pub enum RemoteSupervisionEvent {
    /// Warning, the actor could resume by itself.
    Warn(RemoteAddress, String),
    /// Actor terminated with or without error.
    Terminated(RemoteAddress, Option<String>),
    /// Actor state changed.
    State(RemoteAddress, ActorState),
}

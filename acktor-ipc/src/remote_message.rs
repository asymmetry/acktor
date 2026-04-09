//! Remote message which can be sent over an IPC channel.

use std::fmt::{self, Debug};

use bytes::Bytes;
use tokio::sync::oneshot;

use acktor::{ActorState, Message};

use crate::codec::DecodeContext;
use crate::remote_address::RemoteAddress;

pub use acktor::{Signal as RemoteSignal, cron::CronSignal as RemoteCronSignal};

/// The kind of remote message delivery.
pub enum RemoteMessageKind {
    /// Fire-and-forget: no response is expected.
    DoSend,
    /// Send with a reply channel: the receiver sends the response back through the
    /// [`oneshot::Sender`][tokio::sync::oneshot::Sender].
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
/// This is used both for outbound messages (sent by a local actor to a remote actor through
/// [`Session`][crate::session::Session]) and for inbound messages (received from an IPC channel
/// and forwarded to a local actor for processing).
#[derive(Message)]
#[result_type(())]
pub struct RemoteMessage {
    pub actor_id: usize,
    pub message: Bytes,
    pub kind: RemoteMessageKind,
    pub context: Option<DecodeContext>,
}

impl Debug for RemoteMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteMessage")
            .field("actor_id", &self.actor_id)
            .field("message", &format_args!("Bytes({})", self.message.len()))
            .field("kind", &self.kind)
            .field(
                "context",
                &format_args!(
                    "{}",
                    match &self.context {
                        Some(context) => format!("Session({})", context.index()),
                        None => "None".into(),
                    }
                ),
            )
            .finish()
    }
}

impl RemoteMessage {
    pub fn do_send(actor_id: usize, message: Bytes) -> Self {
        Self {
            actor_id,
            message,
            kind: RemoteMessageKind::DoSend,
            context: None,
        }
    }

    pub fn send(actor_id: usize, message: Bytes, tx: oneshot::Sender<Bytes>) -> Self {
        Self {
            actor_id,
            message,
            kind: RemoteMessageKind::Send(tx),
            context: None,
        }
    }

    /// Sets the decode context on this message.
    pub fn with_context(mut self, context: DecodeContext) -> Self {
        self.context = Some(context);
        self
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

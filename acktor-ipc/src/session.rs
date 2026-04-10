use std::fmt::{self, Debug};

use bytes::Bytes;
use futures_util::TryFutureExt;
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::oneshot;
use tracing::{Instrument, info, warn};

use acktor::{
    Actor, ActorContext, Handler, Message, Sender, SenderIndex,
    macros::{debug_trace, report},
    supervisor::SupervisionEvent,
};
use acktor_ipc_proto::{actor_message, ipc_message, node_message};

use crate::codec::{Decode, Encode};
use crate::errors::{DecodeError, SessionError};
use crate::ipc_method::IpcConnection;
use crate::node::LocalActors;
use crate::remote_address::RemoteAddress;
use crate::remote_message::{RemoteMessage, RemoteMessageKind};

pub mod command;

mod context;
use context::SessionContext;

type Result<T> = std::result::Result<T, SessionError>;

pub(crate) type RemoteActors = HashMap<String, RemoteAddress>;

#[derive(Message)]
#[result_type(())]
pub struct RemoteMessageResult {
    tag: u64,
    result: Bytes,
}

impl Debug for RemoteMessageResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteMessageResult")
            .field("tag", &self.tag)
            .field("result", &format_args!("Bytes({})", self.result.len()))
            .finish()
    }
}

/// An actor which manages the IPC connection to a remote endpoint.
pub struct Session {
    connection: Box<dyn IpcConnection>,
    local_actors: LocalActors,
    remote_actors: RemoteActors,
    tag: u64, // unique tag generator
    actor_msg_reply_map: HashMap<u64, oneshot::Sender<Bytes>>,
    node_msg_reply_map: HashMap<u64, (String, oneshot::Sender<Result<RemoteAddress>>)>,
}

impl Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("connection", &self.connection.peer_endpoint())
            .field("local_actors", &self.local_actors)
            .field("remote_actors", &self.remote_actors)
            .field("tag", &self.tag)
            .finish()
    }
}

impl Session {
    /// Constructs a new [`Session`].
    pub fn new(connection: Box<dyn IpcConnection>, local_actors: LocalActors) -> Self {
        Self {
            connection,
            local_actors,
            remote_actors: HashMap::default(),
            tag: 0,
            actor_msg_reply_map: HashMap::default(),
            node_msg_reply_map: HashMap::default(),
        }
    }

    fn next_tag(&mut self) -> u64 {
        let tag = self.tag;
        self.tag = self.tag.wrapping_add(1);
        tag
    }

    async fn send_ipc_message(&mut self, ipc_msg: ipc_message::IpcMessage) -> Result<()> {
        let encoded_ipc_msg = ipc_msg.encode_to_bytes()?;
        self.connection.send(encoded_ipc_msg).await?;
        Ok(())
    }

    async fn handle_node_message(
        &mut self,
        node_msg: node_message::NodeMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match node_msg.message {
            Some(node_message::NodeMessageType::Command(_command)) => {
                // TODO: handle incoming node commands (CreateActor, GetActor)

                Ok(())
            }

            Some(node_message::NodeMessageType::Reply(reply)) => match reply.reply {
                Some(node_message::NodeReplyType::CreateActor(
                    node_message::CreateActorResult { tag, result },
                )) => {
                    let Some((label, sender)) = self.node_msg_reply_map.remove(&tag) else {
                        return Err(SessionError::InvalidNodeMessageReplyTag(tag));
                    };

                    let result = match result {
                        Some(node_message::CreateActorResultType::Ok(actor_id))
                            if actor_id.is_remote() =>
                        {
                            Err(DecodeError::DecodeRemoteAddress.into())
                        }
                        Some(node_message::CreateActorResultType::Ok(actor_id)) => {
                            let remote_address = RemoteAddress::new(actor_id, ctx.remote_sender());
                            self.remote_actors.insert(label, remote_address.clone());

                            Ok(remote_address)
                        }
                        Some(node_message::CreateActorResultType::Err(e)) => {
                            Err(SessionError::RemoteNodeError(e))
                        }
                        _ => Err(DecodeError::from(
                            "missing field `result` in `CreateActorResult` message",
                        )
                        .into()),
                    };

                    sender
                        .send(result)
                        .map_err(|_| SessionError::SendMessageError("channel closed".into()))
                }

                Some(node_message::NodeReplyType::GetActor(node_message::GetActorResult {
                    tag,
                    result,
                })) => {
                    let Some((label, sender)) = self.node_msg_reply_map.remove(&tag) else {
                        return Err(SessionError::InvalidNodeMessageReplyTag(tag));
                    };

                    let result = match result {
                        Some(node_message::GetActorResultType::Ok(actor_id))
                            if actor_id.is_remote() =>
                        {
                            Err(DecodeError::DecodeRemoteAddress.into())
                        }
                        Some(node_message::GetActorResultType::Ok(actor_id)) => {
                            let remote_address = RemoteAddress::new(actor_id, ctx.remote_sender());
                            self.remote_actors.insert(label, remote_address.clone());

                            Ok(remote_address)
                        }
                        Some(node_message::GetActorResultType::Err(e)) => {
                            Err(SessionError::RemoteNodeError(e))
                        }
                        _ => Err(DecodeError::from(
                            "missing field `result` in `GetActorResult` message",
                        )
                        .into()),
                    };

                    sender
                        .send(result)
                        .map_err(|_| SessionError::SendMessageError("channel closed".into()))
                }

                _ => Err(DecodeError::from("missing field `reply` in `NodeReply` message").into()),
            },

            _ => Err(DecodeError::from("missing field `message` in `NodeMessage` message").into()),
        }
    }

    /// Handles an inbound remote message.
    async fn handle_actor_message(
        &mut self,
        actor_msg: actor_message::ActorMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match actor_msg.message {
            Some(actor_message::ActorMessageType::Send(send)) => {
                let actor_message::Send {
                    actor_id,
                    message,
                    tag,
                } = send;

                // TODO: we should forward any error as a remote message result back to the sender

                if actor_id.is_remote() {
                    return Err(DecodeError::DecodeRemoteAddress.into());
                }

                let Some(recipient) = self.local_actors.get(&actor_id) else {
                    // TODO: this is an error
                    return Ok(());
                };

                let (tx, rx) = oneshot::channel();

                recipient
                    .do_send(RemoteMessage {
                        actor_id,
                        message,
                        kind: RemoteMessageKind::Send(tx),
                        context: Some(ctx.decode_context()),
                    })
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))?;

                let address = ctx.address();

                tokio::spawn(
                    async move {
                        let result = rx.await?;
                        address.do_send(RemoteMessageResult { tag, result }).await?;
                        Result::Ok(())
                    }
                    .inspect_err(|e| {
                        warn!("Could not handle remote message: {}", report!(e));
                    })
                    .in_current_span(),
                );

                Ok(())
            }

            Some(actor_message::ActorMessageType::DoSend(do_send)) => {
                let actor_message::DoSend { actor_id, message } = do_send;

                if actor_id.is_remote() {
                    return Err(DecodeError::DecodeRemoteAddress.into());
                }

                let Some(recipient) = self.local_actors.get(&actor_id) else {
                    // TODO: return error
                    return Ok(());
                };

                recipient
                    .do_send(RemoteMessage {
                        actor_id,
                        message,
                        kind: RemoteMessageKind::DoSend,
                        context: Some(ctx.decode_context()),
                    })
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))
            }

            Some(actor_message::ActorMessageType::Reply(actor_message::Reply { tag, message })) => {
                if let Some(sender) = self.actor_msg_reply_map.remove(&tag) {
                    sender
                        .send(message)
                        .map_err(|_| SessionError::SendMessageError("channel closed".into()))
                } else {
                    Err(SessionError::InvalidActorMessageReplyTag(tag))
                }
            }

            _ => Err(DecodeError::from("missing field `message` in `ActorMessage` message").into()),
        }
    }

    async fn handle_ipc_message(
        &mut self,
        msg: Bytes,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let ipc_msg = ipc_message::IpcMessage::decode(msg, None)?;

        if let Some(message) = ipc_msg.message {
            match message {
                ipc_message::IpcMessageType::Node(node_msg) => {
                    self.handle_node_message(node_msg, ctx).await?;
                }
                ipc_message::IpcMessageType::Actor(actor_msg) => {
                    self.handle_actor_message(actor_msg, ctx).await?;
                }
            }
        }

        Ok(())
    }
}

impl Actor for Session {
    type Context = SessionContext;
    type Error = SessionError;

    async fn post_start(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        info!("Session {} is started", self.connection.peer_endpoint());

        Ok(())
    }

    async fn post_stop(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        self.connection.close().await?;

        info!("Session {} is stopped", self.connection.peer_endpoint());

        Ok(())
    }
}

impl Handler<command::AddActor> for Session {
    type Result = ();

    async fn handle(&mut self, msg: command::AddActor, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let recipient = msg.0;
        self.local_actors.insert(recipient.index(), recipient);
    }
}

impl Handler<command::RemoveActor> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::RemoveActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        self.local_actors.remove(&msg.0);
    }
}

impl Handler<command::CreateRemoteActor> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::CreateRemoteActor,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::CreateRemoteActor {
            label,
            r#type,
            config,
            tx,
        } = msg;

        let tag = self.next_tag();
        let ipc_msg = ipc_message::IpcMessage::node_message(
            node_message::NodeMessage::create_actor(label.clone(), r#type, config, tag),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            let _ = tx.send(Err(e));

            return;
        }

        self.node_msg_reply_map.insert(tag, (label, tx));
    }
}

impl Handler<command::GetRemoteActor> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::GetRemoteActor,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::GetRemoteActor { label, tx } = msg;

        if let Some(remote_address) = self.remote_actors.get(&label) {
            let _ = tx.send(Ok(remote_address.clone()));
            return;
        }

        let tag = self.next_tag();
        let ipc_msg = ipc_message::IpcMessage::node_message(node_message::NodeMessage::get_actor(
            label.clone(),
            tag,
        ));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            let _ = tx.send(Err(e));

            return;
        }

        self.node_msg_reply_map.insert(tag, (label, tx));
    }
}

impl Handler<RemoteMessage> for Session {
    type Result = ();

    /// Handles an outbound remote message.
    async fn handle(
        &mut self,
        msg: RemoteMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let RemoteMessage {
            actor_id,
            message,
            kind,
            ..
        } = msg;

        let (ipc_msg, tag) = match kind {
            RemoteMessageKind::Send(_) => {
                let tag = self.next_tag();
                (
                    ipc_message::IpcMessage::actor_message(actor_message::ActorMessage::send(
                        actor_id, message, tag,
                    )),
                    Some(tag),
                )
            }
            RemoteMessageKind::DoSend => (
                ipc_message::IpcMessage::actor_message(actor_message::ActorMessage::do_send(
                    actor_id, message,
                )),
                None,
            ),
        };

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), e))
                .await;

            return;
        }

        if let (Some(tag), RemoteMessageKind::Send(tx)) = (tag, kind) {
            self.actor_msg_reply_map.insert(tag, tx);
        }
    }
}

impl Handler<RemoteMessageResult> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: RemoteMessageResult,
        ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let RemoteMessageResult { tag, result } = msg;

        let ipc_msg =
            ipc_message::IpcMessage::actor_message(actor_message::ActorMessage::reply(tag, result));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), e))
                .await;
        }
    }
}

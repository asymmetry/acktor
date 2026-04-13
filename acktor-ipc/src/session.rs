use std::fmt::{self, Debug};

use ahash::HashMap;
use bytes::Bytes;
use futures_util::TryFutureExt;
use tokio::sync::oneshot;
use tracing::{Instrument, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, Handler, Message, Sender, SenderId,
    macros::{debug_trace, report},
    message::FutureMessageResult,
    supervisor::SupervisionEvent,
};
use acktor_ipc_proto::{actor_message, ipc_message, node_message};

use crate::actor_handle::ActorHandle;
use crate::codec::{Decode, DecodeContext, Encode};
use crate::double_map::DoubleMap;
use crate::errors::{DecodeError, SessionError};
use crate::ipc_method::IpcConnection;
use crate::node::{
    LabelMap,
    factory::{self, Factory},
};
use crate::remote_actor::RemoteActorRegistry;
use crate::remote_address::RemoteAddress;
use crate::remote_message::{RemoteMessage, RemoteMessageKind};

pub mod command;

mod session_ref;
pub use session_ref::SessionHandle;

mod context;
use context::SessionContext;

type Result<T> = std::result::Result<T, SessionError>;

#[derive(Message)]
#[result_type(())]
struct RemoteMessageResult {
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

#[derive(Debug, Message)]
#[result_type(())]
struct CreateActorResult {
    tag: u64,
    result: Result<ActorId>,
}

/// An actor which manages the IPC connection to a remote endpoint.
pub struct Session {
    connection: Box<dyn IpcConnection>,
    factory: Address<Factory>,
    registry: RemoteActorRegistry,
    label_map: LabelMap,
    remote_addr_map: DoubleMap<ActorId, String, RemoteAddress>,
    tag: u64, // unique tag generator
    actor_msg_reply_map: HashMap<u64, oneshot::Sender<Bytes>>,
    node_msg_reply_map: HashMap<u64, oneshot::Sender<Result<RemoteAddress>>>,
}

impl Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("connection", &self.connection.peer_endpoint())
            .field("registry", &self.registry)
            .field("label_map", &self.label_map)
            .field("remote_addr_map", &self.remote_addr_map)
            .field("tag", &self.tag)
            .finish()
    }
}

impl Session {
    /// Constructs a new [`Session`].
    pub fn new(
        connection: Box<dyn IpcConnection>,
        factory: Address<Factory>,
        registry: RemoteActorRegistry,
        label_map: LabelMap,
    ) -> Self {
        Self {
            connection,
            factory,
            registry,
            label_map,
            remote_addr_map: DoubleMap::default(),
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
        let encoded_ipc_msg = ipc_msg.encode_to_bytes(None)?;
        self.connection.send(encoded_ipc_msg).await?;

        Ok(())
    }

    fn decode_context(&self, ctx: &<Self as Actor>::Context) -> DecodeContext {
        DecodeContext::new(ctx.address(), self.registry.clone())
    }

    async fn handle_node_message(
        &mut self,
        node_msg: node_message::NodeMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match node_msg.message {
            Some(node_message::NodeMessageType::Command(node_command)) => {
                match node_command.command {
                    Some(node_message::NodeCommandType::CreateActor(
                        node_message::CreateActor {
                            label,
                            r#type,
                            config,
                            tag,
                        },
                    )) => {
                        let factory_address = self.factory.clone();
                        let address = ctx.address();

                        tokio::spawn(
                            async move {
                                let result = async {
                                    factory_address
                                        .send(factory::CreateActor {
                                            label,
                                            r#type,
                                            config,
                                        })
                                        .await?
                                        .await?
                                }
                                .await;

                                address.do_send(CreateActorResult { tag, result }).await?;

                                Result::Ok(())
                            }
                            .inspect_err(|e| {
                                warn!(
                                    "Could not send CreateActor result to sender: {}",
                                    report!(e)
                                );
                            })
                            .in_current_span(),
                        );

                        Ok(())
                    }

                    Some(node_message::NodeCommandType::GetActor(node_message::GetActor {
                        actor_handle: Some(actor_handle),
                        tag,
                    })) => {
                        let result = match &actor_handle {
                            node_message::ActorHandle::ActorId(actor_id) => self
                                .registry
                                .get(*actor_id)
                                .ok_or_else(|| SessionError::ActorNotFound(actor_id.to_string())),
                            node_message::ActorHandle::Label(label) => {
                                let actor_id = self
                                    .label_map
                                    .get(label)
                                    .ok_or_else(|| SessionError::ActorNotFound(label.clone()))?;
                                self.registry.get(*actor_id).ok_or_else(|| {
                                    SessionError::ActorNotFound(actor_id.to_string())
                                })
                            }
                        }
                        .map(|recipient| recipient.index());

                        let ipc_msg = ipc_message::IpcMessage::node_message(
                            node_message::NodeMessage::get_actor_result(tag, result),
                        );

                        if let Err(e) = self.send_ipc_message(ipc_msg).await {
                            ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), e))
                                .await;
                        }

                        Ok(())
                    }

                    _ => Err(
                        DecodeError::from("missing field `command` in `NodeCommand` message")
                            .into(),
                    ),
                }
            }

            Some(node_message::NodeMessageType::Reply(reply)) => match reply.reply {
                Some(node_message::NodeReplyType::CreateActor(
                    node_message::CreateActorResult { tag, result },
                )) => {
                    let Some(sender) = self.node_msg_reply_map.remove(&tag) else {
                        return Err(SessionError::InvalidNodeMessageReplyTag(tag));
                    };

                    let result = match result {
                        Some(node_message::CreateActorResultType::Ok(actor_id))
                            if actor_id.is_remote() =>
                        {
                            Err(DecodeError::DecodeRemoteAddress.into())
                        }

                        Some(node_message::CreateActorResultType::Ok(actor_id)) => Ok(
                            RemoteAddress::new(actor_id, ctx.address(), self.registry.clone()),
                        ),

                        Some(node_message::CreateActorResultType::Err(e)) => {
                            Err(SessionError::RemoteActorError(e))
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
                    let Some(sender) = self.node_msg_reply_map.remove(&tag) else {
                        return Err(SessionError::InvalidNodeMessageReplyTag(tag));
                    };

                    let result = match result {
                        Some(node_message::GetActorResultType::Ok(actor_id))
                            if actor_id.is_remote() =>
                        {
                            Err(DecodeError::DecodeRemoteAddress.into())
                        }

                        Some(node_message::GetActorResultType::Ok(actor_id)) => Ok(
                            RemoteAddress::new(actor_id, ctx.address(), self.registry.clone()),
                        ),

                        Some(node_message::GetActorResultType::Err(e)) => {
                            Err(SessionError::RemoteActorError(e))
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

                let rx = match async {
                    if actor_id.is_remote() {
                        return Err(DecodeError::DecodeRemoteAddress.into());
                    }

                    let Some(recipient) = self.registry.get(actor_id) else {
                        return Err(SessionError::ForwardInboundMessageFailed(
                            format!("no actor registered for id {}", actor_id).into(),
                        ));
                    };

                    let (tx, rx) = oneshot::channel();

                    recipient
                        .do_send(RemoteMessage {
                            actor_id,
                            message,
                            kind: RemoteMessageKind::Send(tx),
                            decode_context: Some(self.decode_context(ctx)),
                        })
                        .await
                        .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))?;

                    Ok(rx)
                }
                .await
                {
                    Ok(rx) => rx,
                    Err(e) => {
                        let ipc_msg = ipc_message::IpcMessage::actor_message(
                            actor_message::ActorMessage::reply::<String>(tag, Err(report!(e))),
                        );
                        self.send_ipc_message(ipc_msg).await?;

                        return Err(e);
                    }
                };

                let address = ctx.address();

                tokio::spawn(
                    async move {
                        let result = rx.await?;
                        address.do_send(RemoteMessageResult { tag, result }).await?;

                        Result::Ok(())
                    }
                    .inspect_err(|e| {
                        warn!(
                            "Could not send ActorMessage result to sender: {}",
                            report!(e)
                        );
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

                let Some(recipient) = self.registry.get(actor_id) else {
                    return Err(SessionError::ForwardInboundMessageFailed(
                        format!("no actor registered for id {}", actor_id).into(),
                    ));
                };

                recipient
                    .do_send(RemoteMessage {
                        actor_id,
                        message,
                        kind: RemoteMessageKind::DoSend,
                        decode_context: Some(self.decode_context(ctx)),
                    })
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))
            }

            Some(actor_message::ActorMessageType::Reply(actor_message::Reply { tag, result })) => {
                let Some(sender) = self.actor_msg_reply_map.remove(&tag) else {
                    return Err(SessionError::InvalidActorMessageReplyTag(tag));
                };

                match result {
                    Some(actor_message::ReplyResultType::Ok(message)) => sender
                        .send(message)
                        .map_err(|_| SessionError::SendMessageError("channel closed".into())),

                    Some(actor_message::ReplyResultType::Err(err)) => {
                        // drop(sender); // sender will be dropped when this function returns
                        Err(SessionError::RemoteActorError(err))
                    }

                    None => {
                        Err(DecodeError::from("missing field `result` in `Reply` message").into())
                    }
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

impl Handler<command::CreateRemoteActor> for Session {
    type Result = FutureMessageResult<command::CreateRemoteActor>;

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
        } = msg;

        let (tx, rx) = oneshot::channel();

        let tag = self.next_tag();
        let ipc_msg = ipc_message::IpcMessage::node_message(
            node_message::NodeMessage::create_actor(label, r#type, config, tag),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            let _ = tx.send(Err(e));
        } else {
            self.node_msg_reply_map.insert(tag, tx);
        }

        FutureMessageResult::new(async move {
            rx.await
                .map_err(|e| SessionError::SendMessageError(e.into()))?
        })
    }
}

impl Handler<command::GetRemoteActor> for Session {
    type Result = FutureMessageResult<command::GetRemoteActor>;

    async fn handle(
        &mut self,
        msg: command::GetRemoteActor,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::GetRemoteActor { actor } = msg;

        let (tx, rx) = oneshot::channel();

        let tag = self.next_tag();
        let ipc_msg = match &actor {
            ActorHandle::Index(actor_id) => ipc_message::IpcMessage::node_message(
                node_message::NodeMessage::get_actor_with_index(*actor_id, tag),
            ),
            ActorHandle::Label(label) => ipc_message::IpcMessage::node_message(
                node_message::NodeMessage::get_actor_with_label(label.clone(), tag),
            ),
        };

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            let _ = tx.send(Err(e));
        } else {
            self.node_msg_reply_map.insert(tag, tx);
        }

        FutureMessageResult::new(async move {
            rx.await
                .map_err(|e| SessionError::SendMessageError(e.into()))?
        })
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

impl Handler<CreateActorResult> for Session {
    type Result = ();

    async fn handle(&mut self, msg: CreateActorResult, ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let CreateActorResult { tag, result } = msg;

        let ipc_msg = ipc_message::IpcMessage::node_message(
            node_message::NodeMessage::create_actor_result(tag, result),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), e))
                .await;
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

        let ipc_msg = ipc_message::IpcMessage::actor_message(actor_message::ActorMessage::reply::<
            String,
        >(tag, Ok(result)));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), e))
                .await;
        }
    }
}

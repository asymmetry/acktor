use std::fmt::{self, Debug};
use std::result::Result as StdResult;

use ahash::HashMap;
use bytes::Bytes;
use futures_util::{FutureExt, TryFutureExt};
use tracing::{Instrument, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, Message, Recipient, Sender,
    SenderId, channel::oneshot, message::FutureMessageResult, utils::debug_trace,
};
use acktor_ipc_proto::{actor_message, ipc_message, node_message};

use crate::actor_handle::ActorHandle;
use crate::codec::{Decode, DecodeContext, Encode};
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

mod session_handle;
pub use session_handle::SessionHandle;

mod context;
use context::SessionContext;

type Result<T> = StdResult<T, SessionError>;

#[derive(Message)]
#[result_type(())]
struct ActorMessageReply {
    tag: u64,
    result: StdResult<Bytes, String>,
}

impl Debug for ActorMessageReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMessageReply")
            .field("tag", &self.tag)
            .field(
                "result",
                &format_args!(
                    "{}",
                    match &self.result {
                        Ok(bytes) => format!("Ok(Bytes({}))", bytes.len()),
                        Err(e) => format!("Err({})", e),
                    }
                ),
            )
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
    tag: u64, // unique tag generator
    decode_context: Option<DecodeContext>,
    node_msg_reply_map: HashMap<u64, oneshot::Sender<Result<RemoteAddress>>>,
    actor_msg_reply_map: HashMap<u64, oneshot::Sender<Bytes>>,
}

impl Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("connection", &self.connection.peer_endpoint())
            .field("factory", &self.factory)
            .field("registry", &self.registry)
            .field("label_map", &self.label_map)
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
            tag: 0,
            decode_context: None,
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
        self.connection
            .send(encoded_ipc_msg)
            .await
            .map_err(SessionError::SendOutboundMessageFailed)?;

        Ok(())
    }

    fn decode_context(&self) -> Result<&DecodeContext> {
        self.decode_context
            .as_ref()
            .ok_or_else(|| DecodeError::MissingDecodeContext.into())
    }

    fn find_actor(&self, handle: &node_message::ActorHandle) -> Result<Recipient<RemoteMessage>> {
        match &handle {
            node_message::ActorHandle::ActorId(actor_id) => self
                .registry
                .get(*actor_id)
                .ok_or_else(|| SessionError::ActorNotFound(actor_id.to_string())),

            node_message::ActorHandle::Label(label) => self
                .label_map
                .get(label)
                .ok_or_else(|| SessionError::ActorNotFound(label.clone()))
                .and_then(|actor_id| {
                    self.registry
                        .get(*actor_id)
                        .ok_or_else(|| SessionError::ActorNotFound(actor_id.to_string()))
                }),
        }
    }

    async fn handle_node_command(
        &mut self,
        command: node_message::NodeCommand,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match command.command {
            Some(node_message::NodeCommandType::CreateActor(node_message::CreateActor {
                label,
                r#type,
                config,
                tag,
            })) => {
                let factory = self.factory.clone();
                let address = ctx.address();

                // spawn a task to handle the potentially time consuming actor creation process
                tokio::spawn(
                    async move {
                        factory
                            .send(factory::CreateActor {
                                label,
                                r#type,
                                config,
                            })
                            .await?
                            .await?
                    }
                    .then(move |result| async move {
                        // send the result back to this session actor
                        // the IpcConnection can not be cloned without mutex lock
                        address.do_send(CreateActorResult { tag, result }).await
                    })
                    .inspect_err(|e| {
                        warn!(
                            "Could not send `NodeMessage::CreateActorResult` to remote peer: {}",
                            e.report()
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
                let result = self
                    .find_actor(&actor_handle)
                    .map(|recipient| recipient.index());

                let ipc_msg = ipc_message::IpcMessage::node_message(
                    node_message::NodeMessage::get_actor_result(tag, result),
                );

                self.send_ipc_message(ipc_msg).await.inspect_err(|e| {
                    warn!(
                        "Could not send `NodeMessage::GetActorResult` to remote peer: {}",
                        e.report()
                    )
                })
            }

            _ => Err(DecodeError::from("missing field `command` in `NodeCommand` message").into()),
        }
    }

    async fn handle_node_reply(&mut self, reply: node_message::NodeReply) -> Result<()> {
        match reply.reply {
            Some(node_message::NodeReplyType::CreateActor(node_message::CreateActorResult {
                tag,
                result,
            })) => {
                let sender = self
                    .node_msg_reply_map
                    .remove(&tag)
                    .ok_or(SessionError::InvalidNodeMessageReplyTag(tag))?;

                let result = match result {
                    Some(node_message::CreateActorResultType::Ok(actor_id)) => self
                        .decode_context()?
                        .create_remote_address(actor_id)
                        .map_err(Into::into),

                    Some(node_message::CreateActorResultType::Err(e)) => {
                        Err(SessionError::RemotePeerError(e))
                    }

                    _ => Err(DecodeError::from(
                        "missing field `result` in `CreateActorResult` message",
                    )
                    .into()),
                };

                sender
                    .send(result)
                    .map_err(|_| SessionError::ForwardNodeMessageReplyFailed)
            }

            Some(node_message::NodeReplyType::GetActor(node_message::GetActorResult {
                tag,
                result,
            })) => {
                let sender = self
                    .node_msg_reply_map
                    .remove(&tag)
                    .ok_or(SessionError::InvalidNodeMessageReplyTag(tag))?;

                let result = match result {
                    Some(node_message::GetActorResultType::Ok(actor_id)) => self
                        .decode_context()?
                        .create_remote_address(actor_id)
                        .map_err(Into::into),

                    Some(node_message::GetActorResultType::Err(e)) => {
                        Err(SessionError::RemotePeerError(e))
                    }

                    _ => Err(DecodeError::from(
                        "missing field `result` in `GetActorResult` message",
                    )
                    .into()),
                };

                sender
                    .send(result)
                    .map_err(|_| SessionError::ForwardNodeMessageReplyFailed)
            }

            _ => Err(DecodeError::from("missing field `reply` in `NodeReply` message").into()),
        }
    }

    fn find_actor_to_forward(&self, actor_id: ActorId) -> Result<Recipient<RemoteMessage>> {
        if actor_id.is_remote() {
            return Err(DecodeError::DecodeRemoteAddress.into());
        }

        self.registry.get(actor_id).ok_or_else(|| {
            SessionError::ForwardInboundMessageFailed(
                format!("no actor registered for id {}", actor_id).into(),
            )
        })
    }

    /// Handles an inbound remote message.
    async fn handle_actor_message(
        &mut self,
        message: actor_message::ActorMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match message.message {
            Some(actor_message::ActorMessageType::Send(send)) => {
                let actor_message::Send {
                    actor_id,
                    message,
                    tag,
                } = send;

                let address = ctx.address();

                let recipient = self.find_actor_to_forward(actor_id);

                let decode_context = self.decode_context()?.clone();

                let (tx, rx) = oneshot::channel();

                // spawn a task to handle the potentially time consuming message handling process
                tokio::spawn(
                    async move {
                        recipient?
                            .do_send(
                                RemoteMessage::send(actor_id, message, tx)
                                    .with_context(decode_context),
                            )
                            .await
                            .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))?;

                        let result = rx
                            .await
                            .map_err(|e| SessionError::HandleInboundMessageFailed(e.into()))?;

                        Ok::<Bytes, SessionError>(result)
                    }
                    .then(move |result| async move {
                        let result = ActorMessageReply {
                            tag,
                            result: result.map_err(|e| e.report()),
                        };

                        address.do_send(result).await
                    })
                    .inspect_err(|e| {
                        warn!(
                            "Could not send `ActorMessage::Reply` to remote peer: {}",
                            e.report()
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
                    .do_send(
                        RemoteMessage::do_send(actor_id, message)
                            .with_context(self.decode_context()?.clone()),
                    )
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))
            }

            Some(actor_message::ActorMessageType::Reply(actor_message::Reply { tag, result })) => {
                let sender = self
                    .actor_msg_reply_map
                    .remove(&tag)
                    .ok_or(SessionError::InvalidActorMessageReplyTag(tag))?;

                let result: Result<_> = match result {
                    Some(actor_message::ReplyResultType::Ok(message)) => Ok(message),

                    Some(actor_message::ReplyResultType::Err(err)) => {
                        Err(SessionError::RemotePeerError(err))
                    }

                    None => {
                        Err(DecodeError::from("missing field `result` in `Reply` message").into())
                    }
                };

                match result {
                    Ok(bytes) => sender
                        .send(bytes)
                        .map_err(|_| SessionError::ForwardActorMessageReplyFailed),
                    Err(e) => sender
                        .send_err(e)
                        .map_err(|_| SessionError::ForwardActorMessageReplyFailed),
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

        match ipc_msg.message {
            Some(ipc_message::IpcMessageType::Node(message)) => match message.message {
                Some(node_message::NodeMessageType::Command(command)) => {
                    self.handle_node_command(command, ctx).await
                }
                Some(node_message::NodeMessageType::Reply(reply)) => {
                    self.handle_node_reply(reply).await
                }
                _ => Err(
                    DecodeError::from("missing field `message` in `NodeMessage` message").into(),
                ),
            },
            Some(ipc_message::IpcMessageType::Actor(message)) => {
                self.handle_actor_message(message, ctx).await
            }
            _ => Err(DecodeError::from("missing field `message` in `IpcMessage` message").into()),
        }
    }
}

impl Actor for Session {
    type Context = SessionContext;
    type Error = SessionError;

    async fn post_start(&mut self, ctx: &mut Self::Context) -> Result<()> {
        info!("Session {} is started", self.connection.peer_endpoint());

        self.decode_context = Some(DecodeContext::new(ctx.address(), self.registry.clone()));

        Ok(())
    }

    async fn post_stop(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        self.connection
            .close()
            .await
            .map_err(SessionError::IoError)?;

        info!("Session {} is stopped", self.connection.peer_endpoint());

        Ok(())
    }
}

/// See [`handle_node_command`][Session::handle_node_command] for what remote session actor will
/// do when it receives the IpcMessage sent by this handler.
/// See [`handle_node_reply`][Session::handle_node_reply] for how this session actor sends the
/// result with the `tx` created in this handler when it receives the corresponding NodeReply
/// from the remote peer.
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
            warn!(
                "Could not send `NodeMessage::CreateActor` to remote peer: {}",
                e.report()
            );
            let _ = tx.send(Err(e));
        } else {
            self.node_msg_reply_map.insert(tag, tx);
        }

        FutureMessageResult::new(rx.map(|r| r.unwrap_or_else(|e| Err(e.into()))))
    }
}

/// See [`handle_node_command`][Session::handle_node_command] for what remote session actor will
/// do when it receives the IpcMessage sent by this handler.
/// See [`handle_node_reply`][Session::handle_node_reply] for how this session actor sends the
/// result with the `tx` created in this handler when it receives the corresponding NodeReply
/// from the remote peer.
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
            warn!(
                "Could not send `NodeMessage::GetActor` to remote peer: {}",
                e.report()
            );
            let _ = tx.send(Err(e));
        } else {
            self.node_msg_reply_map.insert(tag, tx);
        }

        FutureMessageResult::new(rx.map(|r| r.unwrap_or_else(|e| Err(e.into()))))
    }
}

impl Handler<RemoteMessage> for Session {
    type Result = ();

    /// Handles an outbound remote message.
    async fn handle(
        &mut self,
        msg: RemoteMessage,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let RemoteMessage {
            actor_id,
            message,
            kind,
            ..
        } = msg;

        match kind {
            RemoteMessageKind::Send(tx) => {
                let tag = self.next_tag();
                let ipc_msg = ipc_message::IpcMessage::actor_message(
                    actor_message::ActorMessage::send(actor_id, message, tag),
                );

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    warn!(
                        "Could not send `ActorMessage::Send` to remote peer: {}",
                        e.report()
                    );
                    let _ = tx.send_err(e);

                    return;
                }

                self.actor_msg_reply_map.insert(tag, tx);
            }
            RemoteMessageKind::DoSend => {
                let ipc_msg = ipc_message::IpcMessage::actor_message(
                    actor_message::ActorMessage::do_send(actor_id, message),
                );

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    warn!(
                        "Could not send `ActorMessage::DoSend` to remote peer: {}",
                        e.report()
                    );
                }
            }
        }
    }
}

impl Handler<CreateActorResult> for Session {
    type Result = ();

    async fn handle(&mut self, msg: CreateActorResult, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let CreateActorResult { tag, result } = msg;

        let ipc_msg = ipc_message::IpcMessage::node_message(
            node_message::NodeMessage::create_actor_result(tag, result),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `NodeMessage::CreateActorResult` to remote peer: {}",
                e.report()
            )
        }
    }
}

impl Handler<ActorMessageReply> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: ActorMessageReply,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let ActorMessageReply { tag, result } = msg;

        let ipc_msg =
            ipc_message::IpcMessage::actor_message(actor_message::ActorMessage::reply(tag, result));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `ActorMessage::Reply` to remote peer: {}",
                e.report()
            )
        }
    }
}

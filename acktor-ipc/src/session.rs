use std::fmt::{self, Debug};
use std::result::Result as StdResult;

use ahash::HashMap;
use bytes::Bytes;
use futures_util::{FutureExt, TryFutureExt};
use tokio::time::{Duration, Instant};
use tracing::{Instrument, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, Message, Recipient, Sender,
    SenderId, channel::oneshot, message::FutureMessageResult, utils::debug_trace,
};
use acktor_ipc_proto::{actor_message, ipc_message, node_message, utils as proto_utils};

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
use crate::remote_message::RemoteMessage;

pub mod command;

mod session_handle;
pub use session_handle::SessionHandle;

mod context;
use context::SessionContext;

type Result<T> = StdResult<T, SessionError>;

#[derive(Message)]
#[result_type(())]
struct ActorMessageResponse {
    tag: u64,
    result: StdResult<Bytes, String>,
}

impl Debug for ActorMessageResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorMessageResponse")
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
struct CreateActorResponse {
    tag: u64,
    result: StdResult<ActorId, String>,
}

/// An actor which manages the IPC connection to a remote endpoint.
pub struct Session {
    connection: Box<dyn IpcConnection>,
    factory: Address<Factory>,
    registry: RemoteActorRegistry,
    label_map: LabelMap,
    tag: u64, // unique tag generator
    decode_context: Option<DecodeContext>,
    node_msg_res_tx_map: HashMap<u64, (oneshot::Sender<Result<RemoteAddress>>, Instant)>,
    actor_msg_res_tx_map: HashMap<u64, (oneshot::Sender<Bytes>, Instant)>,
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
            actor_msg_res_tx_map: HashMap::default(),
            node_msg_res_tx_map: HashMap::default(),
        }
    }

    fn cleanup_expired_res_tx(&mut self) {
        let now = Instant::now();

        self.node_msg_res_tx_map
            .retain(|_, (_, timestamp)| now.duration_since(*timestamp) < Duration::from_secs(60));
        self.actor_msg_res_tx_map
            .retain(|_, (_, timestamp)| now.duration_since(*timestamp) < Duration::from_secs(60));
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

    fn find_actor(&self, actor: &ActorHandle) -> Result<Recipient<RemoteMessage>> {
        match actor {
            ActorHandle::Index(actor_id) => {
                if actor_id.is_remote() {
                    return Err(DecodeError::DecodeRemoteAddress.into());
                }

                self.registry
                    .get(*actor_id)
                    .ok_or_else(|| SessionError::ActorNotFound(actor_id.to_string()))
            }

            ActorHandle::Label(label) => self
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

    async fn handle_node_message(
        &mut self,
        message: node_message::NodeMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match message.message {
            Some(node_message::MessageType::CreateActor(node_message::CreateActor {
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
                        // send the result back to this actor with CreateActorResponse message
                        // the IpcConnection can not be cloned into the spawned task without a
                        // Arc<Mutex<..>>, so we convert this into a sequential message handling
                        // process
                        address
                            .do_send(CreateActorResponse {
                                tag,
                                result: result.map_err(|e| e.report()),
                            })
                            .await
                    })
                    .inspect_err(|e| {
                        // we can not do much if sending the response back to this actor fails,
                        // just log it
                        warn!(
                            "Could not send `NodeMessageResponse::CreateActor` to remote peer: {}",
                            e.report()
                        );
                    })
                    .in_current_span(),
                );

                Ok(())
            }

            Some(node_message::MessageType::GetActor(node_message::GetActor { actor, tag })) => {
                // convert proto::utils::ActorHandle to crate::ActorHandle, ugly
                let actor = match actor {
                    Some(proto_utils::ActorHandle { handle }) => match handle {
                        Some(proto_utils::ActorHandleType::Index(actor_id)) => {
                            ActorHandle::Index(actor_id)
                        }
                        Some(proto_utils::ActorHandleType::Label(label)) => {
                            ActorHandle::Label(label)
                        }
                        None => {
                            return Err(DecodeError::from(
                                "missing field `handle` in `ActorHandle`",
                            )
                            .into());
                        }
                    },

                    _ => {
                        return Err(DecodeError::from(
                            "missing field `actor` in `NodeMessage::GetActor`",
                        )
                        .into());
                    }
                };

                let result = self.find_actor(&actor).map(|recipient| recipient.index());

                let ipc_msg = ipc_message::IpcMessage::node_message_response(
                    node_message::NodeMessageResponse::get_actor(
                        tag,
                        result.map_err(|e| e.report()),
                    ),
                );

                self.send_ipc_message(ipc_msg).await.inspect_err(|e| {
                    // we can not do much if sending the response back to the remote peer fails,
                    // just log it
                    warn!(
                        "Could not send `NodeMessageResponse::GetActor` to remote peer: {}",
                        e.report()
                    );
                })
            }

            _ => Err(DecodeError::from("missing field `message` in `NodeMessage`").into()),
        }
    }

    fn _handle_node_message_response(
        &mut self,
        tag: u64,
        result: Option<node_message::ResultType>,
        name: &str,
    ) -> Result<()> {
        let (sender, _) = self
            .node_msg_res_tx_map
            .remove(&tag)
            // if the tag is not found in the map, we do not know who to send the result to, and
            // we do not kown who to report the error to either, so just return an error and the
            // session's context will log it
            .ok_or(SessionError::InvalidNodeMsgResTxTag(tag))?;

        // remote error and processing error should be reported to the original sender who is
        // waiting for a `Result<RemoteAddress, SessionError>`
        let result = match result {
            Some(node_message::ResultType::ActorId(actor_id)) => match self.decode_context() {
                Ok(ctx) => ctx.create_remote_address(actor_id).map_err(Into::into),
                Err(e) => Err(e),
            },
            Some(node_message::ResultType::Err(e)) => Err(SessionError::RemotePeerError(e)),

            _ => Err(DecodeError::from(format!(
                "missing field `result` in `NodeMessageResponse::{}`",
                name
            ))
            .into()),
        };

        sender
            .send(result)
            // we can not do much if sending the result back to the original sender fails, just
            // return an error and the session's context will log it
            .map_err(|_| SessionError::ForwardNodeMsgResFailed)
    }

    fn handle_node_message_response(
        &mut self,
        response: node_message::NodeMessageResponse,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let tag = response.tag;

        match response.response {
            Some(node_message::ResponseType::CreateActor(node_message::ResultRemoteAddress {
                result,
            })) => self._handle_node_message_response(tag, result, "CreateActor"),

            Some(node_message::ResponseType::GetActor(node_message::ResultRemoteAddress {
                result,
            })) => self._handle_node_message_response(tag, result, "GetActor"),

            _ => Err(DecodeError::from("missing field `response` in `NodeMessageResponse`").into()),
        }
    }

    async fn handle_actor_message(
        &mut self,
        message: actor_message::ActorMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let actor_message::ActorMessage {
            actor_id,
            message_id,
            message,
            tag,
        } = message;

        match tag {
            Some(tag) => {
                // send

                let address = ctx.address();
                let recipient = self.find_actor(&ActorHandle::Index(actor_id));
                let decode_context = self.decode_context().cloned();
                let (tx, rx) = oneshot::channel();

                // spawn a task to handle the potentially time consuming message handling process
                tokio::spawn(
                    async move {
                        recipient?
                            .do_send(
                                RemoteMessage::send(actor_id, message_id, message, tx)
                                    .with_context(decode_context?),
                            )
                            .await
                            .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))?;

                        let result = rx
                            .await
                            .map_err(|e| SessionError::HandleInboundMessageFailed(e.into()))?;

                        Ok::<Bytes, SessionError>(result)
                    }
                    .then(move |result| async move {
                        // send the result back to this actor with ActorMessageResponse message
                        // the IpcConnection can not be cloned into the spawned task without a
                        // Arc<Mutex<..>>, so we convert this into a sequential message handling
                        // process
                        address
                            .do_send(ActorMessageResponse {
                                tag,
                                result: result.map_err(|e| e.report()),
                            })
                            .await
                    })
                    .inspect_err(|e| {
                        // we can not do much if sending the response back to this actor fails,
                        // just log it
                        warn!(
                            "Could not send `ActorMessageResponse` to remote peer: {}",
                            e.report()
                        );
                    })
                    .in_current_span(),
                );

                Ok(())
            }

            None => {
                // do_send

                // sender has explicitly indicated that it does not care about the result of this
                // message, so we just return the error and the session's context will log it
                self.find_actor(&ActorHandle::Index(actor_id))?
                    .do_send(
                        RemoteMessage::do_send(actor_id, message_id, message)
                            .with_context(self.decode_context()?.clone()),
                    )
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))
            }
        }
    }

    async fn handle_actor_message_response(
        &mut self,
        response: actor_message::ActorMessageResponse,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let tag = response.tag;

        let (sender, _) = self
            .actor_msg_res_tx_map
            .remove(&tag)
            // if the tag is not found in the map, we do not know who to send the result
            // to, and we do not kown who to report the error to either, so just return
            // an error and the session's context will log it
            .ok_or(SessionError::InvalidActorMsgResTxTag(tag))?;

        // remote error and processing error should be reported to the original sender who is
        // waiting for a `Result<M::Result, RecvError>`
        let result: Result<_> = match response.response {
            Some(actor_message::ResponseType::Ok(bytes)) => Ok(bytes),
            Some(actor_message::ResponseType::Err(err)) => Err(SessionError::RemotePeerError(err)),
            None => {
                Err(DecodeError::from("missing field `response` in `ActorMessageResponse`").into())
            }
        };

        // we can not do much if sending the result back to the original sender fails, just return
        // an error and the session's context will log it
        match result {
            Ok(bytes) => sender
                .send(bytes)
                .map_err(|_| SessionError::ForwardActorMessageResFailed),
            Err(e) => sender
                .send_err(e)
                .map_err(|_| SessionError::ForwardActorMessageResFailed),
        }
    }

    async fn handle_ipc_message(
        &mut self,
        message: Bytes,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let ipc_message = ipc_message::IpcMessage::decode(message, None)?;

        match ipc_message.message {
            Some(ipc_message::IpcMessageType::NodeMessage(message)) => {
                self.handle_node_message(message, ctx).await
            }
            Some(ipc_message::IpcMessageType::NodeMessageResponse(response)) => {
                self.handle_node_message_response(response, ctx)
            }
            Some(ipc_message::IpcMessageType::ActorMessage(message)) => {
                self.handle_actor_message(message, ctx).await
            }
            Some(ipc_message::IpcMessageType::ActorMessageResponse(response)) => {
                self.handle_actor_message_response(response, ctx).await
            }
            _ => Err(DecodeError::from("missing field `message` in `IpcMessage`").into()),
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

/// See [`handle_node_message`][Session::handle_node_message] for what the remote peer actor will
/// do when it receives the `NodeMessage` sent by this handler.
/// See [`handle_node_message_response`][Session::handle_node_message_response] for how this actor
/// forwards the result to the original sender when it receives the `NodeMessageResponse` from the
/// remote peer actor.
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
            // sends the error back to the original sender who is waiting for a
            // `Result<RemoteAddress, SessionError>`
            if let Err(e) = tx.send(Err(e)) {
                // we can not do much if sending the error back to the original sender fails, just
                // log it
                warn!(
                    "Could not report the error in `Handler<CreateRemoteActor>` to original\
                     sender: {}",
                    e.report()
                );
            }
        } else {
            self.node_msg_res_tx_map.insert(tag, (tx, Instant::now()));
        }

        FutureMessageResult::new(rx.map(|r| r.unwrap_or_else(|e| Err(e.into()))))
    }
}

/// See [`handle_node_message`][Session::handle_node_message] for what the remote peer actor will
/// do when it receives the `NodeMessage` sent by this handler.
/// See [`handle_node_message_response`][Session::handle_node_message_response] for how this actor
/// forwards the result to the original sender when it receives the `NodeMessageResponse` from the
/// remote peer actor.
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
            // sends the error back to the original sender who is waiting for a
            // `Result<RemoteAddress, SessionError>`
            if let Err(e) = tx.send(Err(e)) {
                // we can not do much if sending the error back to the original sender fails, just
                // log it
                warn!(
                    "Could not report the error in `Handler<GetRemoteActor>` to original sender:\
                     {}",
                    e.report()
                );
            }
        } else {
            self.node_msg_res_tx_map.insert(tag, (tx, Instant::now()));
        }

        FutureMessageResult::new(rx.map(|r| r.unwrap_or_else(|e| Err(e.into()))))
    }
}

/// See [`handle_actor_message`][Session::handle_actor_message] for what the remote peer actor
/// will do when it receives the `ActorMessage` sent by this handler.
/// See [`handle_actor_message_response`][Session::handle_actor_message_response] for how this
/// actor forwards the result to the original sender when it receives the `ActorMessageResponse`
/// from the remote peer actor.
impl Handler<RemoteMessage> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: RemoteMessage,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let RemoteMessage {
            actor_id,
            message_id,
            message,
            result_tx,
            ..
        } = msg;

        match result_tx {
            Some(tx) => {
                // send

                let tag = self.next_tag();
                let ipc_msg = ipc_message::IpcMessage::actor_message(
                    actor_message::ActorMessage::send(actor_id, message_id, message, tag),
                );

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    warn!(
                        "Could not send `ActorMessage` to remote peer: {}",
                        e.report()
                    );
                    // sends the error back to the original sender who is waiting for a
                    // `Result<M::Result, RecvError>`, typically a `RemoteAddress`, note that the
                    // error original sender receives is a `RecvError` because the signature of
                    // the `Sender` trait so we need to use `send_err` here
                    if let Err(e) = tx.send_err(e) {
                        // we can not do much if sending the error back to the original sender
                        // fails, just log it
                        warn!(
                            "Could not report the error in `Handler<RemoteMessage>` to original\
                             sender: {}",
                            e.report()
                        );
                    }

                    return;
                }

                self.actor_msg_res_tx_map.insert(tag, (tx, Instant::now()));
            }

            None => {
                // do_send

                let ipc_msg = ipc_message::IpcMessage::actor_message(
                    actor_message::ActorMessage::do_send(actor_id, message_id, message),
                );

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    // sender has explicitly indicated that it does not care about the result of
                    // this message, so we just log the error
                    warn!(
                        "Could not do_send `ActorMessage` to remote peer: {}",
                        e.report()
                    );
                }
            }
        }
    }
}

impl Handler<CreateActorResponse> for Session {
    type Result = ();

    async fn handle(&mut self, msg: CreateActorResponse, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let CreateActorResponse { tag, result } = msg;

        let ipc_msg = ipc_message::IpcMessage::node_message_response(
            node_message::NodeMessageResponse::create_actor(tag, result),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            // we can not do much if sending the response back to the remote peer fails, just log
            // it
            warn!(
                "Could not send `NodeMessageResponse::CreateActor` to remote peer: {}",
                e.report()
            )
        }
    }
}

impl Handler<ActorMessageResponse> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: ActorMessageResponse,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let ActorMessageResponse { tag, result } = msg;

        let ipc_msg = ipc_message::IpcMessage::actor_message_response(
            actor_message::ActorMessageResponse::new(tag, result),
        );

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `ActorMessageResponse` to remote peer: {}",
                e.report()
            )
        }
    }
}

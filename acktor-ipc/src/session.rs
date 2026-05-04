//! Per-connection session actor.
//!
//! A [`Session`] wraps a single [`IpcConnection`] and mediates all traffic over it: routing
//! inbound frames to local actors, forwarding outbound messages from [`RemoteAddress`]es, and
//! tracking pending request tags for response correlation. Sessions are owned by a
//! [`Node`][crate::node::Node] and are created through it rather than directly.
//!

use std::fmt::{self, Debug};
use std::result::Result as StdResult;
use std::sync::Arc;

use ahash::HashMap;
use bytes::Bytes;
use futures_util::{FutureExt, TryFutureExt};
use tokio::time::{Duration, Instant};
use tracing::{Instrument, debug, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, Message, Sender, SenderInfo,
    channel::oneshot,
    message::FutureMessageResult,
    utils::{ShortName, debug_trace},
};
use acktor_ipc_proto::{message, utils as proto_utils};

use crate::actor_ref::ActorRef;
use crate::codec::{Decode, DecodeContext, DecodeError, Encode, EncodeContext};
use crate::error::SessionError;
use crate::ipc_method::IpcConnection;
use crate::node::actor_mgr::{self, ActorMgr};
use crate::remote::{
    BinaryMessage, RemoteAddressable, RemoteMailbox, RemoteMailboxRegistry, RemoteSpawnable,
};

pub mod command;

mod session_ref;
pub use session_ref::SessionRef;

mod context;
use context::SessionContext;

mod proxy;
use proxy::Proxy;

type Result<T> = StdResult<T, SessionError>;

/// How long a pending request may sit in the response maps before the cleanup sweep resolves
/// it with [`SessionError::ResponseTimeout`]. The observable timeout is up to
/// `RESPONSE_TIMEOUT + CLEANUP_INTERVAL` because the sweep is periodic.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the session runs the response-map cleanup sweep.
pub(crate) const CLEANUP_INTERVAL: Duration = Duration::from_secs(15);

struct MessageResponse {
    tag: u64,
    result: StdResult<Bytes, String>,
}

impl Debug for MessageResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MessageResponse").field(&self.tag).finish()
    }
}

impl Message for MessageResponse {
    type Result = ();
}

/// An actor which manages the IPC connection to a remote endpoint.
pub struct Session {
    connection: Box<dyn IpcConnection>,
    registry: RemoteMailboxRegistry,
    actor_mgr: Address<ActorMgr>,
    tag: u64, // unique tag generator
    proxy: Option<Arc<Proxy>>,
    message_res_tx_map: HashMap<u64, (oneshot::Sender<Bytes>, Instant)>,
}

impl Session {
    /// Constructs a new [`Session`]. Called internally by [`Node`][crate::node::Node] when it
    /// accepts or initiates an IPC connection; not intended for direct use.
    pub(crate) fn new(
        connection: Box<dyn IpcConnection>,
        registry: RemoteMailboxRegistry,
        actor_mgr: Address<ActorMgr>,
    ) -> Self {
        Self {
            connection,
            registry,
            actor_mgr,
            tag: 0,
            proxy: None,
            message_res_tx_map: HashMap::default(),
        }
    }

    // TODO: once MSRV is bumped to 1.88, rewrite the two collect-then-remove loops with
    // `HashMap::extract_if`.
    fn cleanup_message_res_tx_map(&mut self) {
        let now = Instant::now();

        let tags_to_remove = self
            .message_res_tx_map
            .iter()
            .filter_map(|(tag, (tx, timestamp))| {
                if tx.is_closed() || now.duration_since(*timestamp) >= RESPONSE_TIMEOUT {
                    Some(*tag)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for tag in tags_to_remove {
            if let Some((tx, _)) = self.message_res_tx_map.remove(&tag) {
                if tx.is_closed() {
                    debug!(
                        "The sender of message with tag {} has closed the response rx, \
                         remove the corresponding response tx",
                        tag
                    );
                } else {
                    let _ = tx.send_err(SessionError::ResponseTimeout);
                }
            }
        }
    }

    fn next_tag(&mut self) -> u64 {
        let tag = self.tag;
        self.tag = self.tag.wrapping_add(1);
        tag
    }

    async fn send_ipc_message(&mut self, ipc_msg: message::IpcMessage) -> Result<()> {
        let encoded_ipc_msg = ipc_msg.encode_to_bytes(None)?;
        self.connection
            .send(encoded_ipc_msg)
            .await
            .map_err(SessionError::SendOutboundMessageFailed)?;

        Ok(())
    }

    fn encode_context(&self) -> Arc<dyn EncodeContext + Send + Sync> {
        self.proxy
            .clone()
            .expect("proxy is always available after session is started")
            as Arc<dyn EncodeContext + Send + Sync>
    }

    fn decode_context(&self) -> Arc<dyn DecodeContext + Send + Sync> {
        self.proxy
            .clone()
            .expect("proxy is always available after session is started")
            as Arc<dyn DecodeContext + Send + Sync>
    }

    fn find_mailbox(&self, actor_id: ActorId) -> Result<RemoteMailbox> {
        if actor_id.is_remote() {
            return Err(DecodeError::DecodeRemoteAddress.into());
        }

        self.registry
            .get(actor_id.as_local())
            .ok_or_else(|| SessionError::ActorNotFound(actor_id.to_string()))
    }

    async fn find_mailbox_by_label(&self, label: String) -> Result<RemoteMailbox> {
        self.actor_mgr
            .send(actor_mgr::GetActor { label })
            .await?
            .await?
    }

    async fn handle_node_message(
        &mut self,
        message: message::NodeMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        match message.message {
            Some(message::NodeMessageType::CreateActor(message::CreateActor {
                type_id,
                label,
                config,
                tag,
            })) => {
                let actor_mgr = self.actor_mgr.clone();
                let address = ctx.address();

                // spawn a task to handle the potentially time consuming actor creation process
                tokio::spawn(
                    async move {
                        actor_mgr
                            .send(actor_mgr::CreateActor {
                                type_id,
                                label,
                                config,
                            })
                            .await?
                            .await?
                    }
                    .then(move |result| async move {
                        let result = match result {
                            Ok(actor_id) => proto_utils::ResultAddress::ok(actor_id.as_local()),
                            Err(e) => proto_utils::ResultAddress::err(e.report()),
                        };

                        let bytes: Bytes = prost::Message::encode_to_vec(&result).into();

                        // send the result back to this actor with the MessageResponse message
                        // the IpcConnection can not be cloned into the spawned task without a
                        // Arc<Mutex<..>>, so we convert this into a sequential message handling
                        // process
                        address
                            .do_send(MessageResponse {
                                tag,
                                result: Ok(bytes),
                            })
                            .await
                    })
                    .inspect_err(|e| {
                        // we can not do much if sending the response back to this actor fails,
                        // just log it
                        warn!(
                            "Could not send `NodeMessageResponse::CreateActor` to remote node: {}",
                            e.report()
                        );
                    })
                    .in_current_span(),
                );

                Ok(())
            }

            Some(message::NodeMessageType::GetActor(message::GetActor { actor, tag })) => {
                let result = match actor {
                    Some(proto_utils::ActorRef { r#ref }) => match r#ref {
                        Some(proto_utils::ActorRefType::Index(actor_id)) => {
                            self.find_mailbox(ActorId::new(actor_id))
                        }
                        Some(proto_utils::ActorRefType::Label(label)) => {
                            self.find_mailbox_by_label(label).await
                        }
                        None => Err(DecodeError::from("missing field `ref` in `ActorRef`").into()),
                    },
                    _ => Err(
                        DecodeError::from("missing field `actor` in `NodeMessage::GetActor`")
                            .into(),
                    ),
                };

                let result = match result {
                    Ok(mailbox) => proto_utils::ResultAddress::ok(mailbox.index().as_local()),
                    Err(e) => proto_utils::ResultAddress::err(e.report()),
                };

                let bytes: Bytes = prost::Message::encode_to_vec(&result).into();
                let ipc_msg = message::IpcMessage::message_response(message::MessageResponse::new(
                    tag,
                    Ok(bytes),
                ));

                self.send_ipc_message(ipc_msg).await.inspect_err(|e| {
                    // we can not do much if sending the response back to the remote node fails,
                    // just log it
                    warn!(
                        "Could not send `NodeMessageResponse::GetActor` to remote node: {}",
                        e.report()
                    );
                })
            }

            _ => Err(DecodeError::from("missing field `message` in `NodeMessage`").into()),
        }
    }

    async fn handle_actor_message(
        &mut self,
        message: message::ActorMessage,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let message::ActorMessage {
            actor_id,
            message_id,
            message,
            tag,
        } = message;

        let actor_id = ActorId::new(actor_id);

        match tag {
            Some(tag) => {
                // send

                let address = ctx.address();
                let recipient = self.find_mailbox(actor_id);
                let (tx, rx) = oneshot::channel();
                let message = BinaryMessage::send(actor_id, message_id, message, tx)
                    .with_encode_context(self.encode_context())
                    .with_decode_context(self.decode_context());

                // spawn a task to handle the potentially time consuming message handling process
                tokio::spawn(
                    async move {
                        recipient?
                            .do_send(message)
                            .await
                            .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))?;

                        let result = rx
                            .await
                            .map_err(|e| SessionError::HandleInboundMessageFailed(e.into()))?;

                        Ok::<Bytes, SessionError>(result)
                    }
                    .then(move |result| async move {
                        // send the result back to this actor with the MessageResponse message
                        // the IpcConnection can not be cloned into the spawned task without a
                        // Arc<Mutex<..>>, so we convert this into a sequential message handling
                        // process
                        address
                            .do_send(MessageResponse {
                                tag,
                                result: result.map_err(|e| e.report()),
                            })
                            .await
                    })
                    .inspect_err(|e| {
                        // we can not do much if sending the response back to this actor fails,
                        // just log it
                        warn!(
                            "Could not send `ActorMessageResponse` to remote node: {}",
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
                self.find_mailbox(actor_id)?
                    .do_send(
                        BinaryMessage::do_send(actor_id, message_id, message)
                            .with_decode_context(self.decode_context()),
                    )
                    .await
                    .map_err(|e| SessionError::ForwardInboundMessageFailed(e.into()))
            }
        }
    }

    fn handle_message_response(
        &mut self,
        response: message::MessageResponse,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let tag = response.tag;

        let (sender, _) = self
            .message_res_tx_map
            .remove(&tag)
            // if the tag is not found in the map, we do not know who to send the result
            // to, and we do not know who to report the error to either, so just return
            // an error and the session's context will log it
            .ok_or(SessionError::InvalidMessageResTxTag(tag))?;

        // remote error and processing error should be reported to the original sender who is
        // waiting for a `Result<M::Result, RecvError>`
        let result: Result<_> = match response.response {
            Some(message::ResponseType::Ok(bytes)) => Ok(bytes),
            Some(message::ResponseType::Err(err)) => Err(SessionError::RemoteNodeError(err)),
            None => Err(DecodeError::from("missing field `response` in `MessageResponse`").into()),
        };

        // we can not do much if sending the result back to the original sender fails, just return
        // an error and the session's context will log it
        match result {
            Ok(bytes) => sender
                .send(bytes)
                .map_err(|_| SessionError::ForwardMessageResFailed),
            Err(e) => sender
                .send_err(e)
                .map_err(|_| SessionError::ForwardMessageResFailed),
        }
    }

    async fn handle_ipc_message(
        &mut self,
        message: Bytes,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let ipc_message = message::IpcMessage::decode(message, None)?;

        match ipc_message.message {
            Some(message::IpcMessageType::NodeMessage(message)) => {
                self.handle_node_message(message, ctx).await
            }
            Some(message::IpcMessageType::ActorMessage(message)) => {
                self.handle_actor_message(message, ctx).await
            }
            Some(message::IpcMessageType::MessageResponse(response)) => {
                self.handle_message_response(response, ctx)
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

        self.proxy = Some(Proxy::new(ctx.address(), self.registry.clone()));

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

impl<A> Handler<command::RemoteCreateActor<A>> for Session
where
    A: Actor + RemoteSpawnable,
{
    type Result = FutureMessageResult<command::RemoteCreateActor<A>>;

    async fn handle(
        &mut self,
        msg: command::RemoteCreateActor<A>,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command RemoteCreateActor<{}>", ShortName::of::<A>());

        let command::RemoteCreateActor { label, config, .. } = msg;

        let (tx, rx) = oneshot::channel();

        let tag = self.next_tag();
        let ipc_msg = message::IpcMessage::node_message(message::NodeMessage::create_actor(
            A::TYPE_ID.as_u64(),
            label,
            config,
            tag,
        ));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `NodeMessage::CreateActor` to remote node: {}",
                e.report()
            );
            // sends the error back to the original sender who is waiting for a
            // `Result<Result<Address<A>, SessionError>, RecvError>`
            if let Err(e) = tx.send_err(e) {
                // we can not do much if sending the error back to the original sender fails, just
                // log it
                warn!(
                    "Could not report the error in `Handler<RemoteCreateActor<{}>>` to original \
                     sender: {}",
                    ShortName::of::<A>(),
                    e.report()
                );
            }
        } else {
            self.message_res_tx_map.insert(tag, (tx, Instant::now()));
        }

        let decode_context = self.decode_context();

        FutureMessageResult::new(async move {
            let bytes = rx.await?;
            let result = <proto_utils::ResultAddress as prost::Message>::decode(bytes)
                .map_err(DecodeError::ProstDecodeError)?;
            match result.result {
                Some(proto_utils::ResultAddressType::Ok(actor_id)) => {
                    Address::new_with_decode_context(actor_id, decode_context.as_ref())
                        .map_err(Into::into)
                }
                Some(proto_utils::ResultAddressType::Err(e)) => {
                    Err(SessionError::RemoteNodeError(e))
                }
                _ => Err(DecodeError::other("missing field `result` in `ResultAddress`").into()),
            }
        })
    }
}

impl<A> Handler<command::RemoteGetActor<A>> for Session
where
    A: Actor + RemoteAddressable,
{
    type Result = FutureMessageResult<command::RemoteGetActor<A>>;

    async fn handle(
        &mut self,
        msg: command::RemoteGetActor<A>,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command RemoteGetActor<{}>", ShortName::of::<A>());

        let command::RemoteGetActor { actor, .. } = msg;

        let (tx, rx) = oneshot::channel();

        let tag = self.next_tag();
        let ipc_msg = match &actor {
            ActorRef::Index(actor_id) => message::IpcMessage::node_message(
                message::NodeMessage::get_actor_by_index(actor_id.as_local(), tag),
            ),
            ActorRef::Label(label) => message::IpcMessage::node_message(
                message::NodeMessage::get_actor_by_label(label.clone(), tag),
            ),
        };

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `NodeMessage::GetActor` to remote node: {}",
                e.report()
            );
            // sends the error back to the original sender who is waiting for a
            // `Result<Result<Address<A>, SessionError>, RecvError>`
            if let Err(e) = tx.send_err(e) {
                // we can not do much if sending the error back to the original sender fails, just
                // log it
                warn!(
                    "Could not report the error in `Handler<GetRemoteActor>` to original sender: \
                     {}",
                    e.report()
                );
            }
        } else {
            self.message_res_tx_map.insert(tag, (tx, Instant::now()));
        }

        let decode_context = self.decode_context();

        FutureMessageResult::new(async move {
            let bytes = rx.await?;
            let result = <proto_utils::ResultAddress as prost::Message>::decode(bytes)
                .map_err(DecodeError::ProstDecodeError)?;
            match result.result {
                Some(proto_utils::ResultAddressType::Ok(actor_id)) => {
                    Address::new_with_decode_context(actor_id, decode_context.as_ref())
                        .map_err(Into::into)
                }
                Some(proto_utils::ResultAddressType::Err(e)) => {
                    Err(SessionError::RemoteNodeError(e))
                }
                _ => Err(DecodeError::other("missing field `result` in `ResultAddress`").into()),
            }
        })
    }
}

impl Handler<BinaryMessage> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: BinaryMessage,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let BinaryMessage {
            actor_id,
            message_id,
            bytes: message,
            result_tx,
            ..
        } = msg;

        match result_tx {
            Some(tx) => {
                // send

                let tag = self.next_tag();
                let ipc_msg = message::IpcMessage::actor_message(message::ActorMessage::send(
                    actor_id, message_id, message, tag,
                ));

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    warn!(
                        "Could not send `ActorMessage` to remote node: {}",
                        e.report()
                    );
                    // sends the error back to the original sender who is waiting for a
                    // `Result<M::Result, RecvError>`
                    if let Err(e) = tx.send_err(e) {
                        // we can not do much if sending the error back to the original sender
                        // fails, just log it
                        warn!(
                            "Could not report the error in `Handler<BinaryMessage>` to original \
                             sender: {}",
                            e.report()
                        );
                    }

                    return;
                }

                self.message_res_tx_map.insert(tag, (tx, Instant::now()));
            }

            None => {
                // do_send

                let ipc_msg = message::IpcMessage::actor_message(message::ActorMessage::do_send(
                    actor_id, message_id, message,
                ));

                if let Err(e) = self.send_ipc_message(ipc_msg).await {
                    // sender has explicitly indicated that it does not care about the result of
                    // this message, so we just log the error
                    warn!(
                        "Could not do_send `ActorMessage` to remote node: {}",
                        e.report()
                    );
                }
            }
        }
    }
}

impl Handler<MessageResponse> for Session {
    type Result = ();

    async fn handle(
        &mut self,
        msg: MessageResponse,
        _ctx: &mut <Self as Actor>::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let MessageResponse { tag, result } = msg;

        let ipc_msg =
            message::IpcMessage::message_response(message::MessageResponse::new(tag, result));

        if let Err(e) = self.send_ipc_message(ipc_msg).await {
            warn!(
                "Could not send `MessageResponse` to remote node: {}",
                e.report()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_fmt() {
        let ok = MessageResponse {
            tag: 42,
            result: Ok(Bytes::from_static(b"payload")),
        };
        assert_eq!(format!("{ok:?}"), "MessageResponse(42)");

        let err = MessageResponse {
            tag: 7,
            result: Err("boom".to_string()),
        };
        assert_eq!(format!("{err:?}"), "MessageResponse(7)");
    }
}

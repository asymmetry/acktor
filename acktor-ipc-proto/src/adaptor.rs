use std::io;

#[cfg(not(target_arch = "wasm32"))]
use ahash::HashMap;
use bytes::Bytes;
use crossbeam_channel::Sender;
use prost::Message as _;
#[cfg(target_arch = "wasm32")]
use rustc_hash::FxHashMap as HashMap;

use crate::message;

/// A parsed actor message.
#[derive(Debug)]
pub struct ParsedActorMessage {
    pub actor_id: u64,
    pub message_id: u64,
    pub message: Bytes,
}

/// An adaptor for IPC communication with remote actors. This adaptor is only meant to be used
/// in WebAssembly environments where the `Node` actor in the `actor-ipc` crate cannot be used
/// due to the limitation of the async support.
#[derive(Debug)]
pub struct ActorAdaptor {
    tag: u64,
    result_senders: HashMap<u64, (Sender<Bytes>, i64)>,
}

impl Default for ActorAdaptor {
    #[inline]
    fn default() -> Self {
        Self {
            tag: 0,
            result_senders: HashMap::default(),
        }
    }
}

impl ActorAdaptor {
    /// Constructs a new [`ActorAdaptor`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    fn next_tag(&mut self) -> u64 {
        let tag = self.tag;
        self.tag += 1;
        tag
    }

    /// Cleans up the expired result senders that have been waiting for responses for longer than
    /// the specified timeout.
    pub fn cleanup(&mut self, now: i64, timeout: i64) {
        self.result_senders
            .retain(|_, (_, timestamp)| now - *timestamp < timeout);
    }

    /// Sends a message to a remote actor identified by `actor_id` without expecting a response.
    pub fn do_send<F, E>(
        &mut self,
        actor_id: u64,
        message_id: u64,
        message: Bytes,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(Bytes) -> Result<(), E>,
    {
        let ipc_message = message::IpcMessage::actor_message(message::ActorMessage::do_send(
            actor_id, message_id, message,
        ));

        send_func(ipc_message.encode_to_vec().into())
    }

    /// Sends a message to a remote actor identified by `actor_id` and expects a response.
    pub fn send<F, E>(
        &mut self,
        actor_id: u64,
        message_id: u64,
        message: Bytes,
        result_tx: Sender<Bytes>,
        timestamp: i64,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(Bytes) -> Result<(), E>,
    {
        let tag = self.next_tag();
        self.result_senders.insert(tag, (result_tx, timestamp));

        let ipc_message = message::IpcMessage::actor_message(message::ActorMessage::send(
            actor_id, message_id, message, tag,
        ));

        send_func(ipc_message.encode_to_vec().into())
    }

    pub fn parse(&mut self, msg: Bytes) -> Result<Option<ParsedActorMessage>, io::Error> {
        let ipc_msg = message::IpcMessage::decode(msg)?;

        // in WebAssembly environments, we ignore the NodeMessage and the NodeMessageResponse, and
        // we also ignore the ActorMessage that expects a response

        match ipc_msg.message {
            Some(message::IpcMessageType::ActorMessage(message::ActorMessage {
                actor_id,
                message_id,
                message,
                tag: None,
            })) => Ok(Some(ParsedActorMessage {
                actor_id,
                message_id,
                message,
            })),

            Some(message::IpcMessageType::MessageResponse(message::MessageResponse {
                tag,
                response: Some(response),
            })) => match response {
                message::ResponseType::Ok(ok) => {
                    if let Some((rx, _)) = self.result_senders.remove(&tag) {
                        let _ = rx.try_send(ok);
                    }

                    Ok(None)
                }

                message::ResponseType::Err(err) => Err(io::Error::other(err)),
            },

            _ => Err(io::Error::other("unsupported ipc message")),
        }
    }
}

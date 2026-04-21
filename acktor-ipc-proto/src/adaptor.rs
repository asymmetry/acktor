use std::io;

use ahash::HashMap;
use bytes::{Bytes, BytesMut};
use crossbeam_channel::Sender;
use prost::Message as _;

use crate::{actor_message, ipc_message};

/// A parsed actor message.
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
    result_senders: HashMap<u64, Sender<Bytes>>,
    buffer: BytesMut,
}

impl Default for ActorAdaptor {
    #[inline]
    fn default() -> Self {
        Self {
            tag: 0,
            result_senders: HashMap::default(),
            buffer: BytesMut::with_capacity(8192),
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

    /// Sends a message to a remote actor identified by `actor_id` without expecting a response.
    pub fn do_send<'a, F, E>(
        &'a mut self,
        actor_id: usize,
        message_id: u64,
        message: Bytes,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(&'a [u8]) -> Result<(), E>,
    {
        let ipc_message = ipc_message::IpcMessage::actor_message(
            actor_message::ActorMessage::do_send(actor_id as u64, message_id, message),
        );

        let len = ipc_message.encoded_len();
        self.buffer.resize(len, 0);

        // buffer has been resized, this is infallible
        let _ = ipc_message.encode(&mut self.buffer);

        send_func(&self.buffer[..len])
    }

    /// Sends a message to a remote actor identified by `actor_id` and expects a response.
    pub fn send<'a, F, E>(
        &'a mut self,
        actor_id: usize,
        message_id: u64,
        message: Bytes,
        result_tx: Sender<Bytes>,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(&'a [u8]) -> Result<(), E>,
    {
        let tag = self.next_tag();
        self.result_senders.insert(tag, result_tx);

        let ipc_message = ipc_message::IpcMessage::actor_message(
            actor_message::ActorMessage::send(actor_id as u64, message_id, message, tag),
        );

        let len = ipc_message.encoded_len();
        self.buffer.resize(len, 0);

        // buffer has been resized, this is infallible
        let _ = ipc_message.encode(&mut self.buffer);

        send_func(&self.buffer[..len])
    }

    pub fn parse(&mut self, msg: Bytes) -> Result<Option<ParsedActorMessage>, io::Error> {
        let ipc_msg = ipc_message::IpcMessage::decode(msg)?;

        // in WebAssembly environments, we ignore the NodeMessage and the NodeMessageResponse, and
        // we also ignore the ActorMessage that expects a response

        match ipc_msg.message {
            Some(ipc_message::IpcMessageType::ActorMessage(actor_message::ActorMessage {
                actor_id,
                message_id,
                message,
                tag: None,
            })) => Ok(Some(ParsedActorMessage {
                actor_id,
                message_id,
                message,
            })),

            Some(ipc_message::IpcMessageType::ActorMessageResponse(
                actor_message::ActorMessageResponse {
                    tag,
                    response: Some(response),
                },
            )) => match response {
                actor_message::ResponseType::Ok(ok) => {
                    if let Some(rx) = self.result_senders.remove(&tag) {
                        let _ = rx.try_send(ok);
                    }

                    Ok(None)
                }

                actor_message::ResponseType::Err(err) => Err(io::Error::other(err)),
            },

            _ => Err(io::Error::other("unsupported ipc message type")),
        }
    }
}

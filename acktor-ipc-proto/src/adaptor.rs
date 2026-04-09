use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use crossbeam_channel::Sender;
use rustc_hash::FxHashMap as HashMap;

use crate::{actor_message, ipc_message};

/// A parsed message which is the [`DoSend`][actor_message::DoSend] actor message type.
pub struct ParsedActorDoSendMessage {
    pub actor_id: usize,
    pub message: Bytes,
}

/// An adaptor for IPC communication with remote actors. This adaptor is only meant to be used
/// in WebAssembly environments where the `Node` actor in the `actor-ipc` crate cannot be used
/// due to the limitation of the async support.
#[derive(Debug)]
pub struct ActorAdaptor {
    tag: AtomicU64,
    result_senders: HashMap<u64, Sender<Vec<u8>>>,
    buffer: BytesMut,
}

impl Default for ActorAdaptor {
    #[inline]
    fn default() -> Self {
        Self {
            tag: AtomicU64::new(0),
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

    /// Sends a message to a remote actor identified by `actor_id` without expecting a response.
    pub fn do_send<'a, F, E>(
        &'a mut self,
        actor_id: usize,
        msg: Bytes,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(&'a [u8]) -> Result<(), E>,
    {
        let ipc_message = ipc_message::IpcMessage::actor_message(
            actor_message::ActorMessage::do_send(actor_id as u64, msg),
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
        msg: Bytes,
        result_tx: Sender<Vec<u8>>,
        send_func: F,
    ) -> Result<(), E>
    where
        F: FnOnce(&'a [u8]) -> Result<(), E>,
    {
        let tag = self.tag.fetch_add(1, Ordering::Relaxed);
        self.result_senders.insert(tag, result_tx);

        let ipc_message = ipc_message::IpcMessage::actor_message(
            actor_message::ActorMessage::send(actor_id as u64, msg, tag),
        );

        let len = ipc_message.encoded_len();
        self.buffer.resize(len, 0);

        // buffer has been resized, this is infallible
        let _ = ipc_message.encode(&mut self.buffer);

        send_func(&self.buffer[..len])
    }

    pub fn parse(&mut self, msg: Bytes) -> Result<Option<ParsedActorDoSendMessage>, io::Error> {
        let ipc_msg = ipc_message::IpcMessage::decode(msg)?;

        // in WebAssembly environments, we ignore the NodeMessage and the ControlMessage, and we
        // also ignore the ActorMessage::Send variant

        if let Some(ipc_message::IpcMessageType::Actor(actor_msg)) = ipc_msg.message {
            match actor_msg.message {
                Some(actor_message::ActorMessageType::DoSend(actor_message::DoSend {
                    actor_id,
                    message,
                })) => Ok(Some(ParsedActorDoSendMessage {
                    actor_id: actor_id as usize,
                    message,
                })),

                Some(actor_message::ActorMessageType::Reply(actor_message::Reply {
                    tag,
                    reply,
                })) => {
                    if let Some(rx) = self.result_senders.remove(&tag) {
                        let _ = rx.try_send(reply.to_vec());
                    }

                    Ok(None)
                }

                _ => {
                    // NOTE: in WebAssembly environment, we do not support the
                    // ActorMessage::Send variant

                    Err(io::Error::other("unsupported actor message type"))
                }
            }
        } else {
            // NOTE: in WebAssembly environment, we do not support the NodeMessage and
            // the ControlMessage

            Err(io::Error::other("unsupported ipc message type"))
        }
    }
}

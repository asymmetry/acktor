use std::fmt::{self, Debug};
use std::sync::Arc;

use bytes::Bytes;

use super::Message;
use crate::channel::oneshot;
use crate::codec::{DecodeContext, EncodeContext};

/// A binary message which is used to communicate with actors in other processes.
///
/// This type has two use cases:
///
/// - The message sent via an [`Address`][crate::address::Address], if the address is a remote
///   address, the message will be encoded to a `BinaryMessage` and the runtime will route the
///   `bytes` part to the proper remote process.
/// - The binary message received by the runtime will be parsed as a `BinaryMessage` and be routed
///   to the proper local actor to handle it.
///
/// It carries optional encode and decode contexts which can be used to decode the message and
/// encode the message response. They are only required by the second use case and runtime should
/// take care of this automatically.
pub struct BinaryMessage {
    /// The local index part of an [`ActorId`].
    pub actor_id: u64,
    pub message_id: u64,
    pub bytes: Bytes,
    pub result_tx: Option<oneshot::Sender<Bytes>>,
    pub decode_msg_ctx: Option<Arc<dyn DecodeContext + Send + Sync>>,
    pub encode_res_ctx: Option<Arc<dyn EncodeContext + Send + Sync>>,
}

impl Debug for BinaryMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(match &self.result_tx {
            Some(_) => "BinaryMessage<Send>",
            None => "BinaryMessage<DoSend>",
        })
        .field("actor_id", &self.actor_id)
        .field("message_id", &self.message_id)
        .field("bytes", &format_args!("Bytes({})", self.bytes.len()))
        .finish()
    }
}

impl Message for BinaryMessage {
    type Result = ();
}

impl BinaryMessage {
    /// Constructs a new [`BinaryMessage`] which does not expect a response.
    pub fn do_send(actor_id: u64, message_id: u64, bytes: Bytes) -> Self {
        Self {
            actor_id,
            message_id,
            bytes,
            result_tx: None,
            decode_msg_ctx: None,
            encode_res_ctx: None,
        }
    }

    /// Constructs a new [`BinaryMessage`] which expects a response.
    pub fn send(actor_id: u64, message_id: u64, bytes: Bytes, tx: oneshot::Sender<Bytes>) -> Self {
        Self {
            actor_id,
            message_id,
            bytes,
            result_tx: Some(tx),
            decode_msg_ctx: None,
            encode_res_ctx: None,
        }
    }

    /// Sets the decode context on this message.
    pub fn with_decode_context(mut self, context: Arc<dyn DecodeContext + Send + Sync>) -> Self {
        self.decode_msg_ctx = Some(context);
        self
    }

    /// Sets the encode context on this message.
    pub fn with_encode_context(mut self, context: Arc<dyn EncodeContext + Send + Sync>) -> Self {
        self.encode_res_ctx = Some(context);
        self
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_debug_fmt() {
        // do_send variant: result_tx is None
        let msg = BinaryMessage::do_send(7, 99, Bytes::from_static(b"hello"));
        assert_eq!(
            format!("{:?}", msg),
            "BinaryMessage<DoSend> { actor_id: 7, message_id: 99, bytes: Bytes(5) }"
        );

        // send variant: result_tx is Some
        let (tx, _rx) = oneshot::channel::<Bytes>();
        let msg = BinaryMessage::send(7, 99, Bytes::from_static(b"hi"), tx);
        assert_eq!(
            format!("{:?}", msg),
            "BinaryMessage<Send> { actor_id: 7, message_id: 99, bytes: Bytes(2) }"
        );
    }
}

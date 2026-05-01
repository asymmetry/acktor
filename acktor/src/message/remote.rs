use std::fmt::{self, Debug};
use std::sync::Arc;

use bytes::Bytes;

use super::Message;
use crate::actor::ActorId;
use crate::channel::oneshot;
use crate::codec::{DecodeContext, EncodeContext};

/// An encoded message which is used to communicate with actors in other processes.
///
/// It also carries optional encode and decode contexts which can be used to decode the message
/// and encode the message response.
pub struct EncodedMessage {
    /// The index part of an [`ActorId`].
    ///
    /// Usually refers to an actor in another process
    pub actor_id: u64,
    pub message_id: u64,
    pub bytes: Bytes,
    pub result_tx: Option<oneshot::Sender<Bytes>>,
    pub decode_msg_ctx: Option<Arc<dyn DecodeContext + Send + Sync>>,
    pub encode_res_ctx: Option<Arc<dyn EncodeContext + Send + Sync>>,
}

impl Debug for EncodedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(match &self.result_tx {
            Some(_) => "EncodedMessage<Send>",
            None => "EncodedMessage<DoSend>",
        })
        .field("actor_id", &self.actor_id)
        .field("message_id", &self.message_id)
        .field("bytes", &format_args!("Bytes({})", self.bytes.len()))
        .finish()
    }
}

impl Message for EncodedMessage {
    type Result = ();
}

impl EncodedMessage {
    /// Constructs a new [`RemoteMessage`] which does not expect a response.
    pub fn do_send(actor_id: ActorId, message_id: u64, bytes: Bytes) -> Self {
        Self {
            actor_id: actor_id.as_local(),
            message_id,
            bytes,
            result_tx: None,
            decode_msg_ctx: None,
            encode_res_ctx: None,
        }
    }

    /// Constructs a new [`RemoteMessage`] which expects a response.
    pub fn send(
        actor_id: ActorId,
        message_id: u64,
        bytes: Bytes,
        tx: oneshot::Sender<Bytes>,
    ) -> Self {
        Self {
            actor_id: actor_id.as_local(),
            message_id,
            bytes,
            result_tx: Some(tx),
            decode_msg_ctx: None,
            encode_res_ctx: None,
        }
    }

    /// Sets the decode context on this message.
    pub fn with_decode_context<C>(mut self, context: C) -> Self
    where
        C: DecodeContext + Send + Sync + 'static,
    {
        self.decode_msg_ctx = Some(Arc::new(context));
        self
    }

    /// Sets the encode context on this message.
    pub fn with_encode_context<C>(mut self, context: C) -> Self
    where
        C: EncodeContext + Send + Sync + 'static,
    {
        self.encode_res_ctx = Some(Arc::new(context));
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
        let msg = EncodedMessage::do_send(ActorId::new(7), 99, Bytes::from_static(b"hello"));
        assert_eq!(
            format!("{:?}", msg),
            "EncodedMessage<DoSend> { actor_id: 7, message_id: 99, bytes: Bytes(5) }"
        );

        // send variant: result_tx is Some
        let (tx, _rx) = oneshot::channel::<Bytes>();
        let msg = EncodedMessage::send(ActorId::new(7), 99, Bytes::from_static(b"hi"), tx);
        assert_eq!(
            format!("{:?}", msg),
            "EncodedMessage<Send> { actor_id: 7, message_id: 99, bytes: Bytes(2) }"
        );
    }
}

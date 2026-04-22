use bytes::Bytes;

pub use crate::proto::actor_message::actor_message_response::Response as ResponseType;
pub use crate::proto::actor_message::{ActorMessage, ActorMessageResponse};

impl ActorMessage {
    #[inline]
    pub fn send(actor_id: u64, message_id: u64, message: Bytes, tag: u64) -> Self {
        Self {
            actor_id,
            message_id,
            message,
            tag: Some(tag),
        }
    }

    #[inline]
    pub fn do_send(actor_id: u64, message_id: u64, message: Bytes) -> Self {
        Self {
            actor_id,
            message_id,
            message,
            tag: None,
        }
    }
}

impl ActorMessageResponse {
    #[inline]
    pub fn new(tag: u64, response: Result<Bytes, String>) -> Self {
        Self {
            tag,
            response: Some(match response {
                Ok(ok) => ResponseType::Ok(ok),
                Err(err) => ResponseType::Err(err),
            }),
        }
    }
}

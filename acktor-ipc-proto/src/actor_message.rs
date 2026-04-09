use bytes::Bytes;

pub use crate::proto::actor_message::actor_message::Message as ActorMessageType;
pub use crate::proto::actor_message::{ActorMessage, DoSend, Reply, Send};

impl ActorMessage {
    #[inline]
    pub fn send(actor_id: u64, message: Bytes, tag: u64) -> Self {
        Self {
            message: Some(ActorMessageType::Send(Send {
                actor_id,
                message,
                tag,
            })),
        }
    }

    #[inline]
    pub fn do_send(actor_id: u64, message: Bytes) -> Self {
        Self {
            message: Some(ActorMessageType::DoSend(DoSend { actor_id, message })),
        }
    }

    #[inline]
    pub fn reply(tag: u64, message: Bytes) -> Self {
        Self {
            message: Some(ActorMessageType::Reply(Reply { tag, message })),
        }
    }
}

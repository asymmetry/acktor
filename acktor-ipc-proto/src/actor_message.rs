use std::fmt::Display;

use bytes::Bytes;

pub use crate::proto::actor_message::actor_message::Message as ActorMessageType;
pub use crate::proto::actor_message::reply::Result as ReplyResultType;
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
    pub fn reply<E>(tag: u64, result: Result<Bytes, E>) -> Self
    where
        E: Display,
    {
        Self {
            message: Some(ActorMessageType::Reply(Reply {
                tag,
                result: Some(match result {
                    Ok(message) => ReplyResultType::Ok(message),
                    Err(e) => ReplyResultType::Err(e.to_string()),
                }),
            })),
        }
    }
}

use bytes::Bytes;

// use crate::actor_message::{ActorMessage, ActorMessageResponse};
// use crate::node_message::{NodeMessage, NodeMessageResponse};
use crate::utils::ActorRef;

pub use crate::proto::message::ipc_message::Message as IpcMessageType;
pub use crate::proto::message::message_response::Response as ResponseType;
pub use crate::proto::message::{ActorMessage, IpcMessage, MessageResponse};
pub use crate::proto::node_message::node_message::Message as NodeMessageType;
pub use crate::proto::node_message::{CreateActor, GetActor, NodeMessage};

impl IpcMessage {
    #[inline]
    pub fn actor_message(message: ActorMessage) -> Self {
        Self {
            message: Some(IpcMessageType::ActorMessage(message)),
        }
    }

    #[inline]
    pub fn node_message(message: NodeMessage) -> Self {
        Self {
            message: Some(IpcMessageType::NodeMessage(message)),
        }
    }

    #[inline]
    pub fn message_response(response: MessageResponse) -> Self {
        Self {
            message: Some(IpcMessageType::MessageResponse(response)),
        }
    }
}

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

impl NodeMessage {
    #[inline]
    pub fn create_actor(type_id: u64, label: String, config: String, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::CreateActor(CreateActor {
                type_id,
                label,
                config,
                tag,
            })),
        }
    }

    #[inline]
    pub fn get_actor_by_index(actor_id: u64, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::GetActor(GetActor {
                actor: Some(ActorRef::index(actor_id)),
                tag,
            })),
        }
    }

    #[inline]
    pub fn get_actor_by_label(label: String, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::GetActor(GetActor {
                actor: Some(ActorRef::label(label)),
                tag,
            })),
        }
    }
}

impl MessageResponse {
    #[inline]
    pub fn new(tag: u64, result: Result<Bytes, String>) -> Self {
        Self {
            tag,
            response: Some(match result {
                Ok(ok) => ResponseType::Ok(ok),
                Err(err) => ResponseType::Err(err),
            }),
        }
    }
}

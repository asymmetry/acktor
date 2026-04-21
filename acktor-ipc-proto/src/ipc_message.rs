use crate::actor_message::{ActorMessage, ActorMessageResponse};
use crate::node_message::{NodeMessage, NodeMessageResponse};

pub use crate::proto::ipc_message::IpcMessage;
pub use crate::proto::ipc_message::ipc_message::Message as IpcMessageType;

impl IpcMessage {
    #[inline]
    pub fn actor_message(actor_message: ActorMessage) -> Self {
        Self {
            message: Some(IpcMessageType::ActorMessage(actor_message)),
        }
    }

    #[inline]
    pub fn actor_message_response(actor_message_response: ActorMessageResponse) -> Self {
        Self {
            message: Some(IpcMessageType::ActorMessageResponse(actor_message_response)),
        }
    }

    #[inline]
    pub fn node_message(node_message: NodeMessage) -> Self {
        Self {
            message: Some(IpcMessageType::NodeMessage(node_message)),
        }
    }

    #[inline]
    pub fn node_message_response(node_message_response: NodeMessageResponse) -> Self {
        Self {
            message: Some(IpcMessageType::NodeMessageResponse(node_message_response)),
        }
    }
}

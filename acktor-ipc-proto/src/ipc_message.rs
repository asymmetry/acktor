use crate::actor_message::ActorMessage;
use crate::node_message::NodeMessage;

pub use crate::proto::ipc_message::IpcMessage;
pub use crate::proto::ipc_message::ipc_message::Message as IpcMessageType;

impl IpcMessage {
    #[inline]
    pub fn node_message(node_message: NodeMessage) -> Self {
        Self {
            message: Some(IpcMessageType::Node(node_message)),
        }
    }

    #[inline]
    pub fn actor_message(actor_message: ActorMessage) -> Self {
        Self {
            message: Some(IpcMessageType::Actor(actor_message)),
        }
    }
}

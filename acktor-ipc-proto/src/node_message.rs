pub use super::proto::node_message::node_message::Message as MessageType;
pub use super::proto::node_message::node_message_response::Response as ResponseType;
pub use super::proto::node_message::result_remote_address::Result as ResultType;
pub use super::proto::node_message::{
    CreateActor, GetActor, NodeMessage, NodeMessageResponse, ResultRemoteAddress,
};
use crate::utils::ActorHandle;

impl NodeMessage {
    #[inline]
    pub fn create_actor(label: String, r#type: String, config: String, tag: u64) -> Self {
        Self {
            message: Some(MessageType::CreateActor(CreateActor {
                label,
                r#type,
                config,
                tag,
            })),
        }
    }

    #[inline]
    pub fn get_actor_with_index(actor_id: u64, tag: u64) -> Self {
        Self {
            message: Some(MessageType::GetActor(GetActor {
                actor: Some(ActorHandle::index(actor_id)),
                tag,
            })),
        }
    }

    #[inline]
    pub fn get_actor_with_label(label: String, tag: u64) -> Self {
        Self {
            message: Some(MessageType::GetActor(GetActor {
                actor: Some(ActorHandle::label(label)),
                tag,
            })),
        }
    }
}

impl NodeMessageResponse {
    #[inline]
    pub fn create_actor(tag: u64, result: Result<u64, String>) -> Self {
        Self {
            tag,
            response: Some(ResponseType::CreateActor(ResultRemoteAddress {
                result: Some(match result {
                    Ok(ok) => ResultType::ActorId(ok),
                    Err(err) => ResultType::Err(err),
                }),
            })),
        }
    }

    #[inline]
    pub fn get_actor(tag: u64, result: Result<u64, String>) -> Self {
        Self {
            tag,
            response: Some(ResponseType::GetActor(ResultRemoteAddress {
                result: Some(match result {
                    Ok(ok) => ResultType::ActorId(ok),
                    Err(err) => ResultType::Err(err),
                }),
            })),
        }
    }
}

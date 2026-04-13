use std::fmt::Display;

pub use super::proto::node_message::create_actor_result::Result as CreateActorResultType;
pub use super::proto::node_message::get_actor::ActorHandle;
pub use super::proto::node_message::get_actor_result::Result as GetActorResultType;
pub use super::proto::node_message::node_command::Command as NodeCommandType;
pub use super::proto::node_message::node_message::Message as NodeMessageType;
pub use super::proto::node_message::node_reply::Reply as NodeReplyType;
pub use super::proto::node_message::{
    CreateActor, CreateActorResult, GetActor, GetActorResult, NodeCommand, NodeMessage, NodeReply,
};

impl NodeMessage {
    #[inline]
    pub fn create_actor(label: String, r#type: String, config: String, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::Command(NodeCommand {
                command: Some(NodeCommandType::CreateActor(CreateActor {
                    label,
                    r#type,
                    config,
                    tag,
                })),
            })),
        }
    }

    #[inline]
    pub fn get_actor_with_index(actor_id: u64, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::Command(NodeCommand {
                command: Some(NodeCommandType::GetActor(GetActor {
                    actor_handle: Some(ActorHandle::ActorId(actor_id)),
                    tag,
                })),
            })),
        }
    }

    #[inline]
    pub fn get_actor_with_label(label: String, tag: u64) -> Self {
        Self {
            message: Some(NodeMessageType::Command(NodeCommand {
                command: Some(NodeCommandType::GetActor(GetActor {
                    actor_handle: Some(ActorHandle::Label(label)),
                    tag,
                })),
            })),
        }
    }

    pub fn create_actor_result<E>(tag: u64, result: Result<u64, E>) -> Self
    where
        E: Display,
    {
        Self {
            message: Some(NodeMessageType::Reply(NodeReply {
                reply: Some(match result {
                    Ok(actor_id) => NodeReplyType::CreateActor(CreateActorResult {
                        tag,
                        result: Some(CreateActorResultType::Ok(actor_id)),
                    }),
                    Err(e) => NodeReplyType::CreateActor(CreateActorResult {
                        tag,
                        result: Some(CreateActorResultType::Err(e.to_string())),
                    }),
                }),
            })),
        }
    }

    pub fn get_actor_result<E>(tag: u64, result: Result<u64, E>) -> Self
    where
        E: Display,
    {
        Self {
            message: Some(NodeMessageType::Reply(NodeReply {
                reply: Some(match result {
                    Ok(actor_id) => NodeReplyType::GetActor(GetActorResult {
                        tag,
                        result: Some(GetActorResultType::Ok(actor_id)),
                    }),
                    Err(e) => NodeReplyType::GetActor(GetActorResult {
                        tag,
                        result: Some(GetActorResultType::Err(e.to_string())),
                    }),
                }),
            })),
        }
    }
}

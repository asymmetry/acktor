use acktor::Message;

use crate::actor_handle::ActorHandle;
use crate::errors::SessionError;
use crate::remote_address::RemoteAddress;

type Result<T> = std::result::Result<T, SessionError>;

/// A command which is used to create an actor in a remote node.
///
/// The remote node needs to know how to create the actor with the given type and config. If
/// the operation is successful, the provided `label` will be used as the actor label of the
/// new actor created in the remote node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct CreateRemoteActor {
    pub label: String,
    pub r#type: String,
    pub config: String,
}

/// A command which is used to get the address of an actor in a remote node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct GetRemoteActor {
    pub actor: ActorHandle,
}

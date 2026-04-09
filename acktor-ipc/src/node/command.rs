//! Commands which can be used to interact with a node.

use acktor::{Message, Recipient};

use crate::errors::NodeError;
use crate::ipc_method::IpcListener;
use crate::remote_address::RemoteAddress;
use crate::remote_message::RemoteMessage;

type Result<T> = std::result::Result<T, NodeError>;

/// A command which is used to set the IPC listener of a node.
#[derive(Debug, Message)]
#[result_type(())]
pub struct SetListener<L>(pub L)
where
    L: IpcListener;

/// A command which is used to add a local actor to a node.
#[derive(Debug, Message)]
#[result_type(())]
pub struct AddActor(pub Recipient<RemoteMessage>);

/// A command which is used to remove a local actor from a node.
#[derive(Debug, Message)]
#[result_type(())]
pub struct RemoveActor(pub u64);

/// A command which is used by a node to actively connect to another node like a client.
///
/// A new session will be created if successful. The endpoint of the connection will be used
/// as the actor label of the new session actor. The user can provide a `session_label` as
/// an alias to the endpoint, both labels can be used to refer to the session actor in the other
/// commands.
#[derive(Debug, Message)]
#[result_type(Result<()>)]
pub struct Connect {
    pub endpoint: String,
    pub session_label: Option<String>,
}

/// A command which is used by a local actor to create an actor in a remote node.
///
/// The remote node needs to know how to create the actor with the given type and config. If
/// the operation is successful, the provided `label` will be used as the actor label of the
/// new actor created in the remote node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct CreateRemoteActor {
    pub session_label: String,
    pub label: String,
    pub r#type: String,
    pub config: String,
}

/// A command which is used by a local actor to get the address of an actor in a remote node.
///
/// This command assumes that the actor index of the remote actor is not known, and it will
/// query the remote node to get the address of the actor with the given label.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct GetRemoteActor {
    pub session_label: String,
    pub label: String,
}

/// A command which is used by a local actor to create a remote address with the given remote
/// actor index.
///
/// This command assumes that the remote actor index is known, and it will not query the remote
/// node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct CreateRemoteAddress {
    pub session_label: String,
    pub actor_index: u64,
}

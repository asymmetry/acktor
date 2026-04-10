//! Commands which can be used to interact with a node.

use std::fmt::{self, Debug};
use std::marker::PhantomData;

use acktor::{Message, Recipient};

use crate::errors::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote_address::RemoteAddress;
use crate::remote_message::RemoteMessage;

type Result<T> = std::result::Result<T, NodeError>;

/// A command which is used to add an IPC listener to a node.
///
/// A node can hold multiple listeners at once so that it can accept inbound connections on
/// several endpoints in parallel.
#[derive(Message)]
#[result_type(())]
pub struct AddListener<L>(pub L)
where
    L: IpcListener;

impl<L> Debug for AddListener<L>
where
    L: IpcListener,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("AddListener<{}>", acktor::utils::type_name::<L>()))
            .field(&self.0.local_endpoint())
            .finish()
    }
}

/// A command which is used to remove an IPC listener from a node.
///
/// The listener is identified by its local endpoint.
#[derive(Debug, Message)]
#[result_type(())]
pub struct RemoveListener(pub String);

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
#[derive(Message)]
#[result_type(Result<()>)]
pub struct Connect<C>
where
    C: IpcConnection,
{
    pub endpoint: String,
    pub session_label: Option<String>,
    pub _phantom: PhantomData<fn(C) -> C>,
}

impl<C> Debug for Connect<C>
where
    C: IpcConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&format!("Connect<{}>", acktor::utils::type_name::<C>()))
            .field("endpoint", &self.endpoint)
            .field("session_label", &self.session_label)
            .finish()
    }
}

impl<C> Connect<C>
where
    C: IpcConnection,
{
    /// Constructs a new [`Connect`] command for the connection type `C`.
    pub fn new(endpoint: String, session_label: Option<String>) -> Self {
        Self {
            endpoint,
            session_label,
            _phantom: PhantomData,
        }
    }
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

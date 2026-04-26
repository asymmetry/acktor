//! Commands which can be used to interact with a node.
//!

use std::fmt::{self, Debug};
use std::marker::PhantomData;

use acktor::{ActorId, Address, Message};

use crate::actor_handle::ActorHandle;
use crate::errors::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote_actor::RemoteActor;
use crate::remote_address::RemoteAddress;
use crate::session::{Session, SessionHandle};

type Result<T> = std::result::Result<T, NodeError>;

/// A command which is used to add an IPC listener to a node.
///
/// A node can hold multiple listeners at once so that it can accept inbound connections on
/// several endpoints in parallel.
#[derive(Message)]
#[result_type(bool)]
pub struct AddListener<L>(pub L)
where
    L: IpcListener;

impl<L> Debug for AddListener<L>
where
    L: IpcListener,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("{}", acktor::utils::ShortName::of::<Self>()))
            .field(&format_args!("{}", self.0.local_endpoint()))
            .finish()
    }
}

/// A command which is used to remove an IPC listener from a node.
///
/// The listener is identified by its local endpoint.
#[derive(Debug, Message)]
#[result_type(bool)]
pub struct RemoveListener(pub String);

/// A command which is used to add an actor to a node.
///
/// The `label` is registered alongside the address so the actor can later be looked up by label
/// from a remote peer. Returns `false` if either the label or the actor is already registered.
#[derive(Message)]
#[result_type(bool)]
pub struct AddActor<A>
where
    A: RemoteActor,
{
    pub label: String,
    pub address: Address<A>,
}

impl<A> Debug for AddActor<A>
where
    A: RemoteActor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("{}", acktor::utils::ShortName::of::<Self>()))
            .field(&self.label)
            .field(&format_args!("{}", self.address.index()))
            .finish()
    }
}

/// A command which is used to remove an actor from a node.
#[derive(Debug, Message)]
#[result_type(bool)]
pub struct RemoveActor(pub ActorId);

/// A command which is used by a node to actively connect to another node like a client.
///
/// A new session will be created if the operation is successful. The endpoint of the connection
/// will be used as the actor label of the new session actor. The user can provide a
/// `session_label` as an alias to the endpoint, both labels can be used to refer to the session
/// actor in the other commands.
///
/// The command will return the address if it succeeds, however users are not recommended to await
/// the result. The recommended way is to listen to the
/// [`NodeEvent::SessionCreated`][crate::node::NodeEvent::SessionCreated] event which is emitted
/// when a session is created, and get the session address from the event.
#[derive(Message)]
#[result_type(Result<Address<Session>>)]
pub struct Connect<C>
where
    C: IpcConnection,
{
    pub endpoint: String,
    pub session_label: Option<String>,
    pub _phantom: PhantomData<fn() -> C>,
}

impl<C> Debug for Connect<C>
where
    C: IpcConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("{}", acktor::utils::ShortName::of::<Self>()))
            .field(&self.endpoint)
            .field(&self.session_label)
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

/// A command which is used to create an actor in a remote node.
///
/// The remote node needs to know how to create the actor with the given type and config. If
/// the operation is successful, the provided `label` will be used as the actor label of the
/// new actor created in the remote node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct CreateRemoteActor {
    pub session: SessionHandle,
    pub label: String,
    pub r#type: String,
    pub config: String,
}

/// A command which is used to get the address of an actor in a remote node.
#[derive(Debug, Message)]
#[result_type(Result<RemoteAddress>)]
pub struct GetRemoteActor {
    pub session: SessionHandle,
    pub actor: ActorHandle,
}

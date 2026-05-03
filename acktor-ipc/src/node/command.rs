//! Commands which can be used to interact with a node.
//!

use std::fmt::{self, Debug};
use std::marker::PhantomData;

use acktor::{Actor, ActorId, Address, Message, actor::RemoteAddressable};

use crate::actor_handle::ActorHandle;
use crate::error::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote::RemoteSpawnable;
use crate::session::{Session, SessionHandle};

type Result<T> = std::result::Result<T, NodeError>;

/// A command which is used to add an IPC listener to a node.
///
/// A node can hold multiple listeners at once so that it can accept inbound connections on
/// several endpoints in parallel.
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

impl<L> Message for AddListener<L>
where
    L: IpcListener,
{
    type Result = bool;
}

/// A command which is used to remove an IPC listener from a node.
///
/// The listener is identified by its local endpoint.
#[derive(Debug)]
pub struct RemoveListener(pub String);

impl Message for RemoveListener {
    type Result = bool;
}

/// A command which is used to add an actor to a node.
///
/// The `label` is registered alongside the address so the actor can later be looked up by label
/// from a remote peer. Returns `false` if either the label or the actor is already registered.
pub struct AddActor<A>
where
    A: Actor + RemoteAddressable,
{
    pub label: String,
    pub address: Address<A>,
}

impl<A> Debug for AddActor<A>
where
    A: Actor + RemoteAddressable,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("{}", acktor::utils::ShortName::of::<Self>()))
            .field(&self.label)
            .field(&self.address.index())
            .finish()
    }
}

impl<A> Message for AddActor<A>
where
    A: Actor + RemoteAddressable,
{
    type Result = bool;
}

/// A command which is used to remove an actor from a node.
#[derive(Debug)]
pub struct RemoveActor(pub ActorId);

impl Message for RemoveActor {
    type Result = bool;
}

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

impl<C> Message for Connect<C>
where
    C: IpcConnection,
{
    type Result = Result<Address<Session>>;
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
#[derive(Debug)]
pub struct CreateRemoteActor<A>
where
    A: Actor + RemoteSpawnable,
{
    pub session: SessionHandle,
    pub label: String,
    pub config: String,
}

impl<A> Message for CreateRemoteActor<A>
where
    A: Actor + RemoteSpawnable,
{
    type Result = Result<Address<A>>;
}

/// A command which is used to get the address of an actor in a remote node.
#[derive(Debug)]
pub struct GetRemoteActor<A>
where
    A: Actor + RemoteAddressable,
{
    pub session: SessionHandle,
    pub actor: ActorHandle,
}

impl<A> Message for GetRemoteActor<A>
where
    A: Actor + RemoteAddressable,
{
    type Result = Result<Address<A>>;
}

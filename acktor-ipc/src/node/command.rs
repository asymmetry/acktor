//! Commands which can be used to interact with a node.
//!

use std::fmt::{self, Debug};
use std::marker::PhantomData;

use acktor::{Actor, Address, Message, utils::ShortName};

use crate::actor_ref::ActorRef;
use crate::error::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote::{RemoteAddressable, RemoteSpawnable};
use crate::session::{Session, SessionRef};

type Result<T> = std::result::Result<T, NodeError>;

/// A command which is used to add an IPC listener to a node.
///
/// A node can hold multiple listeners so that it can accept inbound connections on several local
/// endpoints in parallel. Returns `false` if the listener is already registered.
pub struct AddListener<L>(pub L)
where
    L: IpcListener;

impl<L> Debug for AddListener<L>
where
    L: IpcListener,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.0.local_endpoint())
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
/// The listener is identified by its local endpoint. Returns `false` if no listener with the
/// given endpoint is found.
#[derive(Debug)]
pub struct RemoveListener(pub String);

impl Message for RemoveListener {
    type Result = bool;
}

/// A command which is used to actively connect to another node like a client.
///
/// A new session will be created if the operation is successful. The endpoint of the connection
/// will be used as the actor label of the new session actor. The user can provide a
/// `session_label` as an alias to the endpoint, both labels can be used to refer to the session
/// actor in other commands.
///
/// The command will return the address of the session actor if it succeeds.
pub struct Connect<C>
where
    C: IpcConnection,
{
    pub endpoint: String,
    pub session_label: Option<String>,
    _marker: PhantomData<fn(C) -> C>,
}

impl<C> Debug for Connect<C>
where
    C: IpcConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
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
            _marker: PhantomData,
        }
    }
}

/// A command which is used to add a remote addressable actor to a node.
///
/// The `label` is registered alongside the address so the actor can later be looked up by label
/// from a remote node. Returns `false` if either the label or the actor is already registered.
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
        f.debug_tuple(&ShortName::of::<Self>().to_string())
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
///
/// The actor is identified by its index or label. Returns `false` if no actor is found.
pub struct RemoveActor(pub ActorRef);

impl Debug for RemoveActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RemoveActor")
            .field(match &self.0 {
                ActorRef::Index(index) => index,
                ActorRef::Label(label) => label,
            })
            .finish()
    }
}

impl Message for RemoveActor {
    type Result = bool;
}

/// A command which is used to create an actor in a remote node.
///
/// The actor type should have been registered in the remote node's factory registry. If the
/// operation is successful, the provided `label` will be used as the actor label of the new actor
/// created in the remote node.
#[derive(Debug)]
pub struct RemoteCreateActor<A>
where
    A: Actor + RemoteSpawnable,
{
    pub session: SessionRef,
    pub label: String,
    pub config: String,
    _marker: PhantomData<fn(A) -> A>,
}

impl<A> Message for RemoteCreateActor<A>
where
    A: Actor + RemoteSpawnable,
{
    type Result = Result<Address<A>>;
}

impl<A> RemoteCreateActor<A>
where
    A: Actor + RemoteSpawnable,
{
    /// Constructs a new [`RemoteCreateActor`] command for the actor type `A`.
    pub fn new(session: SessionRef, label: String, config: String) -> Self {
        Self {
            session,
            label,
            config,
            _marker: PhantomData,
        }
    }
}

/// A command which is used to get the address of an actor in a remote node.
#[derive(Debug)]
pub struct RemoteGetActor<A>
where
    A: Actor + RemoteAddressable,
{
    pub session: SessionRef,
    pub actor: ActorRef,
    _marker: PhantomData<fn(A) -> A>,
}

impl<A> Message for RemoteGetActor<A>
where
    A: Actor + RemoteAddressable,
{
    type Result = Result<Address<A>>;
}

impl<A> RemoteGetActor<A>
where
    A: Actor + RemoteAddressable,
{
    /// Constructs a new [`RemoteGetActor`] command for the actor type `A`.
    pub fn new(session: SessionRef, actor: ActorRef) -> Self {
        Self {
            session,
            actor,
            _marker: PhantomData,
        }
    }
}

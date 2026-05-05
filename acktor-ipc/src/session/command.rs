//! Commands which can be used to interact with a session.
//!

use std::marker::PhantomData;

use acktor::{Actor, Address, Message, actor::RemoteAddressable};

use crate::actor_ref::ActorRef;
use crate::error::SessionError;
use crate::remote::RemoteSpawnable;

type Result<T> = std::result::Result<T, SessionError>;

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
    pub fn new(label: String, config: String) -> Self {
        Self {
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
    pub fn new(actor: ActorRef) -> Self {
        Self {
            actor,
            _marker: PhantomData,
        }
    }
}

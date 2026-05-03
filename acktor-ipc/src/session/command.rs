//! Commands which can be used to interact with a session.
//!

use std::marker::PhantomData;

use acktor::{Actor, Address, Message, actor::RemoteAddressable};

use crate::actor_handle::ActorHandle;
use crate::error::SessionError;
use crate::remote::RemoteSpawnable;

type Result<T> = std::result::Result<T, SessionError>;

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
    pub label: String,
    pub config: String,
    pub marker: PhantomData<fn() -> A>,
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
    pub actor: ActorHandle,
    pub marker: PhantomData<fn() -> A>,
}

impl<A> Message for GetRemoteActor<A>
where
    A: Actor + RemoteAddressable,
{
    type Result = Result<Address<A>>;
}

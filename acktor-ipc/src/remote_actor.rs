//! Traits for actors which can be reached over IPC and a registry of this kind of actors.

use std::error::Error;
use std::marker::PhantomData;

use tokio::task::JoinHandle;

use acktor::{Actor, Address, Handler, Recipient};

use crate::remote_message::RemoteMessage;

mod registry;
pub use registry::RemoteActorRegistry;

/// A marker trait for actors which can be reached over IPC.
///
/// Users should implement the [`Handler<RemoteMessage>`] trait for a remote actor to define
/// how it processes incoming remote messages, then apply the
/// [`#[acktor_ipc::remote]`][crate::remote] attribute to the `impl Actor for MyActor { ... }`
/// block so the actor's address will be automatically registered in the [`Node`][crate::Node]'s
/// [`RemoteActorRegistry`].
pub trait RemoteActor: Actor + Handler<RemoteMessage> {}

/// Extends [`RemoteActor`] with the ability to be spawned on demand by peers.
///
/// Implementing this trait opts an actor into peer-initiated creation. [`TYPE_NAME`]
/// is the type identifier the peer sends in the `CreateActor` messsage.
///
/// [`TYPE_NAME`]: RemoteActorFactory::TYPE_NAME
pub trait RemoteActorFactory: RemoteActor {
    /// The type name of this actor.
    ///
    /// [`std::any::type_name`] is not guaranteed to be stable across Rust versions so we require users to provide their own stable type name.
    const TYPE_NAME: &'static str;

    /// Creates an instance of this actor with the given label and configuration string.
    ///
    /// Implementations typically call [`Actor::create`] or [`Actor::run`] internally and return
    /// the actor address and the join handle.
    fn create_remote(
        label: String,
        config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), <Self as Actor>::Error>;
}

/// Dyn-compatible shim stored inside a [`Node`][crate::Node]'s factory map.
///
/// [`RemoteActorFactory`] itself is not dyn-compatible because of the `Actor` supertrait.
/// This trait returns a recipient so the node can store factories as trait objects.
pub(crate) trait DynRemoteActorFactory: Send + Sync + 'static {
    fn create_remote(
        &self,
        label: String,
        config: String,
    ) -> Result<(Recipient<RemoteMessage>, JoinHandle<()>), Box<dyn Error + Send + Sync>>;
}

/// Zero-sized generic adapter that bridges a [`RemoteActorFactory`] impl into the
/// dyn-compatible [`DynRemoteActorFactory`] trait object.
pub(crate) struct RemoteActorShim<A>(pub(crate) PhantomData<fn() -> A>);

impl<A> DynRemoteActorFactory for RemoteActorShim<A>
where
    A: RemoteActorFactory,
{
    fn create_remote(
        &self,
        label: String,
        config: String,
    ) -> Result<(Recipient<RemoteMessage>, JoinHandle<()>), Box<dyn Error + Send + Sync>> {
        let (address, join_handle) = A::create_remote(label, config).map_err(Into::into)?;
        Ok((address.into(), join_handle))
    }
}

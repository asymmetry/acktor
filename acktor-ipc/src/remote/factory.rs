use std::marker::PhantomData;

use acktor::{
    Actor, Address, BoxError, JoinHandle,
    actor::{RemoteAddressable, RemoteMailbox},
};

/// Extends [`RemoteAddressable`] with the ability to be spawned from another process.
pub trait RemoteSpawnable: RemoteAddressable {
    /// Creates an instance of this actor with the given label and configuration string.
    ///
    /// Implementations typically call [`Actor::start`] or [`Actor::create`] internally.
    fn create_remote(
        label: String,
        config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), <Self as Actor>::Error>;
}

/// Dyn-compatible shim stored inside a [`Node`][crate::Node]'s actor factory map.
///
/// [`RemoteSpawnable`] itself is not dyn-compatible because of the `Actor` supertrait. This trait
/// returns a [`RemoteMailbox`] so the node can store factories as `Arc<dyn DynRemoteSpawnable>`.
pub(crate) trait DynRemoteSpawnable: Send + Sync + 'static {
    fn create_remote(
        &self,
        label: String,
        config: String,
    ) -> Result<(RemoteMailbox, JoinHandle<()>), BoxError>;
}

/// Zero-sized generic adapter that bridges a [`RemoteSpawnable`] impl into a dyn-compatible
/// [`DynRemoteSpawnable`] trait object.
pub(crate) struct RemoteSpawnableShim<A>(pub(crate) PhantomData<fn() -> A>);

impl<A> DynRemoteSpawnable for RemoteSpawnableShim<A>
where
    A: RemoteSpawnable,
{
    fn create_remote(
        &self,
        label: String,
        config: String,
    ) -> Result<(RemoteMailbox, JoinHandle<()>), BoxError> {
        let (address, join_handle) = A::create_remote(label, config).map_err(Into::into)?;
        Ok((address.into(), join_handle))
    }
}

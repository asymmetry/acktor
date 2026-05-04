use std::marker::PhantomData;

use acktor::{JoinHandle, error::BoxError};

use super::{RemoteMailbox, RemoteSpawnable};

/// Remote actor factory stored inside a [`Node`][crate::Node]'s actor factory map.
pub(crate) trait RemoteFactory: Send + Sync + 'static {
    fn create_remote(
        &self,
        label: String,
        config: String,
    ) -> Result<(RemoteMailbox, JoinHandle<()>), BoxError>;
}

/// Zero-sized generic adapter that bridges a [`RemoteSpawnable`] impl into a dyn-compatible
/// [`RemoteFactory`] trait object.
pub(crate) struct RemoteFactoryShim<A>(pub(crate) PhantomData<fn(A) -> A>);

impl<A> RemoteFactory for RemoteFactoryShim<A>
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

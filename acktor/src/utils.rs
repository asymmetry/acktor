use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::address::{Recipient, Sender, SenderIndex};
use crate::signal::Signal;

static ADDRESS_INDEX_ALLOCATOR: AtomicUsize = AtomicUsize::new(0);

#[inline]
pub(crate) fn new_address_id() -> usize {
    ADDRESS_INDEX_ALLOCATOR.fetch_add(1, Ordering::AcqRel)
}

#[doc(hidden)]
#[inline]
pub fn type_name<T>() -> Result<&'static str, fmt::Error> {
    let type_name = std::any::type_name::<T>()
        .split("<")
        .next()
        .ok_or(fmt::Error)?;
    type_name.rsplit("::").next().ok_or(fmt::Error)
}

/// Terminates an actor by sending it a [`Signal::Terminate`] message and awaiting its [`JoinHandle`].
pub async fn terminate_actor(address: Recipient<Signal>, join_handle: JoinHandle<()>) {
    if let Err(e) = address.do_send(Signal::Terminate).await {
        warn!("Could not stop actor {}: {}", address.index(), e);
        join_handle.abort();
    }
    debug!("Waiting for actor {} to stop", address.index());
    let _ = join_handle.await;
}

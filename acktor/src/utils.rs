use std::sync::atomic::{AtomicU64, Ordering};

use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::actor::ActorId;
use crate::address::{Recipient, Sender, SenderId};
use crate::signal::Signal;

static ACTOR_ID_ALLOCATOR: AtomicU64 = AtomicU64::new(0);

/// Maximum value `create_actor_id` may return. The MSB of the u64 actor id is reserved.
#[doc(hidden)]
pub const MAX_ACTOR_ID: u64 = (1 << 63) - 1;

#[inline]
pub(crate) fn create_actor_id() -> ActorId {
    let id = ACTOR_ID_ALLOCATOR.fetch_add(1, Ordering::Relaxed);
    debug_assert!(
        id <= MAX_ACTOR_ID,
        "actor id space exhausted (more than {} actors allocated in the current process)",
        MAX_ACTOR_ID
    );
    id
}

#[doc(hidden)]
#[inline]
pub fn type_name<T>() -> &'static str {
    let type_name = std::any::type_name::<T>().split("<").next().unwrap();
    type_name.rsplit("::").next().unwrap()
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

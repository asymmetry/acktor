use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{debug, warn};

/// Logs at info level only in debug mode.
#[doc(hidden)]
#[macro_export]
macro_rules! __debug_info {
    ($($arg:tt)+) => {
        #[cfg(debug_assertions)]
        tracing::info!($($arg)+);
    };
}

/// Logs at trace level only in debug mode.
#[doc(hidden)]
#[macro_export]
macro_rules! __debug_trace {
    ($($arg:tt)+) => {
        #[cfg(debug_assertions)]
        tracing::trace!($($arg)+);
    };
}

#[doc(inline)]
pub use crate::__debug_info as debug_info;
#[doc(inline)]
pub use crate::__debug_trace as debug_trace;

use crate::actor::{ActorId, JoinHandle};
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

/// Terminates an actor by sending it a [`Signal::Terminate`] message and awaiting its
/// [`JoinHandle`].
pub async fn terminate_actor<A>(address: A, join_handle: JoinHandle<()>)
where
    A: Into<Recipient<Signal>>,
{
    let address = address.into();
    if let Err(e) = address.do_send(Signal::Terminate).await {
        warn!("Could not stop actor {}: {}", address.index(), e);
        join_handle.abort();
    }
    debug!("Waiting for actor {} to stop", address.index());
    let _ = join_handle.await;
}

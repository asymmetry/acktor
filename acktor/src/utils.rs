//! Utility functions and types used by this crate.
//!

use std::any::Any;

use tracing::{debug, warn};

use crate::actor::JoinHandle;
use crate::address::{Recipient, Sender, SenderInfo};
use crate::signal::Signal;

#[cfg(test)]
pub(crate) mod test_utils;

mod type_map;
pub use type_map::{NopHasher, TypeMap};

pub use disqualified::ShortName;

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

#[inline]
pub(crate) fn panic_info_to_string(info: &(dyn Any + Send)) -> String {
    if let Some(s) = info.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = info.downcast_ref::<String>() {
        s.clone()
    } else {
        "could not capture the panic info".to_string()
    }
}

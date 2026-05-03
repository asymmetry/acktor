use std::fmt::{self, Debug};
use std::future;
use std::pin::Pin;

use tracing::Instrument;

use super::{Message, MessageResponse};
use crate::actor::Actor;
use crate::channel::oneshot;

/// A helper type which wraps the result of a message handler as a future which runs off the
/// mailbox.
///
/// Return [`FutureMessageResult`] from a handler when the work must be awaited but should not
/// stall the actor. The inner future resolves to `M::Result`, which is what the caller ultimately
/// receives.
///
/// The inner future is spawned into the Tokio runtime and is detached from the actor's
/// lifecycle. It continues running even after the actor is stopped or terminated.
pub struct FutureMessageResult<M>
where
    M: Message,
{
    future: Pin<Box<dyn Future<Output = M::Result> + Send>>,
}

impl<M> Debug for FutureMessageResult<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", crate::utils::ShortName::of::<Self>()))
    }
}

impl<M> FutureMessageResult<M>
where
    M: Message,
{
    /// Wrap a future that produces the handler's result.
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = M::Result> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<A, M> MessageResponse<A, M> for FutureMessageResult<M>
where
    A: Actor,
    M: Message,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        tokio::spawn(
            async move {
                let result = self.future.await;
                if let Some(tx) = tx {
                    let _ = tx.send(result);
                }
            }
            .in_current_span(),
        );
        future::ready(())
    }
}

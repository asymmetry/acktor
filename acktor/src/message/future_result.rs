use std::fmt::{self, Debug};
use std::future;
use std::pin::Pin;

use tokio::sync::oneshot;

use super::{Message, MessageResponse};
use crate::actor::Actor;
use crate::envelope::DefaultEnvelopeProxy;

/// A helper type which wraps the result of a message handler as a future which runs off the
/// mailbox.
///
/// Return [`FutureMessageResult`] from a handler when the work must be awaited but should not
/// stall the actor. The inner future resolves to `M::Result`, which is what the caller of
/// `Address::send` ultimately receives.
pub struct FutureMessageResult<M, EP = DefaultEnvelopeProxy<M>>
where
    M: Message<EP>,
{
    future: Pin<Box<dyn Future<Output = M::Result> + Send>>,
}

impl<M, EP> Debug for FutureMessageResult<M, EP>
where
    M: Message<EP>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "FutureMessageResult<{}>",
            crate::utils::type_name::<M>()?
        ))
    }
}

impl<M, EP> FutureMessageResult<M, EP>
where
    M: Message<EP>,
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

impl<A, M, EP> MessageResponse<A, M, EP> for FutureMessageResult<M, EP>
where
    A: Actor,
    M: Message<EP>,
    EP: 'static,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        tokio::spawn(async move {
            let result = self.future.await;
            if let Some(tx) = tx {
                let _ = tx.send(result);
            }
        });
        future::ready(())
    }
}

use std::fmt::{self, Debug};
use std::future;

use tokio::sync::oneshot;

use super::{Message, MessageResponse};
use crate::actor::Actor;
use crate::envelope::DefaultEnvelopeProxy;

/// A helper type which wraps the result of a message handler as a message response.
///
/// This is useful when the result type of a message does not implement [`MessageResponse`],
/// and you can not implement [`MessageResponse`] for the type due to the orphan rule. In this
/// case, you can wrap the result type with this type and use it as the
/// [`Result`][super::Handler::Result] associate type in the [`Handler`][super::Handler] trait.
pub struct MessageResult<M, EP = DefaultEnvelopeProxy<M>>(pub M::Result)
where
    M: Message<EP>;

impl<M, EP> Debug for MessageResult<M, EP>
where
    M: Message<EP>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "MessageResult<{}>",
            crate::utils::type_name::<M>()
        ))
    }
}

impl<A, M, EP> MessageResponse<A, M, EP> for MessageResult<M, EP>
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
        if let Some(tx) = tx {
            let _ = tx.send(self.0);
        }
        future::ready(())
    }
}

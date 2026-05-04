use std::fmt::{self, Debug};
use std::future;

use super::{Message, MessageResponse};
use crate::actor::Actor;
use crate::channel::oneshot;
use crate::utils::ShortName;

/// A helper type which wraps the result of a message handler as a message response.
///
/// This is useful when the result type of a message does not implement [`MessageResponse`], and
/// you can not implement [`MessageResponse`] for the type due to the orphan rule. In this case,
/// you can wrap the result type with this type and use it as the
/// [`Handler::Result`][super::Handler::Result] associate type.
pub struct MessageResult<M>(pub M::Result)
where
    M: Message;

impl<M> Debug for MessageResult<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", ShortName::of::<Self>()))
    }
}

impl<A, M> MessageResponse<A, M> for MessageResult<M>
where
    A: Actor,
    M: Message,
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

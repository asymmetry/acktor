use std::any::Any;
use std::fmt::{self, Debug};
use std::pin::Pin;

use futures_util::future::FutureExt;
use tracing::debug;

use super::{Envelope, EnvelopeProxy, FromEnvelope, ToEnvelope};
use crate::actor::Actor;
use crate::channel::oneshot;
use crate::message::{Handler, Message, MessageResponse};

/// The default envelope proxy for [`Message`].
///
/// This proxy will invoke the actor's [`Handler<M>`] trait and return the result through
/// an oneshot channel if provided.
pub struct DefaultEnvelopeProxy<M>
where
    M: Message,
{
    pub(crate) message: Option<M>,
    pub(crate) tx: Option<oneshot::Sender<M::Result>>,
}

impl<M> Debug for DefaultEnvelopeProxy<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", crate::utils::ShortName::of::<Self>()))
    }
}

impl<M> DefaultEnvelopeProxy<M>
where
    M: Message,
{
    /// Takes the message out of the envelope proxy, leaving [`None`] in its place.
    pub fn message(&mut self) -> Option<M> {
        self.message.take()
    }
}

impl<A, M> EnvelopeProxy<A> for DefaultEnvelopeProxy<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn handle<'a, 'b>(
        &'a mut self,
        actor: &'b mut A,
        ctx: &'b mut A::Context,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        'b: 'a,
    {
        async {
            let tx = self.tx.take();

            if let Some(msg) = self.message.take() {
                if tx.as_ref().is_some_and(oneshot::Sender::is_closed) {
                    debug!("Skipping handling of the message since result sender is closed");

                    return;
                }

                let result = actor.handle(msg, ctx).await;
                result.handle(ctx, tx).await;
            }
        }
        .boxed()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<A, M> ToEnvelope<A, M, DefaultEnvelopeProxy<M>> for A
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn pack(msg: M, tx: Option<oneshot::Sender<M::Result>>) -> Envelope<A> {
        Envelope::with_proxy(Box::new(DefaultEnvelopeProxy {
            message: Some(msg),
            tx,
        }))
    }
}

impl<A, M> FromEnvelope<A, M, DefaultEnvelopeProxy<M>> for A
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn unpack(mut envelope: Envelope<A>) -> M {
        envelope
            .as_any_mut()
            .downcast_mut::<DefaultEnvelopeProxy<M>>()
            .unwrap_or_else(|| {
                panic!(
                    "envelope type mismatch during downcast: expected DefaultEnvelopeProxy<{}> \
                     for actor {}",
                    crate::utils::ShortName::of::<M>(),
                    crate::utils::ShortName::of::<A>(),
                )
            })
            .message()
            .unwrap_or_else(|| {
                panic!(
                    "message already taken from DefaultEnvelopeProxy<{}> for actor {}",
                    crate::utils::ShortName::of::<M>(),
                    crate::utils::ShortName::of::<A>(),
                )
            })
    }
}

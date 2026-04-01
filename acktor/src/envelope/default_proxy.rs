use std::any::Any;
use std::pin::Pin;

use futures_util::future::FutureExt;
use tokio::sync::oneshot;

use super::{Envelope, EnvelopeProxy, FromEnvelope, ToEnvelope};
use crate::actor::{Actor, ActorContext};
use crate::message::{Handler, Message, MessageResponse};

/// The default envelope proxy for [`Message`].
///
/// This proxy will invoke the actor's [`Handler<M>`] trait and return the result through
/// an oneshot channel if provided.
#[derive(Debug)]
pub struct DefaultEnvelopeProxy<M>
where
    M: Message<Self>,
{
    pub(crate) message: Option<M>,
    pub(crate) tx: Option<oneshot::Sender<M::Result>>,
}

impl<M> DefaultEnvelopeProxy<M>
where
    M: Message<Self>,
{
    /// Takes the message out of the envelope proxy, leaving [`None`] in its place.
    pub fn message(&mut self) -> Option<M> {
        self.message.take()
    }
}

impl<A, M> EnvelopeProxy<A> for DefaultEnvelopeProxy<M>
where
    A: Actor + Handler<M, Self>,
    M: Message<Self>,
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

            // if the provided result sender is closed, we will skip processing the message
            // to avoid unnecessary resource usage.
            if let Some(t) = &tx {
                if t.is_closed() {
                    return;
                }
            }

            if let Some(msg) = self.message.take() {
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

impl<A, C, M> ToEnvelope<A, M, DefaultEnvelopeProxy<M>> for C
where
    A: Actor<Context = Self> + Handler<M>,
    C: ActorContext<A>,
    M: Message,
{
    fn pack(msg: M, tx: Option<oneshot::Sender<M::Result>>) -> Envelope<A> {
        Envelope::new(msg, tx)
    }
}

impl<A, C, M> FromEnvelope<A, M, DefaultEnvelopeProxy<M>> for C
where
    A: Actor<Context = Self> + Handler<M>,
    C: ActorContext<A>,
    M: Message,
{
    fn unpack(mut envelope: Envelope<A>) -> M {
        envelope
            .as_any_mut()
            .downcast_mut::<DefaultEnvelopeProxy<M>>()
            .expect("envelope type mismatch during downcast")
            .message()
            .expect("message already taken from envelope")
    }
}

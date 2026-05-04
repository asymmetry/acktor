use std::any::Any;
use std::fmt::{self, Debug};
use std::pin::Pin;

use futures_util::FutureExt;
use tokio::time::{self, Duration};
use tracing::debug;

use super::{Envelope, EnvelopeProxy, FromEnvelope, IntoEnvelope};
use crate::actor::Actor;
use crate::channel::oneshot;
use crate::message::{Handler, Message, MessageResponse};
use crate::utils::ShortName;

/// Wraps a message with a time budget for handling.
///
/// Pair with [`TimedEnvelopeProxy`] to send `M` to an actor and abort handling if it does not
/// complete within `budget`. The response type matches `M::Result`; on timeout, the result sender
/// receives an error instead of a value.
pub struct Timed<M>
where
    M: Message,
{
    message: M,
    budget: Duration,
}

impl<M> Debug for Timed<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.budget)
            .finish()
    }
}

impl<M> Timed<M>
where
    M: Message,
{
    /// Constructs a new [`Timed`] with the given message and time budget.
    pub fn new(message: M, budget: Duration) -> Self {
        Self { message, budget }
    }

    /// Consumes the [`Timed`] and returns the wrapped message and time budget.
    pub fn into_parts(self) -> (M, Duration) {
        (self.message, self.budget)
    }
}

impl<M> Message for Timed<M>
where
    M: Message,
{
    type Result = M::Result;
}

/// [`EnvelopeProxy`] for [`Timed`] that bounds the actor's handling of `M` by a time budget.
///
/// If `actor.handle(msg)` does not finish within `budget`, the handler future is dropped and the
/// timeout error is forwarded to the result sender with [`oneshot::Sender::send_err`]; otherwise
/// the result is delivered normally.
pub struct TimedEnvelopeProxy<M>
where
    M: Message,
{
    pub(crate) message: Option<M>,
    pub(crate) tx: Option<oneshot::Sender<M::Result>>,
    pub(crate) budget: Duration,
}

impl<M> Debug for TimedEnvelopeProxy<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", ShortName::of::<Self>()))
    }
}

impl<M> TimedEnvelopeProxy<M>
where
    M: Message,
{
    /// Takes the message out of the envelope proxy, leaving [`None`] in its place.
    pub fn message(&mut self) -> Option<M> {
        self.message.take()
    }
}

impl<A, M> EnvelopeProxy<A> for TimedEnvelopeProxy<M>
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

                match time::timeout(self.budget, actor.handle(msg, ctx)).await {
                    Ok(result) => result.handle(ctx, tx).await,
                    Err(e) => {
                        debug!(
                            "Message handling timed out after {:.3} seconds",
                            self.budget.as_secs_f64()
                        );
                        if let Some(tx) = tx {
                            let _ = tx.send_err(e);
                        }
                    }
                }
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

impl<A, M> IntoEnvelope<A, TimedEnvelopeProxy<M>> for Timed<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn pack(self, tx: Option<oneshot::Sender<M::Result>>) -> Envelope<A> {
        let Timed { message, budget } = self;

        Envelope::with_proxy(Box::new(TimedEnvelopeProxy {
            message: Some(message),
            tx,
            budget,
        }))
    }
}

impl<A, M> FromEnvelope<A, TimedEnvelopeProxy<M>> for Timed<M>
where
    A: Actor + Handler<M>,
    M: Message,
{
    fn unpack(mut envelope: Envelope<A>) -> Timed<M> {
        let proxy = envelope
            .as_any_mut()
            .downcast_mut::<TimedEnvelopeProxy<M>>()
            .unwrap_or_else(|| {
                panic!(
                    "envelope proxy mismatch during downcast: expected TimedEnvelopeProxy<{}>",
                    crate::utils::ShortName::of::<M>(),
                )
            });
        let budget = proxy.budget;
        let message = proxy.message().unwrap_or_else(|| {
            panic!(
                "message already taken from TimedEnvelopeProxy<{}>",
                crate::utils::ShortName::of::<M>(),
            )
        });
        Timed { message, budget }
    }
}

use std::any::Any;
use std::pin::Pin;
use std::time::Duration;

use futures_util::FutureExt;
use pretty_assertions::assert_eq;
use tokio::time::{sleep, timeout};

use acktor::{
    Actor, Context, Handler, Message,
    channel::oneshot,
    envelope::{Envelope, EnvelopeProxy, FromEnvelope, ToEnvelope},
    message::MessageResponse,
};

pub struct TimedEP<M>
where
    M: Message,
{
    message: Option<M>,
    tx: Option<oneshot::Sender<M::Result>>,
    budget: Duration,
}

impl<A, M> EnvelopeProxy<A> for TimedEP<M>
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

            let Some(msg) = self.message.take() else {
                return;
            };

            match timeout(self.budget, actor.handle(msg, ctx)).await {
                Ok(result) => result.handle(ctx, tx).await,
                Err(_) => {
                    // tx goes out of scope here; the caller sees the oneshot close empty
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

impl<M> ToEnvelope<Slow, M, TimedEP<M>> for Slow
where
    Slow: Handler<M>,
    M: Message,
{
    fn pack(msg: M, tx: Option<oneshot::Sender<M::Result>>) -> Envelope<Slow> {
        Envelope::with_proxy(Box::new(TimedEP {
            message: Some(msg),
            tx,
            budget: Duration::from_millis(100),
        }))
    }
}

impl<M> FromEnvelope<Slow, M, TimedEP<M>> for Slow
where
    Slow: Handler<M>,
    M: Message,
{
    fn unpack(mut envelope: Envelope<Slow>) -> M {
        envelope
            .as_any_mut()
            .downcast_mut::<TimedEP<M>>()
            .expect("envelope type mismatch during downcast")
            .message
            .take()
            .expect("message already taken from envelope")
    }
}

#[derive(Debug)]
struct Slow;

impl Actor for Slow {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug, Message)]
#[result_type(u32)]
struct Work {
    sleep_millis: u64,
    value: u32,
}

impl Handler<Work> for Slow {
    type Result = u32;

    async fn handle(&mut self, msg: Work, _ctx: &mut Self::Context) -> u32 {
        sleep(Duration::from_millis(msg.sleep_millis)).await;
        msg.value
    }
}

#[tokio::test]
async fn handler_completes() {
    let (address, _) = Slow.run("slow").unwrap();

    let result = address
        .send::<_, TimedEP<_>>(Work {
            sleep_millis: 10,
            value: 42,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn handler_cancelled() {
    let (address, _) = Slow.run("slow").unwrap();

    // oneshot closes empty because the proxy dropped the handler future
    let result = address
        .send::<_, TimedEP<_>>(Work {
            sleep_millis: 1_000,
            value: 42,
        })
        .await
        .unwrap()
        .await;
    assert!(result.is_err());

    // actor is still alive and servicing new messages after the cancellation
    let result = address
        .send::<_, TimedEP<_>>(Work {
            sleep_millis: 10,
            value: 7,
        })
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(result, 7);
}

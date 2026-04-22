use anyhow::anyhow;
use pretty_assertions::assert_eq;

use acktor::{
    Actor, ActorContext, Context, Handler, Message,
    supervisor::{SupervisionEvent, Supervisor},
};

#[derive(Debug)]
pub struct A;

impl Actor for A {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug, Message)]
#[result_type(())]
pub struct Notify;

impl Handler<Notify> for A {
    type Result = ();

    async fn handle(&mut self, _msg: Notify, ctx: &mut Self::Context) -> Self::Result {
        ctx.notify_supervisor(SupervisionEvent::Warn(ctx.address(), anyhow!("A")))
            .await;
        ctx.notify_supervisor(SupervisionEvent::Terminated(ctx.address(), None))
            .await;
        ctx.notify_supervisor(SupervisionEvent::Terminated(
            ctx.address(),
            Some(anyhow!("A")),
        ))
        .await;
    }
}

#[derive(Debug, Message)]
#[result_type(())]
pub struct TryNotify;

impl Handler<TryNotify> for A {
    type Result = ();

    async fn handle(&mut self, _msg: TryNotify, ctx: &mut Self::Context) -> Self::Result {
        ctx.try_notify_supervisor(SupervisionEvent::Warn(ctx.address(), anyhow!("A")));
        ctx.try_notify_supervisor(SupervisionEvent::Terminated(ctx.address(), None));
        ctx.try_notify_supervisor(SupervisionEvent::Terminated(
            ctx.address(),
            Some(anyhow!("A")),
        ));
    }
}

#[derive(Debug)]
pub struct B {
    received: usize,
}

impl Actor for B {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<SupervisionEvent<A>> for B {
    type Result = ();

    async fn handle(&mut self, msg: SupervisionEvent<A>, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            SupervisionEvent::Warn(_, _) => self.received += 1,
            SupervisionEvent::Terminated(_, None) => self.received += 2,
            SupervisionEvent::Terminated(_, Some(_)) => self.received += 4,
            _ => {}
        }
    }
}

#[derive(Debug, Message)]
#[result_type(usize)]
pub struct CheckB;

impl Handler<CheckB> for B {
    type Result = usize;

    async fn handle(&mut self, _msg: CheckB, _ctx: &mut Self::Context) -> Self::Result {
        let result = self.received;
        self.received = 0;
        result
    }
}

#[tokio::test]
async fn test_supervisor() {
    let (a_address, _) = A.run("A").unwrap();

    // no effect
    a_address.send(Notify).await.unwrap().await.unwrap();
    a_address.send(TryNotify).await.unwrap().await.unwrap();

    let (b_address, _) = B { received: 0 }.run("B").unwrap();

    // set B as the supervisor of A
    let command = Supervisor::Set(b_address.clone().into());
    let debug_str = format!("{command:?}");
    assert_eq!(
        debug_str,
        format!("Set(Recipient<SupervisionEvent<A>>({}))", b_address.index())
    );
    a_address.send(command).await.unwrap().await.unwrap();

    // trigger A to notify supervisor
    a_address.send(Notify).await.unwrap().await.unwrap();

    // B should receive the supervision events
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, 7);

    // trigger A to notify supervisor
    a_address.send(TryNotify).await.unwrap().await.unwrap();

    // B should receive the supervision events
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, 7);

    // unset supervisor
    let command = Supervisor::Unset;
    let debug_str = format!("{command:?}");
    assert_eq!(debug_str, "Unset");
    a_address.send(command).await.unwrap().await.unwrap();

    // trigger A to notify supervisor
    a_address.send(Notify).await.unwrap().await.unwrap();

    // B should not receive any supervision events
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, 0);
}

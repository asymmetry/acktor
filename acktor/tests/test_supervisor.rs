use anyhow::{Result, anyhow};
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

#[derive(Debug)]
pub struct Notify;

impl Message for Notify {
    type Result = ();
}

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

#[derive(Debug)]
pub struct TryNotify;

impl Message for TryNotify {
    type Result = ();
}

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

#[derive(Debug)]
pub struct CheckB;

impl Message for CheckB {
    type Result = usize;
}

impl Handler<CheckB> for B {
    type Result = usize;

    async fn handle(&mut self, _msg: CheckB, _ctx: &mut Self::Context) -> Self::Result {
        let result = self.received;
        self.received = 0;
        result
    }
}

#[tokio::test]
async fn test_supervisor() -> Result<()> {
    let (a_address, _) = A.run("A")?;

    // no effect
    a_address.send(Notify).await?.await?;
    a_address.send(TryNotify).await?.await?;

    let (b_address, _) = B { received: 0 }.run("B")?;

    // set B as the supervisor of A
    a_address
        .send(Supervisor::Set(b_address.clone().into()))
        .await?
        .await?;

    // trigger A to notify supervisor
    a_address.send(Notify).await?.await?;

    // B should receive the supervision events
    let received = b_address.send(CheckB).await?.await?;
    assert_eq!(received, 7);

    // trigger A to notify supervisor
    a_address.send(TryNotify).await?.await?;

    // B should receive the supervision events
    let received = b_address.send(CheckB).await?.await?;
    assert_eq!(received, 7);

    // unset supervisor
    a_address.send(Supervisor::Unset).await?.await?;

    // trigger A to notify supervisor
    a_address.send(Notify).await?.await?;

    // B should not receive any supervision events
    let received = b_address.send(CheckB).await?.await?;
    assert_eq!(received, 0);

    Ok(())
}

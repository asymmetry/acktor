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
#[result_type = "()"]
pub struct PingA;

impl Handler<PingA> for A {
    type Result = ();

    async fn handle(&mut self, _msg: PingA, ctx: &mut Self::Context) -> Self::Result {
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
#[result_type = "()"]
pub struct TryPingA;

impl Handler<TryPingA> for A {
    type Result = ();

    async fn handle(&mut self, _msg: TryPingA, ctx: &mut Self::Context) -> Self::Result {
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
    received: bool,
}

impl Actor for B {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<SupervisionEvent<A>> for B {
    type Result = ();

    async fn handle(
        &mut self,
        _message: SupervisionEvent<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.received = true;
    }
}

#[derive(Debug, Message)]
#[result_type = "bool"]
pub struct CheckB;

impl Handler<CheckB> for B {
    type Result = bool;

    async fn handle(&mut self, _msg: CheckB, _ctx: &mut Self::Context) -> Self::Result {
        let result = self.received;
        self.received = false;
        result
    }
}

#[tokio::test]
async fn test_supervisor() {
    let (a_address, _) = A.run("A").unwrap();

    a_address.send(PingA).await.unwrap().await.unwrap();
    a_address.send(TryPingA).await.unwrap().await.unwrap();

    let (b_address, _) = B { received: false }.run("B").unwrap();

    a_address
        .send(Supervisor::Set(b_address.clone().into()))
        .await
        .unwrap()
        .await
        .unwrap();

    a_address.send(PingA).await.unwrap().await.unwrap();

    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);

    a_address.send(TryPingA).await.unwrap().await.unwrap();

    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);
}

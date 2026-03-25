use pretty_assertions::assert_eq;

use acktor::{
    Actor, Context,
    address::Recipient,
    message::{Handler, Message},
    observer::{Observer, ObserverSet, SubjectActor},
};

#[derive(Debug, Clone, Copy)]
pub struct M1;

impl Message for M1 {
    type Result = ();
}

#[derive(Debug, Clone, Copy)]

pub struct M2;

impl Message for M2 {
    type Result = ();
}

#[derive(Debug, Default)]
pub struct A {
    m1_observers: ObserverSet<M1>,
    m2_observers: ObserverSet<M2>,
}

impl Actor for A {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl SubjectActor<M1> for A {
    #[inline]
    fn observers_mut(&mut self) -> &mut ObserverSet<M1> {
        &mut self.m1_observers
    }
}

impl SubjectActor<M2> for A {
    #[inline]
    fn observers_mut(&mut self) -> &mut ObserverSet<M2> {
        &mut self.m2_observers
    }
}

#[derive(Debug)]
pub struct PingA;

impl Message for PingA {
    type Result = ();
}

impl Handler<PingA> for A {
    type Result = ();

    async fn handle(&mut self, _msg: PingA, _ctx: &mut Self::Context) -> Self::Result {
        self.notify_observers(M1).await;
    }
}

#[derive(Debug)]
pub struct TryPingA;

impl Message for TryPingA {
    type Result = ();
}

impl Handler<TryPingA> for A {
    type Result = ();

    async fn handle(&mut self, _msg: TryPingA, _ctx: &mut Self::Context) -> Self::Result {
        self.try_notify_observers(M2);
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

impl Handler<M1> for B {
    type Result = ();

    async fn handle(&mut self, _msg: M1, _ctx: &mut Self::Context) -> Self::Result {
        self.received = true;
    }
}

impl Handler<M2> for B {
    type Result = ();

    async fn handle(&mut self, _msg: M2, _ctx: &mut Self::Context) -> Self::Result {
        self.received = true;
    }
}

#[derive(Debug)]
pub struct CheckB;

impl Message for CheckB {
    type Result = bool;
}

impl Handler<CheckB> for B {
    type Result = bool;

    async fn handle(&mut self, _msg: CheckB, _ctx: &mut Self::Context) -> Self::Result {
        let result = self.received;
        self.received = false;
        result
    }
}

#[tokio::test]
async fn test_observer() {
    let (a_address, _) = A::default().run("A").unwrap();
    let (b_address, _) = B { received: false }.run("B").unwrap();
    let (recipient, mut rx) = Recipient::<M1>::create(16);

    a_address
        .send(Observer::<M1>::Register(b_address.clone().into()))
        .await
        .unwrap()
        .await
        .unwrap();

    a_address
        .send(Observer::<M2>::Register(b_address.clone().into()))
        .await
        .unwrap()
        .await
        .unwrap();

    a_address
        .send(Observer::Register(recipient.clone()))
        .await
        .unwrap()
        .await
        .unwrap();

    a_address.send(PingA).await.unwrap().await.unwrap();

    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);

    let received = rx.recv().await;
    assert_eq!(received.is_some(), true);

    a_address.send(TryPingA).await.unwrap().await.unwrap();

    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);
}

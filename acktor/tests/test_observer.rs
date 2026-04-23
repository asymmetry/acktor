use pretty_assertions::assert_eq;
use tokio::time::Duration;

use acktor::{
    Actor, Context, Handler, Message, Recipient, RecvError, SenderId,
    observer::{Observer, ObserverSet, SubjectActor},
};

#[derive(Debug, Clone, Copy, Message)]
#[result_type(())]
pub struct M1;

#[derive(Debug, Clone, Copy, Message)]
#[result_type(())]
pub struct M2;

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

#[derive(Debug, Message)]
#[result_type(())]
pub struct NotifyM1;

impl Handler<NotifyM1> for A {
    type Result = ();

    async fn handle(&mut self, _msg: NotifyM1, _ctx: &mut Self::Context) -> Self::Result {
        self.notify_observers(M1).await;
    }
}

#[derive(Debug, Message)]
#[result_type(())]
pub struct TryNotifyM2;

impl Handler<TryNotifyM2> for A {
    type Result = ();

    async fn handle(&mut self, _msg: TryNotifyM2, _ctx: &mut Self::Context) -> Self::Result {
        self.try_notify_observers(M2);
    }
}

#[derive(Debug, Message)]
#[result_type(usize)]
pub struct GetObserverCount;

impl Handler<GetObserverCount> for A {
    type Result = usize;

    async fn handle(&mut self, _msg: GetObserverCount, _ctx: &mut Self::Context) -> Self::Result {
        self.m1_observers.len() + self.m2_observers.len()
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

#[derive(Debug, Message)]
#[result_type(bool)]
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
async fn test_observer() {
    // the subject actor
    let (a_address, _) = A::default().run("A").unwrap();

    // use actor as observer
    let (b_address, b_join_handle) = B { received: false }.run("B").unwrap();

    // use none-actor backed recipientas observer
    let (recipient, mut rx) = Recipient::<M1>::create(8);

    // register none-actor backed recipient as an observer for M1
    a_address
        .send(Observer::Register(recipient.clone()))
        .await
        .unwrap()
        .await
        .unwrap();

    // register B as an observer for M1
    let command = Observer::<M1>::Register(b_address.clone().into());
    let debug_str = format!("{command:?}");
    assert_eq!(
        debug_str,
        format!("Observer<M1>::Register({})", b_address.index())
    );
    a_address.send(command).await.unwrap().await.unwrap();

    // register B as an observer for M2
    a_address
        .send(Observer::<M2>::Register(b_address.clone().into()))
        .await
        .unwrap()
        .await
        .unwrap();

    // trigger A to notify M1 observers
    a_address.send(NotifyM1).await.unwrap().await.unwrap();

    // both B and the recipient should receive M1
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);
    let received = rx.recv().await;
    assert_eq!(received.is_ok(), true);

    // trigger A to notify M2 observers
    a_address.send(TryNotifyM2).await.unwrap().await.unwrap();

    // B should receive M2
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);

    // unregister none-actor backed recipient
    let command = Observer::<M1>::Unregister(recipient.clone());
    let debug_str = format!("{command:?}");
    assert_eq!(
        debug_str,
        format!("Observer<M1>::Unregister({})", recipient.index())
    );
    a_address.send(command).await.unwrap().await.unwrap();

    let observer_count = a_address
        .send(GetObserverCount)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observer_count, 2); // B is still registered for both M1 and M2

    // unregister multiple times should not cause error
    let command = Observer::<M1>::Unregister(recipient.clone());
    a_address.send(command).await.unwrap().await.unwrap();

    let observer_count = a_address
        .send(GetObserverCount)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observer_count, 2); // B is still registered for both M1 and M2

    // trigger A to notify M1 observers
    a_address.send(NotifyM1).await.unwrap().await.unwrap();

    // B should receive M1, but the recipient should not
    let received = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_eq!(received, true);
    let received = rx.recv_timeout(Duration::from_millis(10)).await;
    assert!(matches!(received, Err(RecvError::Timeout)));

    // register none-actor backed recipient back
    a_address
        .send(Observer::Register(recipient.clone()))
        .await
        .unwrap()
        .await
        .unwrap();

    // test closed observer cleanup

    let observer_count = a_address
        .send(GetObserverCount)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observer_count, 3);

    drop(rx);

    // trigger A to notify M1 observers, this should cleanup closed M1 observers
    a_address.send(NotifyM1).await.unwrap().await.unwrap();

    let observer_count = a_address
        .send(GetObserverCount)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observer_count, 2); // the recipient gets cleaned from M1 observers

    acktor::utils::terminate_actor(b_address, b_join_handle).await;

    // trigger A to try_notify M2 observers, this should cleanup closed M2 observers
    a_address.send(TryNotifyM2).await.unwrap().await.unwrap();

    let observer_count = a_address
        .send(GetObserverCount)
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(observer_count, 1); // B gets cleaned from M2 observers
}

use std::time::Duration;

use pretty_assertions::{assert_eq, assert_ne};
use tokio::time::{self, Instant};

use acktor::{
    Actor, Handler, Message, Recipient,
    cron::{CronActor, CronActorContext, CronContext, CronSignal},
    observer::{ObserverSet, SubjectActor},
};

#[derive(Debug, Default)]
pub struct A {
    observers: ObserverSet<()>,
}

impl Actor for A {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;
}

impl SubjectActor<()> for A {
    #[inline]
    fn observers_mut(&mut self) -> &mut ObserverSet<()> {
        &mut self.observers
    }
}

impl CronActor for A {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
        self.notify_observers(()).await;
        Ok(Duration::from_millis(50))
    }
}

#[derive(Debug, Default)]
pub struct B {
    count: i32,
}

impl Actor for B {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;
}

impl CronActor for B {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
        self.count += 1;
        Ok(Duration::ZERO)
    }
}

#[derive(Debug, Message)]
#[result_type(i32)]
pub struct CheckB;

impl Handler<CheckB> for B {
    type Result = i32;

    async fn handle(&mut self, _msg: CheckB, ctx: &mut Self::Context) -> Self::Result {
        ctx.pause_task();
        self.count
    }
}

#[tokio::test]
async fn test_task() {
    let (recipient, mut rx) = Recipient::create(16);

    let (a_address, _) = A::create("A", |_| {
        let mut actor = A::default();
        actor.register_observer(recipient.clone());
        Ok(actor)
    })
    .unwrap();

    // time between two messages should be 50 ms

    let _ = rx.recv().await;
    let timestamp = Instant::now();

    let _ = rx.recv().await;
    let elapsed = timestamp.elapsed();
    #[cfg(target_os = "windows")]
    {
        let elapsed = ((elapsed.as_millis() as f64 / 32.0).round() * 32.0) as u128;
        assert_eq!(elapsed, 64);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let elapsed = ((elapsed.as_millis() as f64 / 5.0).round() * 5.0) as u128;
        assert_eq!(elapsed, 50);
    }

    // pause the cron task

    a_address
        .send(CronSignal::Pause)
        .await
        .unwrap()
        .await
        .unwrap();

    loop {
        if let Err(acktor::RecvError::Empty) = rx.try_recv() {
            break;
        }
    }

    // wait for 75 ms, no message should be received

    let result = time::timeout(Duration::from_millis(75), rx.recv()).await;
    assert!(result.is_err());

    // resume the cron task

    a_address
        .send(CronSignal::Resume)
        .await
        .unwrap()
        .await
        .unwrap();

    // time between two messages should be 50 ms

    let _ = rx.recv().await;
    let timestamp = Instant::now();

    let _ = rx.recv().await;
    let elapsed = timestamp.elapsed();
    #[cfg(target_os = "windows")]
    {
        let elapsed = ((elapsed.as_millis() as f64 / 32.0).round() * 32.0) as u128;
        assert_eq!(elapsed, 64);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let elapsed = ((elapsed.as_millis() as f64 / 5.0).round() * 5.0) as u128;
        assert_eq!(elapsed, 50);
    }
}

#[tokio::test]
async fn test_task_no_wait() {
    let (b_address, _) = B::default().run("B").unwrap();

    tokio::time::sleep(Duration::from_millis(1)).await;

    let count_1 = b_address.send(CheckB).await.unwrap().await.unwrap();
    let count_2 = b_address.send(CheckB).await.unwrap().await.unwrap();
    assert_ne!(count_1, 0);
    assert_eq!(count_1, count_2);
}

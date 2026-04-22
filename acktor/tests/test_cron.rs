use std::time::Duration;

use pretty_assertions::{assert_eq, assert_ne};
use tokio::time::{self, Instant};

use acktor::{
    Actor, ActorContext, Context, Handler, Message, Recipient, Sender,
    cron::{CronActor, CronActorContext, CronContext, CronSignal},
    supervisor::{SupervisionEvent, Supervisor},
};

#[derive(Debug)]
pub struct A {
    recipient: Recipient<()>,
}

impl A {
    pub fn new(recipient: Recipient<()>) -> Self {
        Self { recipient }
    }
}

impl Actor for A {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;
}

impl CronActor for A {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
        self.recipient.send(()).await.unwrap();
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

#[derive(Debug, Message)]
#[result_type(bool)]
pub struct IsSupervised;

impl Handler<IsSupervised> for B {
    type Result = bool;

    async fn handle(&mut self, _msg: IsSupervised, ctx: &mut Self::Context) -> Self::Result {
        ctx.supervisor().is_some()
    }
}

#[derive(Debug, Default)]
pub struct Watcher;

impl Actor for Watcher {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<SupervisionEvent<B>> for Watcher {
    type Result = ();

    async fn handle(
        &mut self,
        _msg: SupervisionEvent<B>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
    }
}

#[tokio::test]
async fn test_task() {
    let (recipient, mut rx) = Recipient::create(8);

    let (a_address, _) = A::new(recipient).run("A").unwrap();

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

#[tokio::test]
async fn test_set_unset_supervisor() {
    let (b_address, _) = B::default().run("B").unwrap();
    let (watcher_address, _) = Watcher.run("watcher").unwrap();

    // no supervisor by default
    assert!(!b_address.send(IsSupervised).await.unwrap().await.unwrap());

    // set supervisor
    b_address
        .send(Supervisor::Set(watcher_address.into()))
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(b_address.send(IsSupervised).await.unwrap().await.unwrap());

    // unset supervisor
    b_address
        .send(Supervisor::Unset)
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(!b_address.send(IsSupervised).await.unwrap().await.unwrap());
}

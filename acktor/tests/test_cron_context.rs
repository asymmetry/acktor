use std::time::Duration;

use anyhow::Result;
use pretty_assertions::assert_eq;

use acktor::{
    Actor, ActorContext, Address, Context, Handler, JoinHandle, Message, Stopping,
    cron::{CronActor, CronContext},
    supervisor::SupervisionEvent,
};

#[derive(Debug)]
struct TestActor {
    stopping_action: Stopping,
    count: usize,
}

impl Actor for TestActor {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;

    async fn stopping(&mut self, _ctx: &mut Self::Context) -> Result<Stopping, Self::Error> {
        Ok(self.stopping_action)
    }
}

impl CronActor for TestActor {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
        Ok(Duration::from_secs(3600))
    }
}

#[derive(Debug, Message)]
#[result_type(())]
struct Fail(String);

impl Handler<Fail> for TestActor {
    type Result = ();

    async fn handle(&mut self, msg: Fail, ctx: &mut Self::Context) -> Self::Result {
        ctx.save_error(anyhow::anyhow!("{}", msg.0));
    }
}

#[derive(Debug, Message)]
#[result_type(i32)]
struct Ping(i32);

impl Handler<Ping> for TestActor {
    type Result = i32;

    async fn handle(&mut self, msg: Ping, _ctx: &mut Self::Context) -> Self::Result {
        msg.0
    }
}

#[derive(Debug, Message)]
#[result_type(())]
struct Block(Duration);

impl Handler<Block> for TestActor {
    type Result = ();

    async fn handle(&mut self, msg: Block, _ctx: &mut Self::Context) -> Self::Result {
        tokio::time::sleep(msg.0).await;
    }
}

#[derive(Debug, Message)]
#[result_type(())]
struct Increment;

impl Handler<Increment> for TestActor {
    type Result = ();

    async fn handle(&mut self, _msg: Increment, _ctx: &mut Self::Context) -> Self::Result {
        self.count += 1;
    }
}

#[derive(Debug, Message)]
#[result_type(())]
struct Drain;

impl Handler<Drain> for TestActor {
    type Result = ();

    async fn handle(&mut self, _msg: Drain, ctx: &mut Self::Context) -> Self::Result {
        self.count += 1;
        ctx.drain_mailbox();
    }
}

#[derive(Debug, Message)]
#[result_type(usize)]
struct GetCount;

impl Handler<GetCount> for TestActor {
    type Result = usize;

    async fn handle(&mut self, _msg: GetCount, _ctx: &mut Self::Context) -> Self::Result {
        self.count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Warn(String),
    Terminated(Option<String>),
}

#[derive(Debug, Default)]
struct Watcher {
    events: Vec<Event>,
}

impl Actor for Watcher {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<SupervisionEvent<TestActor>> for Watcher {
    type Result = ();

    async fn handle(
        &mut self,
        msg: SupervisionEvent<TestActor>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            SupervisionEvent::Warn(_, e) => self.events.push(Event::Warn(e.to_string())),
            SupervisionEvent::Terminated(_, opt) => self
                .events
                .push(Event::Terminated(opt.map(|e| e.to_string()))),
            _ => {}
        }
    }
}

#[derive(Debug, Message)]
#[result_type(Vec<Event>)]
struct Collect;

impl Handler<Collect> for Watcher {
    type Result = Vec<Event>;

    async fn handle(&mut self, _msg: Collect, _ctx: &mut Self::Context) -> Self::Result {
        std::mem::take(&mut self.events)
    }
}

#[allow(clippy::type_complexity)]
fn spawn_actors(
    actor: TestActor,
) -> Result<(
    Address<TestActor>,
    JoinHandle<()>,
    Address<Watcher>,
    JoinHandle<()>,
)> {
    let (watcher_address, watcher_join_handle) = Watcher::default().run("watcher")?;

    let (actor_address, actor_join_handle) = TestActor::create("actor", |ctx| {
        ctx.set_supervisor(Some(watcher_address.clone().into()));
        Ok(actor)
    })?;

    Ok((
        actor_address,
        actor_join_handle,
        watcher_address,
        watcher_join_handle,
    ))
}

#[tokio::test]
async fn test_stopping_continue() -> Result<()> {
    let (actor, actor_join_handle, watcher, _) = spawn_actors(TestActor {
        stopping_action: Stopping::Continue,
        count: 0,
    })?;

    actor.do_send(Fail("boom".into())).await?;

    // the actor is still alive since `stopping` returned `Continue`
    let result = actor.send(Ping(42)).await?.await?;
    assert_eq!(result, 42);

    acktor::utils::terminate_actor(actor, actor_join_handle).await;

    let events = watcher.send(Collect).await?.await?;
    assert_eq!(
        events,
        vec![Event::Warn("boom".into()), Event::Terminated(None)],
    );

    Ok(())
}

#[tokio::test]
async fn test_stopping_stop() -> Result<()> {
    let (actor, actor_join_handle, watcher, _) = spawn_actors(TestActor {
        stopping_action: Stopping::Stop,
        count: 0,
    })?;

    actor.do_send(Fail("boom".into())).await?;

    // the actor is stopped since `stopping` returned `Stop`
    actor_join_handle.await?;

    let events = watcher.send(Collect).await?.await?;
    assert_eq!(events, vec![Event::Terminated(Some("boom".into()))]);

    Ok(())
}

#[tokio::test]
async fn test_drain_mailbox() -> Result<()> {
    let (actor, _, _, _) = spawn_actors(TestActor {
        stopping_action: Stopping::Stop,
        count: 0,
    })?;

    // block occupies the actor long enough for the remaining messages to queue behind it
    // in a deterministic order
    let block_rx = actor.send(Block(Duration::from_millis(30))).await?;

    // mailbox order while actor is blocked: [Increment, Increment, Drain, Increment, Increment]
    actor.do_send(Increment).await?;
    actor.do_send(Increment).await?;
    actor.do_send(Drain).await?;
    actor.do_send(Increment).await?;
    actor.do_send(Increment).await?;

    // wait for block to finish, then give the actor time to process the first 3 messages
    // and drain the rest on the next loop iteration.
    block_rx.await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // count arrives after the mailbox has been drained
    let count = actor.send(GetCount).await?.await?;
    // only Increment, Increment, Drain were processed (count = 3)
    assert_eq!(count, 3);

    Ok(())
}

use anyhow::{Result, anyhow};
use pretty_assertions::assert_eq;

use acktor::{
    Actor, ActorContext, ActorState, Context, Handler, Message, Signal,
    supervisor::SupervisionEvent,
};

#[derive(Debug)]
pub struct Normal;

impl Actor for Normal {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug)]
pub struct PostStartFailed;

impl Actor for PostStartFailed {
    type Context = Context<Self>;
    type Error = anyhow::Error;

    async fn post_start(&mut self, _ctx: &mut Self::Context) -> Result<(), Self::Error> {
        Err(anyhow!("post_start failed"))
    }
}

#[derive(Debug, Default)]
pub struct Watcher {
    states: Vec<ActorState>,
}

impl Actor for Watcher {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl<A> Handler<SupervisionEvent<A>> for Watcher
where
    A: Actor,
{
    type Result = ();

    async fn handle(&mut self, msg: SupervisionEvent<A>, _ctx: &mut Self::Context) -> Self::Result {
        if let SupervisionEvent::State(_, state) = msg {
            self.states.push(state);
        }
    }
}

#[derive(Debug, Message)]
#[result_type(Vec<ActorState>)]
pub struct Check;

impl Handler<Check> for Watcher {
    type Result = Vec<ActorState>;

    async fn handle(&mut self, _msg: Check, _ctx: &mut Self::Context) -> Self::Result {
        std::mem::take(&mut self.states)
    }
}

#[tokio::test]
async fn test_normal() -> Result<()> {
    let (watcher, _) = Watcher::default().run("watcher")?;

    let (normal, join_handle) = Normal::create("normal", |ctx| {
        ctx.set_supervisor(Some(watcher.clone().into()));
        Ok(Normal)
    })?;

    normal.do_send(Signal::Stop).await?;
    join_handle.await?;

    let states = watcher.send(Check).await?.await?;
    assert_eq!(
        states,
        vec![
            ActorState::Starting,
            ActorState::Running,
            ActorState::Stopping,
            ActorState::Stopped,
        ],
    );

    let (normal, join_handle) = Normal::create("normal", |ctx| {
        ctx.set_supervisor(Some(watcher.clone().into()));
        Ok(Normal)
    })?;

    normal.do_send(Signal::Terminate).await?;
    join_handle.await?;

    let states = watcher.send(Check).await?.await?;
    assert_eq!(
        states,
        vec![
            ActorState::Starting,
            ActorState::Running,
            ActorState::Stopped,
        ],
    );

    Ok(())
}

#[tokio::test]
async fn test_post_start_failed() -> Result<()> {
    let (watcher, _) = Watcher::default().run("watcher")?;

    let (_, join_handle) = PostStartFailed::create("fail", |ctx| {
        ctx.set_supervisor(Some(watcher.clone().into()));
        Ok(PostStartFailed)
    })?;

    join_handle.await?;

    let states = watcher.send(Check).await?.await?;

    assert!(
        states.contains(&ActorState::Starting),
        "expected Starting in {states:?}",
    );
    assert_eq!(
        states.last(),
        Some(&ActorState::Stopped),
        "actor must end in Stopped state, got {states:?}",
    );

    Ok(())
}

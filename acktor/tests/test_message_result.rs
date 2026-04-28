use std::time::Duration;

use anyhow::Result;
use pretty_assertions::assert_eq;

use acktor::{
    Actor, Context, Handler, Message,
    message::{FutureMessageResult, MessageResult},
};

#[derive(Debug)]
pub struct MyActor;

impl Actor for MyActor {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug, PartialEq, Eq)]
pub struct CustomRes(i32);

#[derive(Debug)]
pub struct GetCustomRes;

impl Message for GetCustomRes {
    type Result = CustomRes;
}

impl Handler<GetCustomRes> for MyActor {
    type Result = MessageResult<GetCustomRes>;

    async fn handle(&mut self, _msg: GetCustomRes, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(CustomRes(42))
    }
}

#[derive(Debug)]
pub struct GetCustomResAsync;

impl Message for GetCustomResAsync {
    type Result = i32;
}

impl Handler<GetCustomResAsync> for MyActor {
    type Result = FutureMessageResult<GetCustomResAsync>;

    async fn handle(&mut self, _msg: GetCustomResAsync, _ctx: &mut Self::Context) -> Self::Result {
        FutureMessageResult::new(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            99
        })
    }
}

#[tokio::test]
async fn test_message_result() -> Result<()> {
    let (addr, join_handle) = MyActor.run("actor")?;

    // MessageResult forwards the custom type as the message response
    let custom = addr.send(GetCustomRes).await?.await?;
    assert_eq!(custom, CustomRes(42));

    // FutureMessageResult runs detached: the handler returns immediately, so the mailbox is
    // not stalled by the 20ms sleep. Dispatch the async work, then dispatch a second message
    // that completes quickly — the second reply arrives before the first
    let slow = addr.send(GetCustomResAsync).await?;
    let fast = addr.send(GetCustomRes).await?;

    assert!(slow.is_empty());

    let fast_result = fast.await?;
    assert_eq!(fast_result, CustomRes(42));

    let slow_result = slow.await?;
    assert_eq!(slow_result, 99);

    acktor::utils::terminate_actor(addr, join_handle).await;

    Ok(())
}

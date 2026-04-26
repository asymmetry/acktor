use anyhow::Result;
use pretty_assertions::assert_eq;
use tokio::time::{self, Duration};

use acktor::{Actor, Context, Handler, Message, SendError, Signal, envelope::Timed};

#[derive(Debug)]
struct Slow;

impl Actor for Slow {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug, Message)]
#[result_type(u32)]
struct Work {
    expense: Duration,
    value: u32,
}

impl Handler<Work> for Slow {
    type Result = u32;

    async fn handle(&mut self, msg: Work, _ctx: &mut Self::Context) -> u32 {
        time::sleep(msg.expense).await;
        msg.value
    }
}

#[tokio::test]
async fn test_handler_done() -> Result<()> {
    let (address, _) = Slow.run("slow")?;

    let result = address
        .send(Timed::new(
            Work {
                expense: Duration::from_millis(100),
                value: 42,
            },
            Duration::from_millis(1000),
        ))
        .await?
        .await?;
    assert_eq!(result, 42);

    Ok(())
}

#[tokio::test]
async fn test_unpack_message() -> Result<()> {
    let (address, join_handle) = Slow.run("slow")?;

    // close the actor's mailbox by terminating it
    address.do_send(Signal::Terminate).await?;
    join_handle.await?;

    // sending now fails; the SendError carries the original Timed<Work> back
    let timed = Timed::new(
        Work {
            expense: Duration::from_millis(100),
            value: 99,
        },
        Duration::from_millis(50),
    );
    let recovered = match address.try_send(timed) {
        Ok(_) => panic!("send should fail after the actor is terminated"),
        Err(SendError::Closed(timed)) => timed,
        Err(other) => panic!("expected Closed, got {other:?}"),
    };
    let (work, budget) = recovered.into_parts();
    assert_eq!(work.value, 99);
    assert_eq!(budget, Duration::from_millis(50));

    Ok(())
}

#[tokio::test]
async fn test_handler_cancelled() -> Result<()> {
    let (address, _) = Slow.run("slow")?;

    // oneshot closes empty because the proxy dropped the handler future
    let result = address
        .send(Timed::new(
            Work {
                expense: Duration::from_millis(100),
                value: 42,
            },
            Duration::from_millis(10),
        ))
        .await?
        .await;
    assert!(result.is_err());

    // actor is still alive and servicing new messages after the cancellation
    let result = address
        .send(Timed::new(
            Work {
                expense: Duration::from_millis(100),
                value: 7,
            },
            Duration::from_millis(1000),
        ))
        .await?
        .await?;
    assert_eq!(result, 7);

    Ok(())
}

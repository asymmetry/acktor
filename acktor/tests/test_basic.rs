use std::time::Duration;

use pretty_assertions::assert_eq;

use acktor::{Actor, Context, Handler, Message, MessageResponse, Signal};

#[derive(Debug, MessageResponse)]
pub struct Res(i64);

#[derive(Debug)]
pub struct Number(i64);

impl Actor for Number {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug, Message)]
#[result_type(Res)]
pub enum Arithmetic {
    Add(i64),
    Subtract(i64),
    Multiply(i64),
    Divide(i64),
}

impl Handler<Arithmetic> for Number {
    type Result = Res;

    async fn handle(&mut self, msg: Arithmetic, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Arithmetic::Add(n) => self.0 += n,
            Arithmetic::Subtract(n) => self.0 -= n,
            Arithmetic::Multiply(n) => self.0 *= n,
            Arithmetic::Divide(n) => self.0 /= n,
        }
        Res(self.0)
    }
}

#[derive(Debug, Message)]
#[result_type(i64)]
pub enum Command {
    Get,
    Set(i64),
}

impl Handler<Command> for Number {
    type Result = i64;

    async fn handle(&mut self, msg: Command, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Command::Get => self.0,
            Command::Set(n) => {
                self.0 = n;
                self.0
            }
        }
    }
}

#[tokio::test]
async fn test_basic() {
    // test run
    let (address, join_handle) = Number(16).run("Number").unwrap();

    // test send
    let result = address
        .send(Arithmetic::Add(32))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(result.0, 48);

    // test do_send
    address.do_send(Arithmetic::Subtract(64)).await.unwrap();
    let result = address.send(Command::Get).await.unwrap().await.unwrap();
    assert_eq!(result, -16);

    // test try_send
    let result = address
        .try_send(Arithmetic::Add(10))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(result.0, -6);

    // test try_do_send
    address.try_do_send(Arithmetic::Subtract(5)).unwrap();
    let result = address.send(Command::Get).await.unwrap().await.unwrap();
    assert_eq!(result, -11);

    // test send_timeout
    let result = address
        .send_timeout(Arithmetic::Multiply(2), Duration::from_secs(1))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(result.0, -22);

    // test do_send_timeout
    address
        .do_send_timeout(Arithmetic::Divide(2), Duration::from_secs(1))
        .await
        .unwrap();
    let result = address.send(Command::Get).await.unwrap().await.unwrap();
    assert_eq!(result, -11);

    let addr = address.clone();
    let result = tokio::task::spawn_blocking(move || {
        // test blocking_do_send
        addr.blocking_do_send(Arithmetic::Add(4)).unwrap();

        // test blocking_send
        let rx = addr.blocking_send(Command::Get).unwrap();
        rx.blocking_recv().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result, -7);

    // test stop
    address.do_send(Signal::Stop).await.unwrap();
    join_handle.await.unwrap();

    // test create
    let (address, join_handle) = Number::create("Number", |_| Ok(Number(16))).unwrap();

    // test terminate
    acktor::utils::terminate_actor(address, join_handle).await;

    // test create_in_span with no parent (root span)
    let (address, join_handle) =
        Number::create_in_span("Number", None, |_| Ok(Number(16))).unwrap();

    acktor::utils::terminate_actor(address, join_handle).await;
}

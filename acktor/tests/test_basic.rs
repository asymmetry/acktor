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

    // test stop
    address.do_send(Signal::Stop).await.unwrap();
    join_handle.await.unwrap();

    // test create
    let (address, join_handle) = Number::create("Number", |_| Ok(Number(16))).unwrap();

    // test terminate
    address.do_send(Signal::Terminate).await.unwrap();
    join_handle.await.unwrap();
}

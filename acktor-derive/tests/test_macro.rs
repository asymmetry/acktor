use pretty_assertions::assert_eq;

use acktor::{Actor, Context, Signal, message::Handler};
use acktor_derive::{Message, MessageResponse};

#[derive(MessageResponse)]
struct Sum(i64);

#[derive(Message)]
#[result_type = "Sum"]
struct Add(i64, i64);

struct Adder;

impl Actor for Adder {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Add> for Adder {
    type Result = Sum;

    async fn handle(&mut self, msg: Add, _ctx: &mut Self::Context) -> Self::Result {
        Sum(msg.0 + msg.1)
    }
}

#[tokio::test]
async fn test_macro() {
    let (address, join_handle) = Adder.run("Adder").unwrap();

    let sum = address.send(Add(1, 2)).await.unwrap().await.unwrap();
    assert_eq!(sum.0, 3);

    address.do_send(Signal::Terminate).await.unwrap();

    join_handle.await.unwrap();
}

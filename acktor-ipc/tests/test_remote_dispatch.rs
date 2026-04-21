use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Address, Context, Handler, Message, channel::oneshot};
use acktor_ipc::{Decode, Encode, RemoteActor, RemoteMessage};

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[codec(zerocopy)]
#[index(1)]
#[repr(C)]
#[result_type(i64)]
pub struct Double {
    pub value: i64,
}

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[codec(zerocopy)]
#[index(2)]
#[repr(C)]
#[result_type(i64)]
pub struct Triple {
    pub value: i64,
}

#[derive(Debug, RemoteActor)]
#[message(Double, Triple)]
pub struct Calculator;

impl Actor for Calculator {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Double> for Calculator {
    type Result = i64;
    async fn handle(&mut self, msg: Double, _ctx: &mut Self::Context) -> Self::Result {
        msg.value * 2
    }
}

impl Handler<Triple> for Calculator {
    type Result = i64;
    async fn handle(&mut self, msg: Triple, _ctx: &mut Self::Context) -> Self::Result {
        msg.value * 3
    }
}

async fn roundtrip<M: Encode + Message>(addr: &Address<Calculator>, msg: M) -> i64
where
    <M as Message>::Result: 'static,
{
    let bytes = msg.encode_to_bytes(None).unwrap();
    let (tx, rx) = oneshot::channel::<Bytes>();
    let rm = RemoteMessage::send(0, <M as acktor_ipc::Encode>::ID, bytes, tx);
    addr.do_send(rm).await.unwrap();
    let bytes = rx.await.unwrap();
    <i64 as Decode>::decode(bytes, None).unwrap()
}

#[tokio::test]
async fn dispatches_by_message_id() {
    let (addr, handle) = Calculator.run("calc").unwrap();

    assert_eq!(roundtrip(&addr, Double { value: 5 }).await, 10);
    assert_eq!(roundtrip(&addr, Triple { value: 5 }).await, 15);

    acktor::utils::terminate_actor(addr, handle).await;
}

#[tokio::test]
async fn unknown_id_returns_error() {
    let (addr, handle) = Calculator.run("calc").unwrap();

    let (tx, rx) = oneshot::channel::<Bytes>();
    // 99 is not Double::ID or Triple::ID
    let rm = RemoteMessage::send(0, 99, Bytes::new(), tx);
    addr.do_send(rm).await.unwrap();

    let err = rx.await.unwrap_err();
    assert!(format!("{err:?}").contains("UnknownMessageId"));

    acktor::utils::terminate_actor(addr, handle).await;
}

use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Address, Context, Handler, Message, channel::oneshot};
use acktor_ipc::{Decode, Encode, RemoteActor, RemoteMessage};

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[result_type(i64)]
#[codec(zerocopy)]
#[index(1)]
#[repr(C)]
pub struct Double {
    pub value: i64,
}

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[result_type(i64)]
#[codec(zerocopy)]
#[index(2)]
#[repr(C)]
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

async fn roundtrip<M>(address: &Address<Calculator>, msg: M) -> i64
where
    M: Message + Encode,
    M::Result: Decode,
{
    let bytes = msg.encode_to_bytes(None).unwrap();
    let (tx, rx) = oneshot::channel::<Bytes>();
    let rm = RemoteMessage::send(0, <M as acktor_ipc::Encode>::ID, bytes, tx);
    address.do_send(rm).await.unwrap();
    let bytes = rx.await.unwrap();
    <i64 as Decode>::decode(bytes, None).unwrap()
}

#[tokio::test]
async fn test_derived_remote_actor() {
    let (address, handle) = Calculator.run("calc").unwrap();

    assert_eq!(roundtrip(&address, Double { value: 5 }).await, 10);
    assert_eq!(roundtrip(&address, Triple { value: 5 }).await, 15);

    let (tx, rx) = oneshot::channel::<Bytes>();
    // 99 is not Double::ID or Triple::ID
    let remote_message = RemoteMessage::send(0, 99, Bytes::new(), tx);
    let debug_str = format!("{remote_message:?}");
    assert_eq!(
        debug_str,
        "RemoteMessage { actor_id: 0, message_id: 99, message: Bytes(0), result_tx: Send }"
    );
    address.do_send(remote_message).await.unwrap();

    let err = rx.await.unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownMessageId"));

    acktor::utils::terminate_actor(address, handle).await;
}

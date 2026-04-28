use anyhow::Result;
use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Address, Context, Handler, Message, MessageId, channel::oneshot};
use acktor_ipc::{Decode, Encode, RemoteActor, RemoteMessage};

#[derive(
    Debug,
    Clone,
    Copy,
    KnownLayout,
    Immutable,
    FromBytes,
    IntoBytes,
    Message,
    MessageId,
    Encode,
    Decode,
)]
#[result_type(i64)]
#[codec(zerocopy)]
#[repr(C)]
pub struct Double {
    pub value: i64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    KnownLayout,
    Immutable,
    FromBytes,
    IntoBytes,
    Message,
    MessageId,
    Encode,
    Decode,
)]
#[result_type(i64)]
#[codec(zerocopy)]
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

async fn roundtrip<M>(address: &Address<Calculator>, msg: M) -> Result<i64>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    let bytes = msg.encode_to_bytes(None)?;
    let (tx, rx) = oneshot::channel::<Bytes>();
    let rm = RemoteMessage::send(0, M::ID, bytes, tx);
    address.do_send(rm).await?;
    let bytes = rx.await?;
    Ok(<i64 as Decode>::decode(bytes, None)?)
}

#[tokio::test]
async fn test_derived_remote_actor() -> Result<()> {
    let (address, handle) = Calculator.run("calc")?;

    assert_eq!(roundtrip(&address, Double { value: 5 }).await?, 10);
    assert_eq!(roundtrip(&address, Triple { value: 5 }).await?, 15);

    let (tx, rx) = oneshot::channel::<Bytes>();
    address
        .do_send(RemoteMessage::send(0, 99, Bytes::new(), tx))
        .await?;

    let err = rx.await.unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownMessageId"));

    acktor::utils::terminate_actor(address, handle).await;

    Ok(())
}

use tokio::time::Duration;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Address, Context, Handler, JoinHandle, Message, MessageId, SenderInfo};
use acktor_ipc::{
    Decode, Encode, Node, NodeError, RemoteAddressable, RemoteSpawnable, StableId,
    ipc_method::websocket::{WebSocketConnection, WebSocketListener},
    node::command,
    remote,
};

mod common;
use common::{connect, pick_free_port, start_client};

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
pub struct Inc {
    pub by: i64,
}

#[derive(Debug, Default, RemoteAddressable, StableId)]
#[message(Inc)]
pub struct Counter {
    total: i64,
}

#[remote]
impl Actor for Counter {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl RemoteSpawnable for Counter {
    fn create_remote(
        label: String,
        _config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), Self::Error> {
        Counter::default().start(label)
    }
}

impl Handler<Inc> for Counter {
    type Result = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Self::Context) -> i64 {
        self.total += msg.by;
        self.total
    }
}

#[tokio::test]
async fn test_remote_spawnable() -> anyhow::Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    let listener = WebSocketListener::bind(&bind_addr).await?;
    // start a server with a pre-registered local actor
    let (address, _) = Counter::default().start("counter-0")?;
    let (server, server_join_handle) = Node::new()
        .with_listener(listener)
        .with_actor("counter-0", address)
        .with_factory::<Counter>()
        .start("server")?;

    let (client, client_join_handle) = start_client()?;
    let session = connect::<WebSocketConnection>(&client, endpoint).await?;

    // test RemoteCreateActor
    let remote = client
        .send(command::RemoteCreateActor::<Counter>::new(
            session.clone().into(),
            "counter-1",
            None,
        ))
        .await?
        .await??;
    assert!(remote.is_remote());
    assert!(!remote.is_closed());

    // test RemoteGetActor by label
    let same = client
        .send(command::RemoteGetActor::<Counter>::new(
            session.clone().into(),
            "counter-1".into(),
        ))
        .await?
        .await??;
    assert_eq!(same.index(), remote.index());

    // test the created actor
    let mut rx = remote.send(Inc { by: 5 }).await?;
    let total = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(total, 5);

    let mut rx = same.send(Inc { by: 10 }).await?;
    let total = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(total, 15);

    // test RemoteCreateActor with duplicate label
    let error = client
        .send(command::RemoteCreateActor::<Counter>::new(
            session.clone().into(),
            "counter-1",
            None,
        ))
        .await?
        .await?
        .unwrap_err();
    assert!(matches!(error, NodeError::SessionError(_)),);

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

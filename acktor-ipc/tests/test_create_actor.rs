use tokio::time::Duration;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{
    Actor, Address, Context, Handler, JoinHandle, Message, MessageId, Sender, SenderInfo,
};
use acktor_ipc::{
    Decode, Encode, Node, NodeError, RemoteActor, RemoteSpawnable,
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

#[derive(Debug, Default, RemoteActor)]
#[message(Inc)]
pub struct Counter {
    total: i64,
}

#[remote_actor]
impl Actor for Counter {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Inc> for Counter {
    type Result = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Self::Context) -> i64 {
        self.total += msg.by;
        self.total
    }
}

impl RemoteSpawnable for Counter {
    const LABEL: &'static str = "Counter";

    fn create_remote(
        label: String,
        _config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), Self::Error> {
        Counter::default().start(label)
    }
}

#[tokio::test]
async fn test_create_actor() -> anyhow::Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    let listener = WebSocketListener::bind(&bind_addr).await?;
    let (server, server_join_handle) = Node::new()
        .with_listener(listener)
        .with_remote_spawnable_actor::<Counter>()
        .start("server")?;

    let (client, client_join_handle) = start_client()?;
    let session = connect::<WebSocketConnection>(&client, endpoint).await?;

    // test CreateRemoteActor
    let remote = client
        .send(command::CreateRemoteActor {
            session: session.clone().into(),
            label: "counter-1".to_string(),
            r#type: Counter::LABEL.to_string(),
            config: String::new(),
        })
        .await?
        .await??;
    assert!(remote.is_remote());
    assert!(!remote.is_closed());

    // test GetRemoteActor by label
    let same = client
        .send(command::GetRemoteActor {
            session: session.clone().into(),
            actor: "counter-1".into(),
        })
        .await?
        .await??;
    assert_eq!(same.index(), remote.index());

    // test the created actor
    let mut rx = remote.send(Inc { by: 5 }).await?;
    let total = rx.recv_timeout(Duration::from_millis(500)).await?;
    assert_eq!(total, 5);

    let mut rx = same.send(Inc { by: 10 }).await?;
    let total = rx.recv_timeout(Duration::from_millis(500)).await?;
    assert_eq!(total, 15);

    // test CreateRemoteActor with duplicate label
    let error = client
        .send(command::CreateRemoteActor {
            session: session.clone().into(),
            label: "counter-1".to_string(),
            r#type: Counter::LABEL.to_string(),
            config: String::new(),
        })
        .await?
        .await?
        .unwrap_err();
    assert!(
        matches!(error, NodeError::CreateRemoteActorFailed(_)),
        "expected CreateRemoteActorFailed for duplicate label, got {error:?}"
    );

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

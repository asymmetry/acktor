use std::collections::HashSet;

use tokio::time::{Duration, timeout};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Context, Handler, Message, Recipient, Sender, SenderId};
use acktor_ipc::{
    ActorHandle, Decode, Encode, RemoteActor, RemoteAddress,
    ipc_method::websocket::WebSocketConnection, node::command, remote,
    session::command as session_command,
};

mod common;
use common::{connect, pick_free_port, start_client, start_websocket_server};

// Minimal echo remote actor: doubles the input.
#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[result_type(i64)]
#[codec(zerocopy)]
#[index(1)]
#[repr(C)]
pub struct Echo {
    pub value: i64,
}

#[derive(Debug, RemoteActor)]
#[message(Echo)]
pub struct EchoServer;

#[remote]
impl Actor for EchoServer {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Echo> for EchoServer {
    type Result = i64;

    async fn handle(&mut self, msg: Echo, _ctx: &mut Self::Context) -> Self::Result {
        msg.value * 2
    }
}

#[tokio::test]
async fn test_remote_address() -> anyhow::Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    // spawn the echo actor and register it on the server node (clone the address so we can
    // terminate it explicitly at the end of the test)
    let (address, join_handle) = EchoServer.run("echo")?;
    let (server, server_join_handle) = start_websocket_server(&bind_addr).await?;
    server
        .send(command::AddActor {
            label: "echo".to_string(),
            address: address.clone(),
        })
        .await?
        .await?;

    let (client, client_join_handle) = start_client()?;
    let client_session = connect::<WebSocketConnection>(&client, endpoint).await?;

    // resolve the remote echo actor by its known index
    let remote = client_session
        .send(session_command::GetRemoteActor {
            actor: ActorHandle::Index(address.index()),
        })
        .await?
        .await??;
    assert_eq!(
        remote.index(),
        RemoteAddress::REMOTE_FLAG
            | ((client_session.index().reverse_bits() >> 1) ^ address.index())
    );

    // two remote addresses created with GetRemoteActor should be equal
    let duplicate = client_session
        .send(session_command::GetRemoteActor {
            actor: ActorHandle::Index(address.index()),
        })
        .await?
        .await??;
    assert_eq!(remote, duplicate);
    assert_eq!(remote.index(), duplicate.index());

    #[allow(clippy::mutable_key_type)]
    let mut map = HashSet::new();
    map.insert(remote.clone());
    map.insert(duplicate.clone());
    assert_eq!(
        map.len(),
        1,
        "two remote addresses created with GetRemoteActor should have the same hash"
    );

    // properties
    assert!(remote.is_remote());
    assert!(!remote.is_closed());
    assert_eq!(remote.capacity(), acktor::DEFAULT_MAILBOX_CAPACITY);

    // send
    let value = 1;
    let mut rx = remote.send(Echo { value }).await?;
    let result = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(result, value * 2);

    // send_timeout
    let value = 2;
    let mut rx = remote
        .send_timeout(Echo { value }, Duration::from_secs(5))
        .await?;
    let result = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(result, value * 2);

    // try_send
    let value = 3;
    let mut rx = remote.try_send(Echo { value })?;
    let result = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(result, value * 2);

    // blocking_send
    let value = 4;
    let remote_clone = remote.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
        let rx = remote_clone.blocking_send(Echo { value })?;
        Ok(rx.blocking_recv()?)
    })
    .await??;
    assert_eq!(result, value * 2);

    // do_send
    let value = 5;
    remote.do_send(Echo { value }).await?;

    // do_send_timeout
    let value = 6;
    remote
        .do_send_timeout(Echo { value }, Duration::from_secs(5))
        .await?;

    // try_do_send
    let value = 7;
    remote.try_do_send(Echo { value })?;

    // blocking_do_send
    let value = 8;
    let remote_clone = remote.clone();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        remote_clone.blocking_do_send(Echo { value })?;
        Ok(())
    })
    .await??;

    let recipient: Recipient<Echo> = remote.clone().into();

    assert!(!recipient.is_closed());
    assert_eq!(recipient.capacity(), acktor::DEFAULT_MAILBOX_CAPACITY);

    // send via Recipient
    let value = 99;
    let mut rx = recipient.send(Echo { value }).await?;
    let result = rx.recv_timeout(Duration::from_millis(100)).await?;
    assert_eq!(result, value * 2);

    // closed
    let closed = recipient.closed();

    acktor::utils::terminate_actor(address, join_handle).await;
    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    assert!(remote.is_closed());
    timeout(Duration::from_millis(500), closed).await?;

    Ok(())
}

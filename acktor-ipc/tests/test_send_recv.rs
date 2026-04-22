use std::time::Duration;

use tokio::net::TcpListener;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Context, Handler, Message, Sender};
use acktor_ipc::{
    ActorHandle, Decode, Encode, Node, RemoteActor, RemoteAddress,
    ipc_method::websocket::{WebSocketConnection, WebSocketListener},
    node::command,
    remote,
    session::command as session_command,
};

async fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

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
async fn test_send_recv() {
    let port = pick_free_port().await;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    // spawn the echo actor and register it on the server node (clone the address so we can
    // terminate it explicitly at the end of the test)
    let (echo_addr, echo_handle) = EchoServer.run("echo").expect("echo run");
    let echo_id = acktor::SenderId::index(&echo_addr);

    let listener = WebSocketListener::bind(&bind_addr)
        .await
        .expect("bind listener");
    let (server_addr, server_handle) = Node::new()
        .with_listener(listener)
        .with_actor(echo_addr.clone())
        .run("server")
        .expect("server node run");

    let (client_addr, client_handle) = Node::new().run("client").expect("client node run");

    let session = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = client_addr
                .send(command::Connect::<WebSocketConnection>::new(
                    endpoint.clone(),
                    Some("server-session".to_string()),
                ))
                .await
                .expect("send Connect")
                .await
                .expect("await Connect response");
            if let Ok(session) = result {
                return session;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Connect did not succeed within timeout");

    // resolve the remote echo actor by its known index
    let remote: RemoteAddress = session
        .send(session_command::GetRemoteActor {
            actor: ActorHandle::Index(echo_id),
        })
        .await
        .expect("send GetRemoteActor")
        .await
        .expect("await GetRemoteActor response")
        .expect("actor not found in remote node");

    // send
    let value = 1;
    let mut rx = remote
        .send(Echo { value })
        .await
        .expect("send returned SendError");
    let result = rx
        .recv_timeout(Duration::from_millis(100))
        .await
        .expect("echo response failed");
    assert_eq!(result, value * 2);

    // send_timeout
    let value = 2;
    let mut rx = remote
        .send_timeout(Echo { value }, Duration::from_secs(5))
        .await
        .expect("send_timeout returned SendError");
    let result = rx
        .recv_timeout(Duration::from_millis(100))
        .await
        .expect("echo response failed");
    assert_eq!(result, value * 2);

    // try_send
    let value = 3;
    let mut rx = remote
        .try_send(Echo { value })
        .expect("try_send returned SendError");
    let result = rx
        .recv_timeout(Duration::from_millis(100))
        .await
        .expect("echo response failed");
    assert_eq!(result, value * 2);

    // blocking_send
    let value = 4;
    let remote_clone = remote.clone();
    let result = tokio::task::spawn_blocking(move || {
        let rx = remote_clone
            .blocking_send(Echo { value })
            .expect("blocking_send returned SendError");
        rx.blocking_recv().expect("echo response failed")
    })
    .await
    .expect("spawn_blocking join");
    assert_eq!(result, value * 2);

    // do_send
    let value = 5;
    remote
        .do_send(Echo { value })
        .await
        .expect("do_send returned SendError");

    // do_send_timeout
    let value = 6;
    remote
        .do_send_timeout(Echo { value }, Duration::from_secs(5))
        .await
        .expect("do_send_timeout returned SendError");

    // try_do_send
    let value = 7;
    remote
        .try_do_send(Echo { value })
        .expect("try_do_send returned SendError");

    // blocking_do_send
    let value = 8;
    let remote_clone = remote.clone();
    tokio::task::spawn_blocking(move || {
        remote_clone
            .blocking_do_send(Echo { value })
            .expect("blocking_do_send returned SendError");
    })
    .await
    .expect("spawn_blocking join");

    // confirm the remote actor is still responsive after the fire-and-forget barrage
    let value = 99;
    let mut rx = remote
        .send(Echo { value })
        .await
        .expect("send returned SendError");
    let result = rx
        .recv_timeout(Duration::from_millis(100))
        .await
        .expect("echo response failed");
    assert_eq!(result, value * 2);

    acktor::utils::terminate_actor(echo_addr, echo_handle).await;
    acktor::utils::terminate_actor(client_addr, client_handle).await;
    acktor::utils::terminate_actor(server_addr, server_handle).await;
}

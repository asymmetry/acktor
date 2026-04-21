use std::time::Duration;

use tokio::net::TcpListener;

use acktor::Actor;
use acktor_ipc::{
    Node,
    ipc_method::{
        pipe::{PipeConnection, PipeListener},
        websocket::{WebSocketConnection, WebSocketListener},
    },
    node::command,
};

async fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn test_basic_websocket() {
    let port = pick_free_port().await;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    // server node with a websocket listener
    let listener = WebSocketListener::bind(&bind_addr)
        .await
        .expect("bind listener");
    let (server_addr, server_handle) = Node::new()
        .with_listener(listener)
        .run("server")
        .expect("server node run");

    // client node
    let (client_addr, client_handle) = Node::new().run("client").expect("client node run");

    // connect client to server; the Connect command may race with the server's accept loop
    // coming online, so retry for a bounded time.
    let session_result = tokio::time::timeout(Duration::from_secs(5), async {
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

    // sanity check: the session has a valid index
    assert!(acktor::SenderId::index(&session_result) > 0);

    acktor::utils::terminate_actor(client_addr, client_handle).await;
    acktor::utils::terminate_actor(server_addr, server_handle).await;
}

#[tokio::test]
async fn test_basic_pipe() {
    // pipe names must be unique across concurrent test processes
    let endpoint = format!("acktor-test-{}", std::process::id());

    // server node with a pipe listener
    let listener = PipeListener::new(&endpoint).expect("bind listener");
    let (server_addr, server_handle) = Node::new()
        .with_listener(listener)
        .run("server")
        .expect("server node run");

    // client node
    let (client_addr, client_handle) = Node::new().run("client").expect("client node run");

    let session_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = client_addr
                .send(command::Connect::<PipeConnection>::new(
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

    assert!(acktor::SenderId::index(&session_result) > 0);

    acktor::utils::terminate_actor(client_addr, client_handle).await;
    acktor::utils::terminate_actor(server_addr, server_handle).await;
}

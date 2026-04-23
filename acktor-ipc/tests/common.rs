#![allow(dead_code)]

use tokio::{
    net::TcpListener,
    time::{Duration, sleep, timeout},
};

use acktor::{Actor, Address, JoinHandle};
use acktor_ipc::{Node, Session, ipc_method::IpcConnection, node::command};

pub async fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[cfg(feature = "websocket")]
pub async fn start_websocket_server(bind_addr: &str) -> (Address<Node>, JoinHandle<()>) {
    use acktor_ipc::ipc_method::websocket::WebSocketListener;

    let listener = WebSocketListener::bind(bind_addr)
        .await
        .expect("bind listener");
    Node::new()
        .with_listener(listener)
        .run("server")
        .expect("server node run")
}

#[cfg(feature = "pipe")]
pub fn start_pipe_server(endpoint: &str) -> (Address<Node>, JoinHandle<()>) {
    use acktor_ipc::ipc_method::pipe::PipeListener;

    let listener = PipeListener::new(endpoint).expect("bind listener");
    Node::new()
        .with_listener(listener)
        .run("server")
        .expect("server node run")
}

pub fn start_client() -> (Address<Node>, JoinHandle<()>) {
    Node::new().run("client").expect("client node run")
}

/// Sends `command::Connect::<C>` to `client` and retries until the server's accept loop is
/// online (or the 5-second timeout fires). Returns the client-side `Session` address.
pub async fn connect<C>(client: &Address<Node>, endpoint: String) -> Address<Session>
where
    C: IpcConnection,
{
    timeout(Duration::from_secs(5), async {
        loop {
            let result = client
                .send(command::Connect::<C>::new(
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
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Connect did not succeed within timeout")
}

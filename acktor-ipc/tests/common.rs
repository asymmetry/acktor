#![allow(dead_code)]

use anyhow::Result;
use tokio::time::{Duration, sleep, timeout};

use acktor::{Actor, Address, JoinHandle};
use acktor_ipc::{Node, Session, ipc_method::IpcConnection, node::command};

#[cfg(feature = "websocket")]
pub async fn pick_free_port() -> Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

#[cfg(feature = "websocket")]
pub async fn start_websocket_server(bind_addr: &str) -> Result<(Address<Node>, JoinHandle<()>)> {
    use acktor_ipc::ipc_method::websocket::WebSocketListener;

    let listener = WebSocketListener::bind(bind_addr).await?;
    Ok(Node::new().with_listener(listener).run("server")?)
}

#[cfg(feature = "pipe")]
pub fn start_pipe_server(endpoint: &str) -> Result<(Address<Node>, JoinHandle<()>)> {
    use acktor_ipc::ipc_method::pipe::PipeListener;

    let listener = PipeListener::new(endpoint)?;
    Ok(Node::new().with_listener(listener).run("server")?)
}

pub fn start_client() -> Result<(Address<Node>, JoinHandle<()>)> {
    Ok(Node::new().run("client")?)
}

/// Sends `command::Connect::<C>` to `client` and retries until the server's accept loop is
/// online (or the 5-second timeout fires). Returns the client-side `Session` address.
pub async fn connect<C>(client: &Address<Node>, endpoint: String) -> Result<Address<Session>>
where
    C: IpcConnection,
{
    let session = timeout(Duration::from_secs(5), async {
        loop {
            let result = client
                .send(command::Connect::<C>::new(
                    endpoint.clone(),
                    Some("server-session".to_string()),
                ))
                .await?
                .await?;
            if let Ok(session) = result {
                return Ok::<_, anyhow::Error>(session);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    Ok(session)
}

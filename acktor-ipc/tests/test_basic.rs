use anyhow::Result;

use acktor_ipc::ipc_method::{pipe::PipeConnection, websocket::WebSocketConnection};

mod common;
use common::{connect, pick_free_port, start_client, start_pipe_server, start_websocket_server};

#[tokio::test]
async fn test_basic_websocket() -> Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{}", port);
    let endpoint = format!("ws://{}", bind_addr);

    let (server, server_join_handle) = start_websocket_server(&bind_addr).await?;
    let (client, client_join_handle) = start_client()?;

    let client_session = connect::<WebSocketConnection>(&client, endpoint).await?;

    // sanity check: the session has a valid index
    assert!(client_session.index().as_local() > 0);

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_basic_pipe() -> Result<()> {
    // pipe names must be unique across concurrent test processes
    let endpoint = format!("acktor-test-{}", std::process::id());

    let (server, server_join_handle) = start_pipe_server(&endpoint)?;
    let (client, client_join_handle) = start_client()?;

    let client_session = connect::<PipeConnection>(&client, endpoint).await?;

    assert!(client_session.index().as_local() > 0);

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

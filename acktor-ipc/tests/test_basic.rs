use acktor_ipc::ipc_method::{pipe::PipeConnection, websocket::WebSocketConnection};

mod common;
use common::{connect, pick_free_port, start_client, start_pipe_server, start_websocket_server};

#[tokio::test]
async fn test_basic_websocket() {
    let port = pick_free_port().await;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    let (server, server_join_handle) = start_websocket_server(&bind_addr).await;
    let (client, client_join_handle) = start_client();

    let command = acktor_ipc::node::command::Connect::<WebSocketConnection>::new(
        endpoint.clone(),
        Some("server-session".to_string()),
    );
    let debug_str = format!("{command:?}");
    assert_eq!(
        debug_str,
        format!("Connect<WebSocketConnection>(\"{endpoint}\", Some(\"server-session\"))")
    );

    let client_session = connect::<WebSocketConnection>(&client, endpoint).await;

    // sanity check: the session has a valid index
    assert!(client_session.index() > 0);

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;
}

#[tokio::test]
async fn test_basic_pipe() {
    // pipe names must be unique across concurrent test processes
    let endpoint = format!("acktor-test-{}", std::process::id());

    let (server, server_join_handle) = start_pipe_server(&endpoint);
    let (client, client_join_handle) = start_client();

    let client_session = connect::<PipeConnection>(&client, endpoint).await;

    assert!(client_session.index() > 0);

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;
}

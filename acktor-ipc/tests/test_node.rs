use pretty_assertions::assert_eq;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Actor, Context, Handler, Message};
use acktor_ipc::{
    Decode, Encode, Node, NodeError, RemoteActor,
    ipc_method::websocket::{WebSocketConnection, WebSocketListener},
    node::command,
    remote,
};

mod common;
use common::{connect, pick_free_port, start_client, start_websocket_server};

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[result_type(())]
#[codec(zerocopy)]
#[index(1)]
#[repr(C)]
pub struct Tick {
    pub value: u64,
}

#[derive(Debug, RemoteActor)]
#[message(Tick)]
pub struct Dummy;

#[remote]
impl Actor for Dummy {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Tick> for Dummy {
    type Result = ();
    async fn handle(&mut self, _: Tick, _: &mut Self::Context) {}
}

#[tokio::test]
async fn test_add_remove_listener() -> anyhow::Result<()> {
    let (node, node_join_handle) = Node::new().run("node")?;

    // 1st add
    let port_1 = pick_free_port().await?;
    let bind_addr_1 = format!("127.0.0.1:{port_1}");
    let listener_1 = WebSocketListener::bind(&bind_addr_1).await?;
    let succeed = node.send(command::AddListener(listener_1)).await?.await?;
    assert!(succeed);

    // 2nd add
    let port_2 = pick_free_port().await?;
    let bind_addr_2 = format!("127.0.0.1:{port_2}");
    let listener_2 = WebSocketListener::bind(&bind_addr_2).await?;
    let succeed = node.send(command::AddListener(listener_2)).await?.await?;
    assert!(succeed);

    // remove 1st
    let succeed = node
        .send(command::RemoveListener(bind_addr_1.clone()))
        .await?
        .await?;
    assert!(succeed);

    // remove 1st again
    let succeed = node
        .send(command::RemoveListener(bind_addr_1.clone()))
        .await?
        .await?;
    assert!(!succeed); // should report false

    // remove unknown
    let succeed = node
        .send(command::RemoveListener("127.0.0.1:0".to_string()))
        .await?
        .await?;
    assert!(!succeed); // should report false

    acktor::utils::terminate_actor(node, node_join_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_add_remove_actor() -> anyhow::Result<()> {
    let (node, node_join_handle) = Node::new().run("node")?;
    let (dummy, dummy_join_handle) = Dummy.run("dummy")?;
    let dummy_idx = dummy.index();

    // add
    let succeed = node
        .send(command::AddActor {
            label: "dummy".to_string(),
            address: dummy.clone(),
        })
        .await?
        .await?;
    assert!(succeed);

    // add again with the same label and address
    let succeed = node
        .send(command::AddActor {
            label: "dummy".to_string(),
            address: dummy.clone(),
        })
        .await?
        .await?;
    assert!(!succeed); // duplicate label rejected

    // remove
    let succeed = node.send(command::RemoveActor(dummy_idx)).await?.await?;
    assert!(succeed);

    // remove again
    let succeed = node.send(command::RemoveActor(dummy_idx)).await?.await?;
    assert!(!succeed); // should report false

    acktor::utils::terminate_actor(dummy, dummy_join_handle).await;
    acktor::utils::terminate_actor(node, node_join_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_debug_fmt() -> anyhow::Result<()> {
    // AddListener
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let listener = WebSocketListener::bind(&bind_addr).await?;
    let cmd = command::AddListener(listener);
    assert_eq!(
        format!("{cmd:?}"),
        format!("AddListener<WebSocketListener>({bind_addr})")
    );

    // AddActor
    let (dummy, dummy_join_handle) = Dummy.run("dummy")?;
    let dummy_idx = dummy.index();
    let cmd = command::AddActor {
        label: "dummy".to_string(),
        address: dummy.clone(),
    };
    assert_eq!(
        format!("{cmd:?}"),
        format!("AddActor<Dummy>(\"dummy\", {dummy_idx})")
    );
    acktor::utils::terminate_actor(dummy, dummy_join_handle).await;

    // Connect with a session label
    let cmd = command::Connect::<WebSocketConnection>::new(
        "ws://localhost:9000".to_string(),
        Some("session-x".to_string()),
    );
    assert_eq!(
        format!("{cmd:?}"),
        "Connect<WebSocketConnection>(\"ws://localhost:9000\", Some(\"session-x\"))"
    );

    // Connect without a session label
    let cmd = command::Connect::<WebSocketConnection>::new("ws://localhost:9000".to_string(), None);
    assert_eq!(
        format!("{cmd:?}"),
        "Connect<WebSocketConnection>(\"ws://localhost:9000\", None)"
    );

    Ok(())
}

#[tokio::test]
async fn test_actor_commands() -> anyhow::Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    let (server, server_join_handle) = start_websocket_server(&bind_addr).await?;
    let (client, client_join_handle) = start_client()?;

    // test Connect
    let session = connect::<WebSocketConnection>(&client, endpoint).await?;
    assert!(session.index() > 0);

    // test GetRemoteActor with unknown actor
    let error = client
        .send(command::GetRemoteActor {
            session: "server-session".into(),
            actor: (u64::MAX / 2).into(),
        })
        .await?
        .await?
        .unwrap_err();
    assert!(
        matches!(error, NodeError::RemoteActorNotFound(_)),
        "expected RemoteActorNotFound, got {error:?}"
    );

    // test GetRemoteActor with unknown session
    let error = client
        .send(command::GetRemoteActor {
            session: "nonexistent-session".to_string().into(),
            actor: "0".to_string().into(),
        })
        .await?
        .await?
        .unwrap_err();
    assert!(
        matches!(error, NodeError::SessionNotFound(_)),
        "expected SessionNotFound, got {error:?}"
    );

    // test CreateRemoteActor with failure
    let error = client
        .send(command::CreateRemoteActor {
            session: session.index().into(),
            label: "new".to_string(),
            r#type: "NonExistentType".to_string(),
            config: String::new(),
        })
        .await?
        .await?
        .unwrap_err();
    assert!(
        matches!(error, NodeError::CreateRemoteActorFailed(_)),
        "expected CreateRemoteActorFailed, got {error:?}"
    );

    session.do_send(acktor::Signal::Terminate).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

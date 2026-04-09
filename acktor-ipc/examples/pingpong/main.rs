use std::io::{self, IsTerminal};
use std::process;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tokio::{signal, sync::oneshot, time};
use tracing_subscriber::EnvFilter;

use acktor::{Actor, ActorContext, Signal};
use acktor_ipc::{Node, node};

#[cfg(not(any(feature = "websocket")))]
use acktor_ipc::ipc_method::pipe::PipeListener as Listener;
#[cfg(feature = "websocket")]
use acktor_ipc::ipc_method::websocket::WebSocketListener as Listener;

mod client;
mod message;
mod server;

#[cfg(not(any(feature = "websocket")))]
const ENDPOINT: &str = "pingpong";
#[cfg(feature = "websocket")]
const ENDPOINT: &str = "ws://localhost:12345/";

#[derive(Debug, Clone, ValueEnum)]
pub enum Mode {
    Server,
    Client,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(help = "Operation mode")]
    pub mode: Mode,
}

async fn server() -> Result<()> {
    #[cfg(not(any(feature = "websocket")))]
    let listener = Listener::new("pingpong").context("could not create pipe listener")?;
    #[cfg(feature = "websocket")]
    let listener = Listener::new("localhost:12345")
        .await
        .context("could not create WebSocket listener")?;

    let (address, join_handle) = Node::create(format!("node-{}", process::id()), |ctx| {
        let router = server::ServerRouter::new(ctx.address());
        let node = Node::new(Some(listener)).with_router(router);
        Ok(node)
    })?;

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = tx.send(());
    });

    let _ = rx.await;

    address.do_send(Signal::Terminate).await?;
    join_handle.await?;

    Ok(())
}

async fn client() -> Result<()> {
    let (address, join_handle) =
        Node::<Listener>::default().run(format!("node-{}", process::id()))?;

    let (client_address, client_join_handle) =
        client::Client::new(address.clone()).run("client")?;

    address
        .send(node::command::SetRouter(client::ClientRouter::new(
            client_address.clone(),
        )))
        .await?
        .await?;

    let (tx, mut rx) = oneshot::channel();

    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = tx.send(());
    });

    while address
        .send(node::command::Connect {
            endpoint: ENDPOINT.to_string(),
            session_label: "server-session".to_string(),
        })
        .await?
        .await?
        .is_err()
    {
        tokio::select! {
            _ = &mut rx => break,
            _ = time::sleep(Duration::from_secs(1)) => {}
        }
    }

    tokio::select! {
        _ = &mut rx => {}
        _ = time::sleep(Duration::from_secs(30)) => {}
    }

    address.do_send(Signal::Terminate).await?;
    join_handle.await?;

    client_address.do_send(Signal::Terminate).await?;
    client_join_handle.await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(io::stdout().is_terminal())
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    match args.mode {
        Mode::Server => server().await?,
        Mode::Client => client().await?,
    }

    Ok(())
}

use std::io::{self, IsTerminal};
use std::process;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::{Parser, ValueEnum};
use tokio::{signal, time};
use tracing_subscriber::EnvFilter;

use acktor::{Actor, ActorContext, Recipient, supervisor::SupervisionEvent};
use acktor_ipc::{Node, node::command};

#[cfg(feature = "pipe")]
use acktor_ipc::ipc_method::pipe::{PipeConnection as Connection, PipeListener as Listener};
#[cfg(not(feature = "pipe"))]
use acktor_ipc::ipc_method::websocket::{
    WebSocketConnection as Connection, WebSocketListener as Listener,
};

mod message;

mod client;
use client::Client;

mod server;
use server::Server;

#[cfg(feature = "pipe")]
const ENDPOINT: &str = "pingpong";
#[cfg(not(feature = "pipe"))]
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
    #[cfg(feature = "pipe")]
    let listener = Listener::new(ENDPOINT).context("could not create pipe listener")?;
    #[cfg(not(feature = "pipe"))]
    let listener = Listener::bind("localhost:12345")
        .await
        .context("could not create WebSocket listener")?;

    let (address, join_handle) = Node::new()
        .with_listener(listener)
        .with_actor_factory::<Server>()
        .run(format!("node-{}", process::id()))?;

    signal::ctrl_c().await?;

    acktor::utils::terminate_actor(address, join_handle).await;

    Ok(())
}

async fn client() -> Result<()> {
    let (node_address, node_join_handle) = Node::new().run(format!("node-{}", process::id()))?;

    let session_address = loop {
        if let Ok(session_address) = node_address
            .send(command::Connect::<Connection>::new(
                ENDPOINT.to_string(),
                Some("server-session".to_string()),
            ))
            .await?
            .await?
        {
            break session_address;
        }
        tokio::select! {
            _ = signal::ctrl_c() => {
                acktor::utils::terminate_actor(node_address, node_join_handle).await;
                return Ok(());
            }
            _ = time::sleep(Duration::from_secs(1)) => {}
        }
    };

    let (recipient, mut rx) = Recipient::<SupervisionEvent<Client>>::create(4);

    let (client_address, client_join_handle) = Client::create("ping", |ctx| {
        ctx.set_supervisor(Some(recipient));
        Ok(Client::new(session_address))
    })?;

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                acktor::utils::terminate_actor(client_address, client_join_handle).await;
                acktor::utils::terminate_actor(node_address, node_join_handle).await;

                break;
            }
            Ok(event) = rx.recv() => {
                if let SupervisionEvent::Terminated(_, _) = event {
                    acktor::utils::terminate_actor(node_address, node_join_handle).await;

                    break;
                }
            }
        }
    }

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

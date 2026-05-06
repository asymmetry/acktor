use std::process;
use std::time::Duration;

use tracing::{info, warn};

use acktor::{
    Actor, ActorContext, Address, ErrorReport, Handler,
    cron::{CronActor, CronContext},
    observer::Observer,
};
use acktor_ipc::{RemoteAddressable, Session, remote, session::command};

use crate::message::{Ping, Pong};
use crate::server::Server;

#[derive(Debug, RemoteAddressable)]
#[message(Pong)]
pub struct Client {
    session: Address<Session>,
    server: Option<Address<Server>>,
    id: u64,
}

impl Client {
    pub fn new(session: Address<Session>) -> Self {
        Self {
            session,
            server: None,
            id: 0,
        }
    }
}

#[remote]
impl Actor for Client {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;
}

impl CronActor for Client {
    async fn task(&mut self, ctx: &mut Self::Context) -> anyhow::Result<Duration> {
        if self.server.is_none() {
            let server = self
                .session
                .send(command::RemoteGetActor::new("pong".into()))
                .await?
                .await?;

            let address = match server {
                Ok(addr) => {
                    info!("Got server: {:?}", addr);
                    addr
                }
                Err(e) => {
                    info!("Could not get server: {}", e.report());
                    match self
                        .session
                        .send(command::RemoteCreateActor::new("pong", None))
                        .await?
                        .await?
                    {
                        Ok(addr) => {
                            info!("Created server: {:?}", addr);
                            addr
                        }
                        Err(e) => {
                            warn!("Could not create server: {}", e.report());
                            return Ok(Duration::from_secs(1));
                        }
                    }
                }
            };

            address
                .do_send(Observer::<Pong>::Register(ctx.address().into()))
                .await?;

            self.server = Some(address);
        }

        if let Some(server) = &self.server {
            let msg = Ping {
                id: self.id,
                timestamp: chrono::Utc::now().timestamp_micros(),
            };
            server.do_send(msg).await?;
            info!(
                "Process {} sent a Ping({}, {})",
                process::id(),
                msg.id,
                msg.timestamp,
            );
            self.id += 1;
        }

        Ok(Duration::from_secs(1))
    }
}

impl Handler<Pong> for Client {
    type Result = ();

    async fn handle(&mut self, msg: Pong, _ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Process {} received a Pong({}, {}), latency {} us",
            process::id(),
            msg.id,
            msg.timestamp,
            chrono::Utc::now().timestamp_micros() - msg.timestamp,
        );
    }
}

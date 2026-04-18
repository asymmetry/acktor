use std::process;
use std::time::Duration;

use tracing::{info, warn};

use acktor::{
    Actor, ActorContext, Address, ErrorReport, Handler, Sender,
    cron::{CronActor, CronContext},
    observer::Observer,
};
use acktor_ipc::{
    ActorHandle, Decode, Encode, RemoteActor, RemoteAddress, RemoteMessage, RemoteMessageKind,
    Session, remote, session::command,
};

use crate::message::{Ping, Pong};

#[derive(Debug, RemoteActor)]
pub struct Client {
    session: Address<Session>,
    server: Option<RemoteAddress>,
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
            let get_result = self
                .session
                .send(command::GetRemoteActor {
                    actor: ActorHandle::from("pong"),
                })
                .await?
                .await?;

            let address = match get_result {
                Ok(addr) => {
                    info!("Got remote actor: {:?}", addr);

                    addr
                }
                Err(e) => {
                    info!("Could not get remote actor: {}", e.report());

                    match self
                        .session
                        .send(command::CreateRemoteActor {
                            label: "pong".to_string(),
                            r#type: "Server".to_string(),
                            config: String::new(),
                        })
                        .await?
                        .await?
                    {
                        Ok(addr) => addr,
                        Err(e) => {
                            warn!("Could not create remote actor: {}", e.report());

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
            server.do_send(Ping { id: self.id }).await?;
            self.id += 1;
        }

        Ok(Duration::from_secs(1))
    }
}

impl Handler<Pong> for Client {
    type Result = ();

    async fn handle(&mut self, msg: Pong, _ctx: &mut Self::Context) -> Self::Result {
        info!("Process {} received Pong({})", process::id(), msg.id);
    }
}

impl Handler<RemoteMessage> for Client {
    type Result = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn handle(&mut self, msg: RemoteMessage, ctx: &mut Self::Context) -> Self::Result {
        let RemoteMessage {
            message,
            kind,
            decode_context,
            ..
        } = msg;

        if let Ok(pong) = Pong::decode(message, decode_context.as_ref()) {
            let result = <Self as Handler<Pong>>::handle(self, pong, ctx).await;

            if let RemoteMessageKind::Send(tx) = kind {
                if let Ok(bytes) = result.encode_to_bytes(None) {
                    let _ = tx.send(bytes);
                }
            }
        }

        Ok(())
    }
}

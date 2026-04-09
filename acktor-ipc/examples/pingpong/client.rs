use std::pin::Pin;
use std::process;
use std::time::Duration;

use futures_util::FutureExt;
use tracing::{info, warn};

use acktor::{
    Actor, ActorContext, Address, Handler, Sender,
    cron::{CronActor, CronContext},
    macros::report,
    observer::Observer,
};
use acktor_ipc::{
    Decode, Encode, IpcRouter, Node, RemoteAddress, bytes::Bytes, errors::RouterError, node,
};

#[cfg(not(any(feature = "websocket")))]
use acktor_ipc::ipc_method::pipe::PipeListener as Listener;
#[cfg(feature = "websocket")]
use acktor_ipc::ipc_method::websocket::WebSocketListener as Listener;

use crate::message::{Ping, Pong};

type Result<T> = std::result::Result<T, RouterError>;

#[derive(Debug)]
pub struct Client {
    node: Address<Node<Listener>>,
    server: Option<RemoteAddress>,
}

impl Client {
    pub fn new(node: Address<Node<Listener>>) -> Self {
        Self { node, server: None }
    }
}

impl Actor for Client {
    type Context = CronContext<Self>;
    type Error = anyhow::Error;
}

impl CronActor for Client {
    async fn task(&mut self, ctx: &mut Self::Context) -> anyhow::Result<Duration> {
        if let Some(server) = &self.server {
            if server.do_send(Ping).await.is_err() {
                self.server = None;
            }
        } else {
            let address = if let Ok(address) = self
                .node
                .send(node::command::GetRemoteActor {
                    session_label: "server-session".to_string(),
                    actor_label: "ping".to_string(),
                })
                .await?
                .await?
            {
                address
            } else {
                match self
                    .node
                    .send(node::command::CreateRemoteActor {
                        session_label: "server-session".to_string(),
                        actor_label: "ping".to_string(),
                        r#type: "Server".to_string(),
                        config: "".to_string(),
                    })
                    .await?
                    .await?
                {
                    Ok(address) => address,
                    Err(e) => {
                        warn!("Could not create remote actor: {}", report!(e));
                        return Ok(Duration::from_secs(1));
                    }
                }
            };

            address
                .do_send(Observer::<Pong>::Register(ctx.address().into()))
                .await?;
            self.server = Some(address)
        }

        Ok(Duration::from_secs(1))
    }
}

impl Handler<Pong> for Client {
    type Result = ();

    async fn handle(&mut self, _msg: Pong, _ctx: &mut Self::Context) -> Self::Result {
        info!("Process {} received a Pong", process::id());
    }
}

#[derive(Debug, Clone)]
pub struct ClientRouter {
    client: Address<Client>,
}

impl ClientRouter {
    pub fn new(client: Address<Client>) -> Self {
        Self { client }
    }
}

impl IpcRouter for ClientRouter {
    fn create_actor<'a>(
        &'a self,
        _label: &'a str,
        _type: &'a str,
        _config: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        async move { Err(RouterError::ActorCreationNotSupported) }.boxed()
    }

    fn get_actor(&self, _label: &str) -> Result<usize> {
        Ok(self.client.index())
    }

    fn can_forward(&self, actor_id: usize) -> bool {
        actor_id == self.client.index()
    }

    fn send<'a>(
        &'a self,
        actor_id: usize,
        message: &'a [u8],
        _session: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        async move {
            if !self.can_forward(actor_id) {
                return Err(RouterError::UnknownActorIndex(actor_id));
            }

            let msg = Pong::decode(Bytes::copy_from_slice(message), None)?;
            self.client.send(msg).await?.await?;

            let mut buffer = vec![0; ().buffer_size()];
            let _ = ().encode(buffer.as_mut_slice());

            Ok(buffer)
        }
        .boxed()
    }

    fn do_send<'a>(
        &'a self,
        actor_id: usize,
        message: &'a [u8],
        _session: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        async move {
            if !self.can_forward(actor_id) {
                return Err(RouterError::UnknownActorIndex(actor_id));
            }

            let msg = Pong::decode(Bytes::copy_from_slice(message), None)?;
            self.client.do_send(msg).await?;

            Ok(())
        }
        .boxed()
    }
}

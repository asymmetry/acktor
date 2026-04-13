use std::pin::Pin;
use std::process;
use std::thread;

use bytes::Bytes;
use futures_util::FutureExt;
use once_cell::sync::OnceCell;
use prost::Message as ProstMessage;
use tracing::info;

use acktor::{
    Actor, Address, Context, Handler,
    observer::{Observer, ObserverSet, SubjectActor},
};
use acktor_ipc::{
    Decode, Encode, Node, node,
    proto::control_message::{self, ControlMessage, ControlMessageType},
};

#[cfg(not(any(feature = "websocket")))]
use acktor_ipc::ipc_method::pipe::PipeListener as Listener;
#[cfg(feature = "websocket")]
use acktor_ipc::ipc_method::websocket::WebSocketListener as Listener;

use crate::message::{Ping, Pong};

type Result<T> = std::result::Result<T, RouterError>;

static SERVER: OnceCell<Address<Server>> = OnceCell::new();
static LABEL: OnceCell<String> = OnceCell::new();

#[derive(Debug, Default)]
pub struct Server {
    observers: ObserverSet<Pong>,
}

impl Actor for Server {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl SubjectActor<Pong> for Server {
    fn observers_mut(&mut self) -> &mut ObserverSet<Pong> {
        &mut self.observers
    }
}

impl Handler<Ping> for Server {
    type Result = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) -> Self::Result {
        info!("Process {} received a Ping", process::id());
        self.notify_observers(Pong).await;
    }
}

#[derive(Debug, Clone)]
pub struct ServerRouter {
    node: Address<Node<Listener>>,
}

impl ServerRouter {
    pub fn new(node: Address<Node<Listener>>) -> Self {
        Self { node }
    }
}

impl IpcRouter for ServerRouter {
    fn create_actor<'a>(
        &'a self,
        label: &'a str,
        r#type: &'a str,
        _config: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        async move {
            if r#type != "Server" {
                return Err(RouterError::custom(format!("{type} is not supported")));
            }

            if SERVER.get().is_none() {
                // this is a workaround so that the Server will use a new root Span
                let label_cloned = label.to_string();
                let runtime = tokio::runtime::Handle::current();
                let join_handle = thread::spawn(move || {
                    let _enterd = runtime.enter();
                    Server::default().run(label_cloned)
                });
                let (address, _) = join_handle
                    .join()
                    .unwrap()
                    .map_err(|e| RouterError::Custom(e.into()))?;

                let index = address.index();
                SERVER.set(address).unwrap();
                LABEL.set(label.to_string()).unwrap();

                Ok(index)
            } else {
                Err(RouterError::custom("server actor already exists"))
            }
        }
        .boxed()
    }

    fn get_actor(&self, label: &str) -> Result<usize> {
        if let Some(server) = SERVER.get() {
            if label == LABEL.get().unwrap() {
                return Ok(server.index());
            }
        }

        Err(RouterError::ActorNotExist(label.to_string()))
    }

    fn can_forward(&self, actor_id: usize) -> bool {
        if let Some(server) = SERVER.get() {
            return actor_id == server.index();
        }

        false
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

            let server = SERVER.get().unwrap();

            let msg = Ping::decode(Bytes::copy_from_slice(message), None)?;
            server.send(msg).await?.await?;

            let mut buffer = vec![0; ().buffer_size()];
            let _ = Encode::encode(&(), buffer.as_mut_slice());

            Ok(buffer)
        }
        .boxed()
    }

    fn do_send<'a>(
        &'a self,
        actor_id: usize,
        message: &'a [u8],
        session: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        async move {
            if !self.can_forward(actor_id) {
                return Err(RouterError::UnknownActorIndex(actor_id));
            }

            let server = SERVER.get().unwrap();

            let control_msg = ControlMessage::decode_length_delimited(message);

            if let Ok(control_msg) = control_msg
                && control_msg.signature == control_message::SIGNATURE
            {
                if let Some(ControlMessageType::Observer(observer)) = control_msg.message {
                    match observer.observer {
                        Some(observer::ObserverType::Register(actor_id)) => {
                            let remote_address = self
                                .node
                                .send(node::command::CreateRemoteAddress {
                                    session_label: session.to_string(),
                                    actor_index: actor_id as usize,
                                })
                                .await?
                                .await?
                                .map_err(|e| RouterError::Custom(e.into()))?;

                            server
                                .do_send(Observer::Register(remote_address.into()))
                                .await?;
                        }

                        Some(observer::ObserverType::Unregister(actor_id)) => {
                            let remote_address = self
                                .node
                                .send(node::command::CreateRemoteAddress {
                                    session_label: session.to_string(),
                                    actor_index: actor_id as usize,
                                })
                                .await?
                                .await?
                                .map_err(|e| RouterError::Custom(e.into()))?;

                            server
                                .do_send(Observer::Unregister(remote_address.into()))
                                .await?;
                        }

                        _ => {}
                    }
                }
            } else {
                let msg = Ping::decode(Bytes::copy_from_slice(message), None)?;
                server.do_send(msg).await?;
            }

            Ok(())
        }
        .boxed()
    }
}

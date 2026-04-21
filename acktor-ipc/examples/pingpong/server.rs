use std::process;

use tracing::info;

use acktor::{
    Actor, Address, Context, Handler, JoinHandle,
    observer::{Observer, ObserverSet, SubjectActor},
};
use acktor_ipc::{RemoteActor, RemoteActorFactory, remote};

use crate::message::{Ping, Pong};

#[derive(Debug, Default, RemoteActor)]
#[message(Ping, Observer<Pong>)]
pub struct Server {
    observers: ObserverSet<Pong>,
}

#[remote]
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

    async fn handle(&mut self, msg: Ping, _ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Process {} received a Ping({}, {})",
            process::id(),
            msg.id,
            msg.timestamp,
        );

        self.notify_observers(Pong {
            id: msg.id,
            timestamp: msg.timestamp,
        })
        .await;
    }
}

impl RemoteActorFactory for Server {
    const TYPE_NAME: &'static str = "Server";

    fn create_remote(
        label: String,
        _config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), Self::Error> {
        let (address, join_handle) = Server::default().run(label)?;
        Ok((address, join_handle))
    }
}

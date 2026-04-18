use std::process;

use tracing::info;

use acktor::{
    Actor, Address, Context, Handler, JoinHandle,
    observer::{Observer, ObserverSet, SubjectActor},
};
use acktor_ipc::{
    Decode, Encode, RemoteActor, RemoteActorFactory, RemoteMessage, RemoteMessageKind, remote,
};

use crate::message::{Ping, Pong};

#[derive(Debug, Default, RemoteActor)]
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

impl Handler<RemoteMessage> for Server {
    type Result = ();

    async fn handle(&mut self, msg: RemoteMessage, ctx: &mut Self::Context) -> Self::Result {
        let RemoteMessage {
            message,
            kind,
            decode_context,
            ..
        } = msg;

        let encode_context = decode_context
            .as_ref()
            .map(|ctx| ctx.create_encode_context());

        if let Ok(observer) = Observer::<Pong>::decode(message.clone(), decode_context.as_ref()) {
            self.handle(observer, ctx).await;
        } else if let Ok(ping) = Ping::decode(message, decode_context.as_ref()) {
            self.handle(ping, ctx).await;
        }

        if let RemoteMessageKind::Send(tx) = kind {
            let result = ().encode_to_bytes(encode_context.as_ref());
            let _ = tx.send(result.map_err(Into::into));
        }
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

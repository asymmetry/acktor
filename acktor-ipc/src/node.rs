//! Node actor for managing IPC connections and sessions.

use std::fmt::{self, Debug};
use std::sync::Arc;

use ahash::HashMap;
use futures_util::future::join_all;
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{error, info, warn};

use acktor::{
    Actor, ActorContext, Address, Handler, Recipient, SenderIndex, Signal,
    macros::{debug_trace, report},
    message::FutureMessageResult,
    observer::{ObserverSet, SubjectActor},
    supervisor::SupervisionEvent,
    utils,
};

use crate::errors::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote_address::RemoteAddress;
use crate::remote_message::RemoteMessage;
use crate::session::{self, Session};

pub mod command;

mod event;
pub use event::NodeEvent;

mod context;
use context::NodeContext;

type Result<T> = std::result::Result<T, NodeError>;

pub(crate) type LocalActors = HashMap<u64, Recipient<RemoteMessage>>;

/// An actor which helps to manage the IPC connections.
///
/// The node can hold multiple [`IpcListener`]s to accept incoming IPC connections on several
/// endpoints in parallel. Outbound connections are initiated by sending a
/// [`Connect<C>`][command::Connect] command.
#[derive(Default)]
pub struct Node {
    listeners: Vec<Box<dyn IpcListener>>,
    local_actors: LocalActors,
    sessions: HashMap<String, Address<Session>>,
    session_join_handles: HashMap<Recipient<Signal>, JoinHandle<()>>,
    observers: ObserverSet<NodeEvent>,
}

impl Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoints: Vec<&str> = self.listeners.iter().map(|l| l.local_endpoint()).collect();
        f.debug_struct("Node")
            .field("listeners", &endpoints)
            .field("local_actors", &self.local_actors)
            .field("sessions", &self.sessions)
            // .field("session_join_handles", &self.session_join_handles)
            .field(
                "observers",
                &format_args!("ObserverSet({})", self.observers.len()),
            )
            .finish()
    }
}

impl Node {
    /// Constructs a new [`Node`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an IPC listener to the node.
    pub fn with_listener<L>(mut self, listener: L) -> Self
    where
        L: IpcListener,
    {
        self.listeners.push(Box::new(listener));
        self
    }

    /// Adds a local actor to the node.
    pub fn with_actor<A>(mut self, actor: Address<A>) -> Self
    where
        A: Actor + Handler<RemoteMessage>,
    {
        self.local_actors.insert(actor.index(), actor.into());
        self
    }

    pub(crate) fn listeners(&self) -> &[Box<dyn IpcListener>] {
        &self.listeners
    }

    async fn create_session(
        &mut self,
        connection: Box<dyn IpcConnection>,
        session_label: Option<String>,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let endpoint = connection.peer_endpoint().to_string();

        let (address, join_handle) = Session::create(endpoint.clone(), |child_ctx| {
            child_ctx.set_supervisor(Some(ctx.address().into()));
            Ok(Session::new(connection, self.local_actors.clone()))
        })
        .map_err(NodeError::CreateSessionFailed)?;

        let session_id = address.index();

        self.sessions.insert(endpoint.clone(), address.clone());
        if let Some(label) = session_label {
            self.sessions.insert(label, address.clone());
        }
        self.session_join_handles
            .insert(address.into(), join_handle);

        self.notify_observers(NodeEvent::SessionCreated(session_id, endpoint))
            .await;

        Ok(())
    }
}

impl Actor for Node {
    type Context = NodeContext;
    type Error = NodeError;

    async fn post_start(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        info!("Node is ready");

        Ok(())
    }

    async fn post_stop(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        join_all(
            self.session_join_handles
                .drain()
                .map(|(address, join_handle)| utils::terminate_actor(address, join_handle)),
        )
        .await;

        info!("Node is stopped");

        Ok(())
    }
}

impl SubjectActor<NodeEvent> for Node {
    fn observers_mut(&mut self) -> &mut ObserverSet<NodeEvent> {
        &mut self.observers
    }
}

impl<L> Handler<command::AddListener<L>> for Node
where
    L: IpcListener,
{
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::AddListener<L>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        self.listeners.push(Box::new(msg.0));
    }
}

impl Handler<command::RemoveListener> for Node {
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::RemoveListener,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let endpoint = msg.0;
        self.listeners.retain(|l| l.local_endpoint() != endpoint);
    }
}

impl Handler<command::AddActor> for Node {
    type Result = ();

    async fn handle(&mut self, msg: command::AddActor, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let recipient = msg.0;
        let actor_index = recipient.index();
        self.local_actors.insert(actor_index, recipient.clone());

        for session in self.sessions.values() {
            let _ = session
                .do_send(session::command::AddActor(recipient.clone()))
                .await;
        }
    }
}

impl Handler<command::RemoveActor> for Node {
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::RemoveActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        self.local_actors.remove(&msg.0);

        for session in self.sessions.values() {
            let _ = session.do_send(session::command::RemoveActor(msg.0)).await;
        }
    }
}

impl<T> Handler<command::Connect<T>> for Node
where
    T: IpcConnection,
{
    type Result = Result<()>;

    async fn handle(&mut self, msg: command::Connect<T>, ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::Connect {
            endpoint,
            session_label,
            ..
        } = msg;

        let connection = T::connect(&endpoint).await?;
        let connection: Box<dyn IpcConnection> = Box::new(connection);
        self.create_session(connection, session_label, ctx).await?;

        Ok(())
    }
}

impl Handler<command::CreateRemoteActor> for Node {
    type Result = FutureMessageResult<command::CreateRemoteActor>;

    async fn handle(
        &mut self,
        msg: command::CreateRemoteActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::CreateRemoteActor {
            session_label,
            label,
            r#type,
            config,
        } = msg;

        let result: Result<_> = async {
            let session = self
                .sessions
                .get(&session_label)
                .ok_or(NodeError::SessionNotFound(session_label))?;

            let (tx, rx) = oneshot::channel();

            session
                .do_send(session::command::CreateRemoteActor {
                    label,
                    r#type,
                    config,
                    tx,
                })
                .await?;

            Ok(rx)
        }
        .await;

        FutureMessageResult::new(async move {
            result?.await?.map_err(NodeError::CreateRemoteActorFailed)
        })
    }
}

impl Handler<command::GetRemoteActor> for Node {
    type Result = FutureMessageResult<command::GetRemoteActor>;

    async fn handle(
        &mut self,
        msg: command::GetRemoteActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::GetRemoteActor {
            session_label,
            label,
        } = msg;

        let result: Result<_> = async {
            let session = self
                .sessions
                .get(&session_label)
                .ok_or(NodeError::SessionNotFound(session_label))?;

            let (tx, rx) = oneshot::channel();

            session
                .do_send(session::command::GetRemoteActor { label, tx })
                .await?;

            Ok(rx)
        }
        .await;

        FutureMessageResult::new(
            async move { result?.await?.map_err(NodeError::RemoteActorNotFound) },
        )
    }
}

impl Handler<command::CreateRemoteAddress> for Node {
    type Result = Result<RemoteAddress>;

    async fn handle(
        &mut self,
        msg: command::CreateRemoteAddress,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::CreateRemoteAddress {
            session_label,
            actor_index,
        } = msg;

        let session = self
            .sessions
            .get(&session_label)
            .ok_or(NodeError::SessionNotFound(session_label))?;

        Ok(RemoteAddress::new(actor_index, Arc::new(session.clone())))
    }
}

impl Handler<SupervisionEvent<Session>> for Node {
    type Result = ();

    async fn handle(
        &mut self,
        msg: SupervisionEvent<Session>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle supervision event {:?}", msg);

        match msg {
            SupervisionEvent::Warn(actor, e) => {
                warn!("Session {} error: {}", actor.index(), report!(e));
            }
            SupervisionEvent::Terminated(actor, e) => {
                let session_id = actor.index();

                if let Some(e) = e {
                    error!(
                        "Session {} is stopped with error: {}",
                        actor.index(),
                        report!(e)
                    );
                }

                self.sessions.retain(|_, v| v != &actor);
                self.session_join_handles.remove(&actor.into());

                self.notify_observers(NodeEvent::SessionDeleted(session_id))
                    .await;
            }
            _ => {}
        }
    }
}

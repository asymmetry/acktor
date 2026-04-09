//! Node actor for managing IPC connections and sessions.

use std::fmt::{self, Debug};
use std::sync::Arc;

use futures_util::future::join_all;
use rustc_hash::FxHashMap as HashMap;
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

pub(crate) type LocalActors = HashMap<usize, Recipient<RemoteMessage>>;

/// An actor which helps to manage the IPC connections.
///
/// The node requires an [`IpcListener`] to handle IPC connections.
pub struct Node<L>
where
    L: IpcListener,
{
    listener: Option<L>,
    local_actors: LocalActors,
    sessions: HashMap<String, Address<Session<L::Connection>>>,
    session_join_handles: HashMap<Recipient<Signal>, JoinHandle<()>>,
    observers: ObserverSet<NodeEvent>,
}

impl<L> Debug for Node<L>
where
    L: IpcListener,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field(
                "listener",
                &format_args!(
                    "{}",
                    match &self.listener {
                        Some(_) => utils::type_name::<L>()?,
                        None => "None",
                    }
                ),
            )
            .field("sessions", &self.sessions)
            .field(
                "observers",
                &format_args!("ObserverSet({})", self.observers.len()),
            )
            .finish()
    }
}

impl<L> Default for Node<L>
where
    L: IpcListener,
{
    fn default() -> Self {
        Self {
            listener: None,
            sessions: HashMap::default(),
            session_join_handles: HashMap::default(),
            observers: ObserverSet::default(),
            local_actors: HashMap::default(),
        }
    }
}

impl<L> Node<L>
where
    L: IpcListener,
{
    /// Constructs a new [`Node`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the IPC listener of the node.
    pub fn with_listener(mut self, listener: L) -> Self {
        self.listener = Some(listener);
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

    async fn create_session(
        &mut self,
        connection: L::Connection,
        session_label: Option<String>,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<()> {
        let endpoint = connection.endpoint().to_string();

        let (address, join_handle) = Session::create(endpoint.clone(), |child_ctx| {
            child_ctx.set_supervisor(Some(ctx.address().into()));
            Ok(Session::new(connection, self.local_actors.clone()))
        })
        .map_err(NodeError::CreateSessionFailed)?;

        let session_id = address.index();

        self.sessions.insert(endpoint, address.clone());
        if let Some(label) = session_label {
            self.sessions.insert(label, address.clone());
        }
        self.session_join_handles
            .insert(address.into(), join_handle);

        self.notify_observers(NodeEvent::SessionCreated(session_id))
            .await;

        Ok(())
    }
}

impl<L> Actor for Node<L>
where
    L: IpcListener,
{
    type Context = NodeContext<L>;
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

impl<L> SubjectActor<NodeEvent> for Node<L>
where
    L: IpcListener,
{
    fn observers_mut(&mut self) -> &mut ObserverSet<NodeEvent> {
        &mut self.observers
    }
}

impl<L> Handler<command::SetListener<L>> for Node<L>
where
    L: IpcListener,
{
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::SetListener<L>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!(
            "Handle command SetListener<{}>",
            utils::type_name::<L>().unwrap_or("L")
        );

        self.listener = Some(msg.0);
    }
}

impl<L> Handler<command::AddActor> for Node<L>
where
    L: IpcListener,
{
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

impl<L> Handler<command::RemoveActor> for Node<L>
where
    L: IpcListener,
{
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

impl<L> Handler<command::Connect> for Node<L>
where
    L: IpcListener,
{
    type Result = Result<()>;

    async fn handle(&mut self, msg: command::Connect, ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::Connect {
            endpoint,
            session_label,
        } = msg;

        let connection = L::Connection::connect(&endpoint).await?;
        self.create_session(connection, session_label, ctx).await?;

        Ok(())
    }
}

impl<L> Handler<command::CreateRemoteActor> for Node<L>
where
    L: IpcListener,
{
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

impl<L> Handler<command::GetRemoteActor> for Node<L>
where
    L: IpcListener,
{
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

impl<L> Handler<command::CreateRemoteAddress> for Node<L>
where
    L: IpcListener,
{
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

impl<L, C> Handler<SupervisionEvent<Session<C>>> for Node<L>
where
    L: IpcListener<Connection = C>,
    C: IpcConnection,
{
    type Result = ();

    async fn handle(
        &mut self,
        msg: SupervisionEvent<Session<C>>,
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

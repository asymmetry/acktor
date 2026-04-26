//! Node actor for managing IPC connections and sessions.
//!

use std::marker::PhantomData;
use std::sync::Arc;

use ahash::{HashMap, HashSet};
use dashmap::DashMap;
use futures_util::future::join_all;
use tracing::{error, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, JoinHandle, Recipient, Signal,
    message::FutureMessageResult,
    observer::{ObserverSet, SubjectActor},
    supervisor::SupervisionEvent,
    utils::{debug_trace, terminate_actor},
};

use crate::double_map::DoubleMap;
use crate::errors::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote_actor::{
    RemoteActor, RemoteActorFactory, RemoteActorFactoryRegistry, RemoteActorRegistry,
    RemoteActorShim,
};
use crate::session::{self, Session, SessionHandle};

pub mod command;

mod event;
pub use event::NodeEvent;

mod context;
use context::NodeContext;

pub(crate) mod factory;
use factory::Factory;

type Result<T> = std::result::Result<T, NodeError>;

pub(crate) type LabelMap = Arc<DashMap<String, ActorId, ahash::RandomState>>;

/// An actor which helps to manage the IPC connections.
///
/// The node can hold multiple [`IpcListener`]s to accept incoming IPC connections on several
/// endpoints in parallel. Outbound connections are initiated by sending a
/// [`Connect<C>`][command::Connect] command.
#[derive(Default)]
pub struct Node {
    listener_labels: HashSet<String>,
    listeners: Vec<Box<dyn IpcListener>>,
    // registered factories for peer-initiated actor creation, keyed by the type name.
    factory_registry: Option<RemoteActorFactoryRegistry>,
    factory: Option<Address<Factory>>,
    // `registry` and `label_map`  are cloned into the factory and every session, so all holders
    // observe the same contents. They are wrapped in `Arc` so clone is cheap.
    registry: RemoteActorRegistry,
    label_map: LabelMap,
    sessions: DoubleMap<ActorId, String, Address<Session>>,
    children: HashMap<Recipient<Signal>, JoinHandle<()>>,
    observers: ObserverSet<NodeEvent>,
}

impl Node {
    /// Constructs a new [`Node`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an IPC listener to the node.
    ///
    /// If the node already has a listener listening on the same endpoint, the new listener will
    /// replace the existing one. Note this is not the same as the
    /// [`AddListener`][command::AddListener] command, which will not replace the existing
    /// listener.
    pub fn with_listener<L>(mut self, listener: L) -> Self
    where
        L: IpcListener,
    {
        if self.listener_labels.contains(listener.local_endpoint()) {
            self.listeners
                .retain(|l| l.local_endpoint() != listener.local_endpoint());
        } else {
            self.listener_labels
                .insert(listener.local_endpoint().to_string());
        }
        self.listeners.push(Box::new(listener));
        self
    }

    /// Adds an actor which implements [`RemoteActor`] trait to the node, registering it under
    /// `label` so that remote peers can look it up by name.
    ///
    /// Duplicate labels and duplicate actor ids are silently skipped.
    pub fn with_actor<A>(self, label: String, actor: Address<A>) -> Self
    where
        A: RemoteActor,
    {
        let actor_id = actor.index();
        if !self.label_map.contains_key(&label) && self.registry.insert(actor) {
            self.label_map.insert(label, actor_id);
        }
        self
    }

    /// Adds an actor factory so that remote peers can create instances of `A` by sending a
    /// `CreateActor` node command to this node.
    pub fn with_actor_factory<A>(mut self) -> Self
    where
        A: RemoteActorFactory,
    {
        self.factory_registry.get_or_insert_default().insert(
            A::TYPE_NAME.to_string(),
            Arc::new(RemoteActorShim::<A>(PhantomData)),
        );
        self
    }

    async fn create_session(
        &mut self,
        connection: Box<dyn IpcConnection>,
        session_label: Option<String>,
        ctx: &mut <Self as Actor>::Context,
    ) -> Result<Address<Session>> {
        let endpoint = connection.peer_endpoint().to_string();

        let session_label = session_label.unwrap_or_else(|| endpoint.clone());

        if self.sessions.contains_key2(&session_label) {
            return Err(NodeError::CreateSessionFailed(
                format!("session with label '{}' already exists", session_label).into(),
            ));
        }

        let (address, join_handle) = Session::create(endpoint.clone(), |child_ctx| {
            child_ctx.set_supervisor(Some(ctx.address().into()));

            // SAFETY: `self.factory` is assigned at the end of `post_start`, and `create_session`
            // is only reachable from message handlers, which the actor runtime does not dispatch
            // until `post_start` has returned `Ok`. The unwrap is therefore infallible.
            Ok(Session::new(
                connection,
                self.factory.clone().unwrap(),
                self.registry.clone(),
                self.label_map.clone(),
            ))
        })
        .map_err(|e| NodeError::CreateSessionFailed(e.into()))?;

        let session_id = address.index();

        // this will never fail since we have verified the session label is unique and the actor id
        // is also unique in the same process
        let _ = self
            .sessions
            .insert(session_id, session_label.clone(), address.clone());

        self.children.insert(address.clone().into(), join_handle);

        self.notify_observers(NodeEvent::SessionCreated(address.clone(), session_label))
            .await;

        Ok(address)
    }

    fn get_session(&self, session_ref: &SessionHandle) -> Result<Address<Session>> {
        match session_ref {
            SessionHandle::Address(address) => self
                .sessions
                .get_by_key1(&address.index())
                .cloned()
                .ok_or_else(|| NodeError::SessionNotFound(address.index().to_string())),
            SessionHandle::Index(index) => self
                .sessions
                .get_by_key1(index)
                .cloned()
                .ok_or_else(|| NodeError::SessionNotFound(index.to_string())),
            SessionHandle::Label(label) => self
                .sessions
                .get_by_key2(label)
                .cloned()
                .ok_or_else(|| NodeError::SessionNotFound(label.clone())),
        }
    }
}

impl Actor for Node {
    type Context = NodeContext;
    type Error = NodeError;

    async fn post_start(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        let factory_registry = self.factory_registry.take().unwrap_or_default();

        // the factory actor never fail, so it is not supervised
        let (address, join_handle) = Factory::new(
            factory_registry,
            self.registry.clone(),
            self.label_map.clone(),
        )
        .run("factory")?;

        self.children.insert(address.clone().into(), join_handle);
        self.factory = Some(address);

        info!("Node is ready");

        Ok(())
    }

    async fn post_stop(&mut self, _ctx: &mut Self::Context) -> Result<()> {
        join_all(
            self.children
                .drain()
                .map(|(address, join_handle)| terminate_actor(address, join_handle)),
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
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::AddListener<L>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg,);

        let label = msg.0.local_endpoint();
        if self.listener_labels.contains(label) {
            false
        } else {
            self.listener_labels.insert(label.to_string());
            self.listeners.push(Box::new(msg.0));
            true
        }
    }
}

impl Handler<command::RemoveListener> for Node {
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::RemoveListener,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let label = msg.0;
        if self.listener_labels.remove(&label) {
            ctx.abort_accept_task(&label);
            true
        } else {
            false
        }
    }
}

impl<A> Handler<command::AddActor<A>> for Node
where
    A: RemoteActor,
{
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::AddActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::AddActor { label, address } = msg;

        if self.label_map.contains_key(&label) {
            return false;
        }

        let actor_id = address.index();
        if !self.registry.insert(address) {
            return false;
        }
        self.label_map.insert(label, actor_id);

        true
    }
}

impl Handler<command::RemoveActor> for Node {
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::RemoveActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let actor_id = msg.0;
        self.label_map.retain(|_, id| *id != actor_id);
        self.registry.remove(actor_id).is_some()
    }
}

impl<T> Handler<command::Connect<T>> for Node
where
    T: IpcConnection,
{
    type Result = Result<Address<Session>>;

    async fn handle(&mut self, msg: command::Connect<T>, ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let command::Connect {
            endpoint,
            session_label,
            ..
        } = msg;

        let connection = T::connect(&endpoint).await?;
        let connection: Box<dyn IpcConnection> = Box::new(connection);
        let address = self.create_session(connection, session_label, ctx).await?;

        Ok(address)
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
            session,
            label,
            r#type,
            config,
        } = msg;

        let session = self.get_session(&session);

        FutureMessageResult::new(async move {
            session?
                .send(session::command::CreateRemoteActor {
                    label,
                    r#type,
                    config,
                })
                .await?
                // this await is time consuming since it involves IPC
                .await?
                .map_err(NodeError::CreateRemoteActorFailed)
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

        let command::GetRemoteActor { session, actor } = msg;

        let session = self.get_session(&session);

        FutureMessageResult::new(async move {
            session?
                .send(session::command::GetRemoteActor { actor })
                .await?
                // this await is time consuming since it involves IPC
                .await?
                .map_err(NodeError::RemoteActorNotFound)
        })
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
                warn!("Session {} error: {}", actor.index(), e.report());
            }
            SupervisionEvent::Terminated(session, e) => {
                if let Some(e) = e {
                    error!(
                        "Session {} is stopped with error: {}",
                        session.index(),
                        e.report()
                    );
                }

                self.sessions.retain(|_, _, v| v != &session);
                self.children.remove(&session.clone().into());

                self.notify_observers(NodeEvent::SessionDeleted(session))
                    .await;
            }
            _ => {}
        }
    }
}

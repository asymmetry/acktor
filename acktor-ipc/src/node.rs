//! Node actor for managing IPC connections and sessions.
//!

use std::marker::PhantomData;
use std::sync::Arc;

use ahash::{HashMap, HashSet};
use futures_util::future::join_all;
use tracing::{error, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, JoinHandle, Recipient, Signal,
    message::FutureMessageResult,
    observer::{ObserverSet, SubjectActor},
    supervisor::SupervisionEvent,
    utils::{ShortName, debug_trace, terminate_actor},
};

use crate::double_map::DoubleMap;
use crate::error::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};
use crate::remote::{
    RemoteAddressable, RemoteFactoryRegistry, RemoteFactoryShim, RemoteMailboxRegistry,
    RemoteSpawnable,
};
use crate::session::{self, Session, SessionRef};

pub mod command;

mod event;
pub use event::NodeEvent;

mod context;
use context::NodeContext;

pub(crate) mod actor_mgr;
use actor_mgr::{ActorLabelMap, ActorMgr};

type Result<T> = std::result::Result<T, NodeError>;

/// An actor which helps to manage the IPC connections.
///
/// The node can hold multiple [`IpcListener`]s to accept incoming IPC connections on several
/// endpoints in parallel. Outbound connections are initiated by sending a
/// [`Connect<C>`][command::Connect] command.
pub struct Node {
    listener_labels: HashSet<String>,
    listeners: Vec<Box<dyn IpcListener>>,
    /// Registers remote addressable actors, the key is the actor's index (local part only, not
    /// the stable type id), the value is the actor's `RemoteMailbox`.
    registry: RemoteMailboxRegistry,
    /// Actor manager, which handles the creation of remote spawnable actors.
    actor_mgr: Option<Address<ActorMgr>>,
    //
    sessions: DoubleMap<ActorId, String, Address<Session>>,
    children: HashMap<Recipient<Signal>, JoinHandle<()>>,
    observers: ObserverSet<NodeEvent>,
    /// Registers remote spawnable actor types, the key is the actor's stable type id (as a
    /// `u64`), the value is a `RemoteFactory` trait object.
    ///
    /// The `ActorMgr` actor will take the ownership of this registry in `post_start` and leave a
    /// `None` here.
    _factories: Option<RemoteFactoryRegistry>,
    /// Maps actor labels to actor ids for remote addressable actors, so they can be looked up by
    /// a user-friendly label.
    ///
    /// The `ActorMgr` actor will take the ownership of this map in `post_start` and leave a `None`
    /// here.
    _actor_labels: Option<ActorLabelMap>,
}

impl Default for Node {
    #[inline]
    fn default() -> Self {
        Self {
            listener_labels: HashSet::default(),
            listeners: Vec::new(),
            registry: RemoteMailboxRegistry::default(),
            actor_mgr: None,
            sessions: DoubleMap::default(),
            children: HashMap::default(),
            observers: ObserverSet::new(),
            _factories: Some(RemoteFactoryRegistry::default()),
            _actor_labels: Some(ActorLabelMap::default()),
        }
    }
}

impl Node {
    /// Constructs a new [`Node`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an IPC listener to the node.
    ///
    /// If the node already has a listener listening on the same endpoint, the new listener will
    /// replace the existing one, since the node is not started yet when this method is available.
    /// Note this is not the same as the [`AddListener`][command::AddListener] command, which will
    /// not replace the existing listener.
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

    /// Adds an remote addressable actor to the node.
    ///
    /// It also registers a label for the actor so that remote actors can look it up by a more
    /// user-friendly name.
    ///
    /// Duplicate actors and labels are silently skipped.
    pub fn with_actor<A>(mut self, label: String, actor: Address<A>) -> Self
    where
        A: Actor + RemoteAddressable,
    {
        if let Some(actor_labels) = &mut self._actor_labels {
            let actor_id = actor.index();
            if !actor_labels.contains_key(&label) && self.registry.insert(actor.into()) {
                actor_labels.insert(label, actor_id.as_local());
            }
        }
        self
    }

    /// Adds an remote spawnable actor factory to the node.
    ///
    /// Remote nodes can create instances of this actor type by sending a `CreateActor` node
    /// command.
    pub fn with_factory<A>(mut self) -> Self
    where
        A: Actor + RemoteSpawnable,
    {
        if let Some(factories) = &mut self._factories {
            factories.insert(
                A::TYPE_ID.as_u64(),
                Arc::new(RemoteFactoryShim::<A>(PhantomData)),
            );
        }
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

        if self.sessions.contains_key2(&endpoint) || self.sessions.contains_key2(&session_label) {
            return Err(NodeError::CreateSessionFailed(
                format!("session with endpoint '{}' already exists", endpoint).into(),
            ));
        }

        let session = Session::new(
            connection,
            self.registry.clone(),
            self.actor_mgr.clone().ok_or_else(|| {
                NodeError::CreateSessionFailed("actor manager does not exist".into())
            })?,
        );

        let (address, join_handle) = Session::create(endpoint.clone(), |child_ctx| {
            child_ctx.set_supervisor(Some(ctx.address().into()));
            Ok(session)
        })
        .map_err(|e| NodeError::CreateSessionFailed(e.into()))?;

        let session_id = address.index();

        // this will never fail since we have verified the session label is unique and the session id
        // is also unique as an actor id
        let _ = self
            .sessions
            .insert(session_id, endpoint.clone(), address.clone());
        if session_label != endpoint {
            let _ = self
                .sessions
                .insert(session_id, session_label.clone(), address.clone());
        }

        self.children.insert(address.clone().into(), join_handle);

        self.notify_observers(NodeEvent::SessionCreated(address.clone(), session_label))
            .await;

        Ok(address)
    }

    fn get_session(&self, session_ref: &SessionRef) -> Result<Address<Session>> {
        match session_ref {
            SessionRef::Address(address) => self
                .sessions
                .get_by_key1(&address.index())
                .cloned()
                .ok_or_else(|| NodeError::SessionNotFound(address.index().to_string())),

            SessionRef::Index(index) => self
                .sessions
                .get_by_key1(index)
                .cloned()
                .ok_or_else(|| NodeError::SessionNotFound(index.to_string())),

            SessionRef::Label(label) => self
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
        let factories = self._factories.take().unwrap_or_default();
        let actor_labels = self._actor_labels.take().unwrap_or_default();

        // the ActorMgr never fail, so it is not supervised
        let (address, join_handle) =
            ActorMgr::new(self.registry.clone(), actor_labels, factories).start("mgr")?;
        self.children.insert(address.clone().into(), join_handle);
        self.actor_mgr = Some(address);

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

impl<A> Handler<command::AddActor<A>> for Node
where
    A: Actor + RemoteAddressable,
{
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::AddActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        if let Some(actor_mgr) = &self.actor_mgr {
            if let Ok(rx) = actor_mgr.send(msg).await {
                return rx.await.unwrap_or(false);
            }
        }

        false
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

        if let Some(actor_mgr) = &self.actor_mgr {
            if let Ok(rx) = actor_mgr.send(msg).await {
                return rx.await.unwrap_or(false);
            }
        }

        false
    }
}

impl<A> Handler<command::RemoteCreateActor<A>> for Node
where
    A: Actor + RemoteSpawnable,
{
    type Result = FutureMessageResult<command::RemoteCreateActor<A>>;

    async fn handle(
        &mut self,
        msg: command::RemoteCreateActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command RemoteCreateActor<{}>", ShortName::of::<A>());

        let command::RemoteCreateActor {
            session,
            label,
            config,
            ..
        } = msg;

        let session = self.get_session(&session);

        FutureMessageResult::new(async move {
            session?
                .send(session::command::RemoteCreateActor::new(label, config))
                .await?
                // this await is time consuming since it involves IPC
                .await?
                .map_err(Into::into)
        })
    }
}

impl<A> Handler<command::RemoteGetActor<A>> for Node
where
    A: Actor + RemoteAddressable,
{
    type Result = FutureMessageResult<command::RemoteGetActor<A>>;

    async fn handle(
        &mut self,
        msg: command::RemoteGetActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!("Handle command GetRemoteActor");

        let command::RemoteGetActor { session, actor, .. } = msg;

        let session = self.get_session(&session);

        FutureMessageResult::new(async move {
            session?
                .send(session::command::RemoteGetActor::new(actor))
                .await?
                // this await is time consuming since it involves IPC
                .await?
                .map_err(Into::into)
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

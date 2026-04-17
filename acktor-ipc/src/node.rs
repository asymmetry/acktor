//! Node actor for managing IPC connections and sessions.

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

use ahash::HashMap;
use dashmap::DashMap;
use futures_util::future::join_all;
use tracing::{error, info, warn};

use acktor::{
    Actor, ActorContext, ActorId, Address, ErrorReport, Handler, JoinHandle, Recipient, SenderId,
    Signal,
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

impl Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let listeners: Vec<&str> = self.listeners.iter().map(|l| l.local_endpoint()).collect();
        let sessions: Vec<&str> = self.sessions.iter().map(|(_, l, _)| l.as_str()).collect();
        let observers: Vec<u64> = self
            .observers
            .iter()
            .map(|recipient| recipient.index())
            .collect();

        f.debug_struct("Node")
            .field("listeners", &listeners)
            // factory_registry is moved into the factory actor at startup
            .field("factory", &self.factory)
            .field("registry", &self.registry)
            .field("label_map", &self.label_map)
            .field("sessions", &sessions)
            .field(
                "children",
                &format_args!("HashMap({})", self.children.len()),
            )
            .field("observers", &observers)
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

    /// Adds an actor which implements [`RemoteActor`] trait to the node.
    pub fn with_actor<A>(self, actor: Address<A>) -> Self
    where
        A: RemoteActor,
    {
        self.registry.insert(actor);
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

            // SAFETY: `self.factory` is `Some` from `post_start` until `post_stop`; no new
            // session can be created outside that window, so the unwrap is infallible.
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
    type Result = String;

    async fn handle(
        &mut self,
        msg: command::AddListener<L>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!(
            "Handle command AddListener<{}>",
            acktor::utils::type_name::<L>()
        );

        let label = msg.0.local_endpoint().to_string();
        self.listeners.push(Box::new(msg.0));

        label
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

        let label = msg.0;
        self.listeners.retain(|l| l.local_endpoint() != label);
    }
}

impl<A> Handler<command::AddActor<A>> for Node
where
    A: RemoteActor,
{
    type Result = ();

    async fn handle(
        &mut self,
        msg: command::AddActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        debug_trace!(
            "Handle command AddActor<{}>",
            acktor::utils::type_name::<A>()
        );

        self.registry.insert(msg.0);
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

        let actor_id = msg.0;
        self.label_map.retain(|_, id| *id != actor_id);
        self.registry.remove(actor_id);
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

        let result: Result<_> = async {
            let session = self.get_session(&session)?;

            let rx = session
                .send(session::command::CreateRemoteActor {
                    label,
                    r#type,
                    config,
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

        let command::GetRemoteActor { session, actor } = msg;

        let result: Result<_> = async {
            let session = self.get_session(&session)?;

            let rx = session
                .send(session::command::GetRemoteActor { actor })
                .await?;

            Ok(rx)
        }
        .await;

        FutureMessageResult::new(
            async move { result?.await?.map_err(NodeError::RemoteActorNotFound) },
        )
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

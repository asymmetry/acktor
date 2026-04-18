use std::fmt::{self, Debug};
use std::result::Result;
use std::time::Duration;

use acktor::{
    Actor, ActorId, Handler, Message, Sender, SenderId,
    cron::{CronActor, CronContext},
    utils::debug_trace,
};

use super::{LabelMap, RemoteActorFactoryRegistry};
use crate::errors::{NodeError, SessionError};
use crate::remote_actor::RemoteActorRegistry;

/// A command which is used by a [`Session`][crate::session::Session] to create an actor in the
/// current process on behalf of a remote peer.
#[derive(Debug, Message)]
#[result_type(Result<ActorId, SessionError>)]
pub struct CreateActor {
    pub label: String,
    pub r#type: String,
    pub config: String,
}

/// An actor which is responsible for creating actors in the current process on behalf of remote
/// peers. It also keeps track of the actors in the registy and removes the stale ones.
pub struct Factory {
    factory_registry: RemoteActorFactoryRegistry,
    registry: RemoteActorRegistry,
    label_map: LabelMap,
}

impl Debug for Factory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let factory_registry: Vec<&str> =
            self.factory_registry.keys().map(|s| s.as_str()).collect();
        f.debug_struct("Factory")
            .field("factory_registry", &factory_registry)
            // registry and label_map are not included since they are shared with the node
            .finish()
    }
}

impl Factory {
    pub(crate) fn new(
        factory_registry: RemoteActorFactoryRegistry,
        registry: RemoteActorRegistry,
        label_map: LabelMap,
    ) -> Self {
        Self {
            factory_registry,
            registry,
            label_map,
        }
    }
}

impl Actor for Factory {
    type Context = CronContext<Self>;
    type Error = NodeError;
}

impl CronActor for Factory {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, NodeError> {
        self.registry.retain(|_, recipient| !recipient.is_closed());
        self.label_map
            .retain(|_, actor_id| self.registry.contains(*actor_id));

        Ok(Duration::from_secs(1))
    }
}

impl Handler<CreateActor> for Factory {
    type Result = Result<ActorId, SessionError>;

    async fn handle(&mut self, msg: CreateActor, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let CreateActor {
            label,
            r#type,
            config,
        } = msg;

        let Some(factory) = self.factory_registry.get(&r#type) else {
            return Err(SessionError::RemoteActorFactoryError(
                format!("no factory registered for actor type {}", r#type).into(),
            ));
        };

        if let Some(actor_id) = self.label_map.get(&label) {
            return Err(SessionError::RemoteActorFactoryError(
                format!(
                    "actor with label {} already existed with id {}",
                    label, *actor_id
                )
                .into(),
            ));
        }

        let factory = factory.clone();
        let label_cloned = label.clone();

        let (address, _) = tokio::spawn(async move { factory.create_remote(label_cloned, config) })
            .await
            .map_err(|e| SessionError::RemoteActorFactoryError(e.into()))?
            .map_err(SessionError::RemoteActorFactoryError)?;

        let actor_id = address.index();
        self.registry.insert(address);
        self.label_map.insert(label, actor_id);

        Ok(actor_id)
    }
}

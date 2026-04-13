use std::fmt::{self, Debug};
use std::result::Result;
use std::time::Duration;

use acktor::{
    Actor, ActorId, Handler, Message, Sender, SenderId,
    cron::{CronActor, CronContext},
    macros::debug_trace,
};

use super::{FactoryRegistry, LabelMap};
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
    factories: FactoryRegistry,
    registry: RemoteActorRegistry,
    label_map: LabelMap,
}

impl Debug for Factory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let factory_types: Vec<&str> = self.factories.keys().map(|s| s.as_str()).collect();
        f.debug_struct("Factory")
            .field("factories", &factory_types)
            .field("registry", &self.registry)
            .field("label_map", &self.label_map)
            .finish()
    }
}

impl Factory {
    pub(crate) fn new(
        factories: FactoryRegistry,
        registry: RemoteActorRegistry,
        label_map: LabelMap,
    ) -> Self {
        Self {
            factories,
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
    async fn task(
        &mut self,
        _ctx: &mut Self::Context,
    ) -> std::result::Result<Duration, Self::Error> {
        self.registry.retain(|_, recipient| !recipient.is_closed());

        for entry in self.label_map.iter() {
            let label = entry.key();
            let actor_id = entry.value();
            if !self.registry.contains(*actor_id) {
                self.label_map.remove(label);
            }
        }

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

        if let Some(actor_id) = self.label_map.get(&label) {
            return Err(SessionError::CreateActorFailed(
                format!(
                    "actor with label {} already exists with id {}",
                    label, *actor_id
                )
                .into(),
            ));
        }

        let Some(factory) = self.factories.get(&r#type) else {
            return Err(SessionError::CreateActorFailed(
                format!("no factory registered for actor type {}", r#type).into(),
            ));
        };

        let (address, _) = factory
            .create_remote(label.clone(), config)
            .map_err(SessionError::CreateActorFailed)?;

        let actor_id = address.index();
        self.registry.insert(address);
        self.label_map.insert(label, actor_id);

        Ok(actor_id)
    }
}

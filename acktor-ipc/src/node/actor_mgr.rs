use std::result::Result;
use std::time::Duration;

use ahash::HashMap;

use acktor::{
    Actor, ActorId, Handler, Message, SenderInfo,
    cron::{CronActor, CronContext},
    utils::debug_trace,
};

use super::command;
use crate::actor_ref::ActorRef;
use crate::error::{NodeError, SessionError};
use crate::remote::{
    RemoteAddressable, RemoteFactoryRegistry, RemoteMailbox, RemoteMailboxRegistry,
};

pub(crate) type ActorLabelMap = HashMap<String, u64>;

#[derive(Debug)]
pub(crate) struct CreateActor {
    pub(crate) type_id: u64,
    pub(crate) label: String,
    pub(crate) config: String,
}

impl Message for CreateActor {
    type Result = Result<ActorId, SessionError>;
}

#[derive(Debug)]
pub(crate) struct GetActor {
    pub(crate) label: String,
}

impl Message for GetActor {
    type Result = Result<RemoteMailbox, SessionError>;
}

pub(crate) struct ActorMgr {
    registry: RemoteMailboxRegistry,
    actor_labels: ActorLabelMap,
    factories: RemoteFactoryRegistry,
}

impl ActorMgr {
    pub(crate) fn new(
        registry: RemoteMailboxRegistry,
        actor_labels: ActorLabelMap,
        factories: RemoteFactoryRegistry,
    ) -> Self {
        Self {
            registry,
            actor_labels,
            factories,
        }
    }
}

impl Actor for ActorMgr {
    type Context = CronContext<Self>;
    type Error = NodeError;
}

impl CronActor for ActorMgr {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, NodeError> {
        self.registry.retain(|_, recipient| !recipient.is_closed());
        self.actor_labels
            .retain(|_, actor_id| self.registry.contains_index(*actor_id));

        Ok(Duration::from_secs(60))
    }
}

impl Handler<CreateActor> for ActorMgr {
    type Result = Result<ActorId, SessionError>;

    async fn handle(&mut self, msg: CreateActor, _ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        let CreateActor {
            type_id,
            label,
            config,
        } = msg;

        let Some(factory) = self.factories.get(&type_id) else {
            return Err(SessionError::CreateActorFailed(
                format!("no factory registered for actor type {}", type_id).into(),
            ));
        };

        if let Some(actor_id) = self.actor_labels.get(&label) {
            return Err(SessionError::CreateActorFailed(
                format!(
                    "actor with label {} already existed with id {}",
                    label, *actor_id
                )
                .into(),
            ));
        }

        let (address, _) = tokio::spawn({
            let factory = factory.clone();
            let label = label.clone();
            async move { factory.create_remote(label, config) }
        })
        .await
        .map_err(|e| SessionError::CreateActorFailed(e.into()))?
        .map_err(SessionError::CreateActorFailed)?;

        let actor_id = address.index();
        self.registry.insert(address);
        self.actor_labels.insert(label, actor_id.as_local());

        Ok(actor_id)
    }
}

impl Handler<GetActor> for ActorMgr {
    type Result = Result<RemoteMailbox, SessionError>;

    async fn handle(&mut self, msg: GetActor, _ctx: &mut Self::Context) -> Self::Result {
        // all read/write of actor_labels and all write of registry happens in ActorMgr, so there
        // is no race condition

        debug_trace!("Handle command {:?}", msg);

        let GetActor { label } = msg;

        self.actor_labels
            .get(&label)
            .ok_or_else(|| SessionError::ActorNotFound(label.clone()))
            .and_then(|actor_id| {
                self.registry
                    .get(*actor_id)
                    .ok_or_else(|| SessionError::ActorNotFound(label.clone()))
            })
    }
}

impl<A> Handler<command::AddActor<A>> for ActorMgr
where
    A: Actor + RemoteAddressable,
{
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::AddActor<A>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // all read/write of actor_labels and all write of registry happens in ActorMgr, so there
        // is no race condition

        debug_trace!("Handle command {:?}", msg);

        let command::AddActor { label, address } = msg;

        if self.actor_labels.contains_key(&label) {
            return false;
        }

        let actor_id = address.index().as_local();

        if !self.registry.insert(address.into()) {
            return false;
        }
        self.actor_labels.insert(label, actor_id);

        true
    }
}

impl Handler<command::RemoveActor> for ActorMgr {
    type Result = bool;

    async fn handle(
        &mut self,
        msg: command::RemoveActor,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        // all read/write of actor_labels and all write of registry happens in ActorMgr, so there
        // is no race condition

        debug_trace!("Handle command {:?}", msg);

        match msg.0 {
            ActorRef::Index(actor_id) => {
                let actor_id = actor_id.as_local();
                self.actor_labels.retain(|_, id| *id != actor_id);
                self.registry.remove(actor_id).is_some()
            }
            ActorRef::Label(label) => {
                if let Some(actor_id) = self.actor_labels.remove(&label) {
                    self.registry.remove(actor_id).is_some()
                } else {
                    false
                }
            }
        }
    }
}

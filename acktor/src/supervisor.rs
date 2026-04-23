//! Supervision for actors.
//!
//! This module provides a way to watch the status of an actor.
//!

use std::fmt::{self, Debug, Display};
use std::future::{self, Future};

use crate::actor::{Actor, ActorContext, ActorState};
use crate::address::{Address, Recipient, SenderId};
use crate::message::{Handler, Message};
use crate::utils::debug_trace;

/// A message which is used to report actor status to a supervisor.
pub enum SupervisionEvent<A>
where
    A: Actor,
{
    /// Warning, the actor could resume by itself.
    Warn(Address<A>, A::Error),
    /// Actor terminated with or without error.
    Terminated(Address<A>, Option<A::Error>),
    /// Actor panicked with the given panic info.
    Panicked(Address<A>, String),
    /// Actor state changed.
    State(Address<A>, ActorState),
}

impl<A> Debug for SupervisionEvent<A>
where
    A: Actor,
    A::Error: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SupervisionEvent::Warn(address, error) => f
                .debug_tuple("Warn")
                .field(&address.index())
                .field(&format_args!("{error}"))
                .finish(),
            SupervisionEvent::Terminated(address, error) => f
                .debug_tuple("Terminated")
                .field(&address.index())
                .field(&format_args!(
                    "{}",
                    error
                        .as_ref()
                        .map_or_else(|| "None".to_string(), |e| e.to_string())
                ))
                .finish(),
            SupervisionEvent::Panicked(address, info) => f
                .debug_tuple("Panicked")
                .field(&address.index())
                .field(&format_args!("{info}"))
                .finish(),
            SupervisionEvent::State(address, state) => f
                .debug_tuple("State")
                .field(&address.index())
                .field(state)
                .finish(),
        }
    }
}

impl<A> Message for SupervisionEvent<A>
where
    A: Actor,
{
    type Result = ();
}

/// A message which is used to set/unset a supervisor.
pub enum Supervisor<A>
where
    A: Actor,
{
    /// Set a supervisor.
    Set(Recipient<SupervisionEvent<A>>),
    /// Unset a supervisor.
    Unset,
}

impl<A> Debug for Supervisor<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let actor_type = crate::utils::type_name::<A>();
        match self {
            Supervisor::Set(recipient) => f
                .debug_tuple(&format!("Supervisor<{}>::Set", actor_type))
                .field(&recipient.index())
                .finish(),
            Supervisor::Unset => f
                .debug_tuple(&format!("Supervisor<{}>::Unset", actor_type))
                .finish(),
        }
    }
}

impl<A> Message for Supervisor<A>
where
    A: Actor,
{
    type Result = ();
}

impl<A> Handler<Supervisor<A>> for A
where
    A: Actor,
    A::Context: ActorContext<A>,
{
    type Result = ();

    fn handle(
        &mut self,
        msg: Supervisor<A>,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Self::Result> + Send {
        debug_trace!("Handle command {:?}", msg);

        match msg {
            Supervisor::Set(recipient) => {
                ctx.set_supervisor(Some(recipient));
            }
            Supervisor::Unset => {
                ctx.set_supervisor(None);
            }
        }

        future::ready(())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::channel::mpsc;
    use crate::envelope::Envelope;
    use crate::test_utils::Dummy;

    #[test]
    fn debug_supervision_event() {
        let (tx, _rx) = mpsc::channel::<Envelope<Dummy>>(1);
        let addr = Address::new(tx);
        let idx = addr.index();

        let event: SupervisionEvent<Dummy> =
            SupervisionEvent::Warn(addr.clone(), anyhow::anyhow!("oops"));
        assert_eq!(format!("{event:?}"), format!("Warn({idx}, oops)"));

        let event: SupervisionEvent<Dummy> = SupervisionEvent::Terminated(addr.clone(), None);
        assert_eq!(format!("{event:?}"), format!("Terminated({idx}, None)"));

        let event: SupervisionEvent<Dummy> =
            SupervisionEvent::Terminated(addr.clone(), Some(anyhow::anyhow!("boom")));
        assert_eq!(format!("{event:?}"), format!("Terminated({idx}, boom)"));

        let event: SupervisionEvent<Dummy> =
            SupervisionEvent::Panicked(addr.clone(), "panicked!".to_string());
        assert_eq!(format!("{event:?}"), format!("Panicked({idx}, panicked!)"));

        let event: SupervisionEvent<Dummy> = SupervisionEvent::State(addr, ActorState::Running);
        assert_eq!(format!("{event:?}"), format!("State({idx}, Running)"));
    }
}

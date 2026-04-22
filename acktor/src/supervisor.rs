//! Supervision for actors.
//!
//! This module provides a way to watch the status of an actor.
//!

use std::fmt;
use std::future::{self, Future};

use crate::actor::{Actor, ActorContext, ActorState};
use crate::address::{Address, Recipient, SenderId};
use crate::message::{Handler, Message};
use crate::utils::debug_trace;

/// A message which is used to report actor status to a supervisor.
#[derive(Debug)]
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

impl<A> fmt::Debug for Supervisor<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Supervisor::Set(recipient) => f
                .debug_tuple("Set")
                .field(&format_args!(
                    "Recipient<SupervisionEvent<{}>>({})",
                    crate::utils::type_name::<A>(),
                    recipient.index()
                ))
                .finish(),
            Supervisor::Unset => f.debug_tuple("Unset").finish(),
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

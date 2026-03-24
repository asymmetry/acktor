//! Supervision for actors.
//!
//! This module provides a way to watch the status of an actor.
//!

use std::fmt;
use std::future::{self, Future};

use cactor_macros::debug_trace;

use crate::actor::{Actor, ActorContext, ActorState};
use crate::address::{Address, Recipient};
use crate::message::{Handler, Message};

/// A message which is used to report actor status to a supervisor.
#[allow(dead_code)]
#[derive(Debug)]
pub enum SupervisionEvent<A>
where
    A: Actor,
{
    /// Warning, the actor could resume by itself.
    Warn(Address<A>, A::Error),
    /// Actor terminated with or without error.
    Terminated(Address<A>, Option<A::Error>),
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
            Supervisor::Set(recipient) => f.debug_tuple("Set").field(&recipient).finish(),
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

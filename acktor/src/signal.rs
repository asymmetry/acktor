use std::future::{self, Future};

use acktor_macros::debug_trace;

use crate::actor::{Actor, ActorContext};
use crate::message::{Handler, Message};

/// A message which is used to stop/terminate an actor.
#[derive(Debug)]
pub enum Signal {
    /// Stop the actor.
    ///
    /// Set the actor's state to [`Stopping`][crate::actor::ActorState::Stopping] and trigger
    /// the [`stopping`][Actor::stopping] method of the actor. The actor might be able to resume
    /// by itself.
    Stop,
    /// Terminate the actor.
    ///
    /// Set the actor's state to [`Stopped`][crate::actor::ActorState::Stopped] and trigger
    /// the [`post_stop`][Actor::post_stop] method.
    Terminate,
}

impl Message for Signal {
    type Result = ();
}

impl<A> Handler<Signal> for A
where
    A: Actor,
    A::Context: ActorContext<A>,
{
    type Result = ();

    fn handle(
        &mut self,
        msg: Signal,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Self::Result> + Send {
        debug_trace!("Handle command {:?}", msg);

        match msg {
            Signal::Stop => {
                ctx.stop();
            }
            Signal::Terminate => {
                ctx.terminate();
            }
        }

        future::ready(())
    }
}

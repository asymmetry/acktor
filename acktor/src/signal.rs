use std::future::{self, Future};

use crate::actor::{Actor, ActorContext};
use crate::message::{Handler, Message};
use crate::utils::debug_trace;

/// A message which is used to stop/terminate an actor.
///
/// `Handler<Signal>` is implemented for all actors automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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
    /// the [`post_stop`][Actor::post_stop] method. Any messages still queued in the mailbox
    /// behind this signal are dropped without being handled.
    Terminate,
}

impl Message for Signal {
    type Result = ();
}

impl TryFrom<u8> for Signal {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Signal::Stop),
            1 => Ok(Signal::Terminate),
            _ => Err(()),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal() {
        assert_eq!(Signal::try_from(0), Ok(Signal::Stop));
        assert_eq!(Signal::try_from(1), Ok(Signal::Terminate));
        assert_eq!(Signal::try_from(2), Err(()));
    }
}

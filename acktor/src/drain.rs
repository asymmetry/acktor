//! Compile-time selection of the mailbox draining behavior of an actor context.
//!
//! Discarding queued messages is a niche capability with sharp edges, so an actor opts into it
//! explicitly by picking the drain policy of its context:
//!
//! ```
//! use acktor::{Actor, Context};
//! use acktor::drain::Drainable;
//!
//! struct Worker;
//!
//! impl Actor for Worker {
//!     type Context = Context<Self, Drainable>;
//!     type Error = anyhow::Error;
//! }
//! ```
//!
//! With the default [`NoDrain`] policy the whole machinery is compiled out: the per-context
//! state is zero-sized and the mailbox is never measured.

use std::fmt::Debug;

use crate::actor::Actor;
use crate::address::Mailbox;

/// Describes how an actor context discards messages already queued in its mailbox.
///
/// The two implementations are [`NoDrain`] and [`Drainable`]; this trait is not meant to be
/// implemented outside of this crate.
pub trait DrainPolicy: Send + 'static {
    /// The state the policy keeps in the context, `()` when draining is unsupported.
    type State: Default + Send + Debug;

    /// Records how many messages are queued behind the message which is about to be handled.
    fn snapshot<A>(state: &mut Self::State, mailbox: &Mailbox<A>)
    where
        A: Actor;

    /// Discards the recorded messages if the message handler has requested a drain.
    fn apply<A>(state: &mut Self::State, mailbox: &mut Mailbox<A>)
    where
        A: Actor;
}

/// The default drain policy, which does not support draining at all.
///
/// A context using this policy has no `drain_mailbox` method and never measures its mailbox.
#[derive(Debug, Clone, Copy)]
pub struct NoDrain;

impl DrainPolicy for NoDrain {
    type State = ();

    #[inline(always)]
    fn snapshot<A>(_state: &mut Self::State, _mailbox: &Mailbox<A>)
    where
        A: Actor,
    {
    }

    #[inline(always)]
    fn apply<A>(_state: &mut Self::State, _mailbox: &mut Mailbox<A>)
    where
        A: Actor,
    {
    }
}

/// The drain policy which supports discarding queued messages.
///
/// A context using this policy gains a `drain_mailbox` method and measures its mailbox once per
/// handled message.
#[derive(Debug, Clone, Copy)]
pub struct Drainable;

/// The state kept by the [`Drainable`] policy.
#[derive(Debug, Default)]
pub struct DrainState {
    mailbox_len: usize,
    requested: bool,
}

impl DrainState {
    /// Requests a drain of the messages recorded by the last snapshot.
    #[inline]
    pub(crate) fn request(&mut self) {
        self.requested = true;
    }
}

impl DrainPolicy for Drainable {
    type State = DrainState;

    fn snapshot<A>(state: &mut Self::State, mailbox: &Mailbox<A>)
    where
        A: Actor,
    {
        state.mailbox_len = mailbox.len();
    }

    fn apply<A>(state: &mut Self::State, mailbox: &mut Mailbox<A>)
    where
        A: Actor,
    {
        if state.requested {
            for _ in 0..state.mailbox_len {
                // the mailbox contains at least `mailbox_len` messages, so try_recv never fail
                let _ = mailbox.try_recv();
            }
            state.requested = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::test_utils::{Ping, make_address};

    #[tokio::test]
    async fn test_no_drain_is_zero_sized() -> Result<()> {
        assert_eq!(std::mem::size_of::<<NoDrain as DrainPolicy>::State>(), 0);

        // the policy is inert: nothing is recorded and nothing is discarded
        let (address, mut mailbox) = make_address(4);
        address.try_do_send(Ping(1))?;
        address.try_do_send(Ping(2))?;

        NoDrain::snapshot(&mut (), &mailbox);
        NoDrain::apply(&mut (), &mut mailbox);
        assert_eq!(mailbox.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_drainable_discards_only_the_snapshot() -> Result<()> {
        let (address, mut mailbox) = make_address(4);
        address.try_do_send(Ping(1))?;
        address.try_do_send(Ping(2))?;

        let mut state = DrainState::default();
        Drainable::snapshot(&mut state, &mailbox);

        // enqueued after the snapshot, so it must survive the drain
        address.try_do_send(Ping(3))?;

        state.request();
        Drainable::apply(&mut state, &mut mailbox);
        assert_eq!(mailbox.len(), 1);

        // the request is one-shot
        Drainable::apply(&mut state, &mut mailbox);
        assert_eq!(mailbox.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_drainable_without_request_keeps_everything() -> Result<()> {
        let (address, mut mailbox) = make_address(4);
        address.try_do_send(Ping(1))?;
        address.try_do_send(Ping(2))?;

        let mut state = DrainState::default();
        Drainable::snapshot(&mut state, &mailbox);
        Drainable::apply(&mut state, &mut mailbox);
        assert_eq!(mailbox.len(), 2);

        Ok(())
    }
}

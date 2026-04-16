use std::fmt::{self, Debug};

use crate::actor::Actor;
use crate::channel::mpsc;
use crate::envelope::Envelope;
use crate::errors::RecvError;

/// The mailbox of an actor, which holds a queue of messages to be processed by the actor.
pub struct Mailbox<A>
where
    A: Actor,
{
    rx: tokio::sync::mpsc::Receiver<Envelope<A>>,
}

impl<A> Debug for Mailbox<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("Mailbox<{}>", crate::utils::type_name::<A>()))
    }
}

impl<A> Mailbox<A>
where
    A: Actor,
{
    /// Constructs a new [`Mailbox`] with the given receiver.
    pub fn new(rx: mpsc::Receiver<Envelope<A>>) -> Self {
        Self {
            rx: rx.into_inner(),
        }
    }

    /// Receives the next message for this mailbox.
    pub fn recv(&mut self) -> impl Future<Output = Option<Envelope<A>>> + Send + '_ {
        self.rx.recv()
    }

    /// Tries to receive the next message for this mailbox.
    pub fn try_recv(&mut self) -> Result<Envelope<A>, RecvError> {
        self.rx.try_recv().map_err(Into::into)
    }

    /// Closes the mailbox, preventing any new messages from being sent to it.
    pub fn close(&mut self) {
        self.rx.close();
    }

    /// Checks if a mailbox is closed.
    pub fn is_closed(&self) -> bool {
        self.rx.is_closed()
    }

    /// Checks if a mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    /// Returns the number of messages in the mailbox.
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    /// Returns the current capacity of the mailbox.
    pub fn capacity(&self) -> usize {
        self.rx.capacity()
    }

    /// Returns the maximum buffer capacity of the mailbox.
    pub fn max_capacity(&self) -> usize {
        self.rx.max_capacity()
    }
}

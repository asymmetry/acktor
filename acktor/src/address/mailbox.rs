use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::actor::Actor;
use crate::envelope::Envelope;

/// A type which holds a queue of messages to be processed by an actor.
///
/// Mailbox is the receiver side of the communication channel of an actor.
#[derive(Debug)]
pub struct Mailbox<A>
where
    A: Actor,
{
    rx: mpsc::Receiver<Envelope<A>>,
}

impl<A> Mailbox<A>
where
    A: Actor,
{
    /// Constructs a new mailbox with the given receiver.
    pub fn new(rx: mpsc::Receiver<Envelope<A>>) -> Self {
        Self { rx }
    }

    /// Receives the next message from this mailbox.
    pub fn recv(&mut self) -> impl Future<Output = Option<Envelope<A>>> + Send + '_ {
        self.rx.recv()
    }

    /// Attempts to receive the next message from this mailbox without blocking.
    pub fn try_recv(&mut self) -> Result<Envelope<A>, TryRecvError> {
        self.rx.try_recv()
    }

    /// Receives the next message from this mailbox.
    ///
    /// This method is intended for use cases where you are receiving from synchronous code.
    pub fn blocking_recv(&mut self) -> Option<Envelope<A>> {
        self.rx.blocking_recv()
    }

    /// Checks if the mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    /// Returns the number of messages in the mailbox.
    pub fn len(&self) -> usize {
        self.rx.len()
    }
}

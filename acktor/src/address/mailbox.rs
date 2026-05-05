use std::fmt::{self, Debug};

use crate::actor::Actor;
use crate::channel::mpsc;
use crate::envelope::Envelope;
use crate::error::RecvError;
use crate::utils::ShortName;

/// The mailbox of an actor, which holds a queue of messages to be processed by the actor.
#[repr(transparent)]
pub struct Mailbox<A>(mpsc::Receiver<Envelope<A>>)
where
    A: Actor;

impl<A> Debug for Mailbox<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}", ShortName::of::<Self>()))
    }
}

impl<A> Mailbox<A>
where
    A: Actor,
{
    /// Constructs a new [`Mailbox`] with the given receiver.
    pub fn new(rx: mpsc::Receiver<Envelope<A>>) -> Self {
        Self(rx)
    }

    /// Receives a message from the mailbox.
    pub fn recv(&mut self) -> impl Future<Output = Result<Envelope<A>, RecvError>> + Send {
        self.0.recv()
    }

    /// Attempts to receive a message from the mailbox.
    pub fn try_recv(&mut self) -> Result<Envelope<A>, RecvError> {
        self.0.try_recv()
    }

    /// Closes the mailbox, preventing any new messages from being sent to it.
    pub fn close(&mut self) {
        self.0.close();
    }

    /// Checks if the mailbox is closed.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Checks if the mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of messages in the mailbox.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the current capacity of the mailbox.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns the maximum buffer capacity of the mailbox.
    pub fn max_capacity(&self) -> usize {
        self.0.max_capacity()
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use crate::error::SendError;
    use crate::test_utils::{Ping, make_address};

    #[tokio::test]
    async fn test_mailbox() -> Result<()> {
        // basics
        let (a1, mut m1) = make_address(4);
        assert!(m1.is_empty());
        assert_eq!(m1.len(), 0);
        assert_eq!(m1.capacity(), 4);
        assert_eq!(m1.max_capacity(), 4);
        assert!(m1.try_recv().is_err());
        assert!(!m1.is_closed());

        // len reflects pending messages
        a1.try_do_send(Ping(1))?;
        a1.try_do_send(Ping(2))?;
        assert_eq!(m1.len(), 2);
        assert!(!m1.is_empty());

        // close() propagates to the address and rejects further sends
        assert!(!a1.is_closed());
        m1.close();
        assert!(a1.is_closed());
        let result = a1.try_do_send(Ping(3));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_debug_fmt() {
        let (_, mailbox) = make_address(4);
        assert_eq!(format!("{:?}", mailbox), "Mailbox<Dummy>");
    }
}

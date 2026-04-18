use crate::actor::Actor;
use crate::channel::{mpsc, oneshot};
use crate::envelope::{Envelope, ToEnvelope};
use crate::message::Message;

/// Permit to send one message to an actor.
#[derive(Debug)]
pub struct SendPermit<'a, A>
where
    A: Actor,
{
    pub(super) permit: mpsc::Permit<'a, Envelope<A>>,
}

impl<A> SendPermit<'_, A>
where
    A: Actor,
{
    /// Sends a message to an actor using the permit and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the
    /// message response.
    ///
    /// This method will consume the permit.
    pub fn send<M, EP>(self, msg: M) -> oneshot::Receiver<M::Result>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)));
        rx
    }

    /// Sends a message to an actor using the permit without expecting a response.
    ///
    /// This method will consume the permit.
    pub fn do_send<M, EP>(self, msg: M)
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP>,
    {
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None));
    }
}

/// Owned permit to send one message to an actor.
#[derive(Debug)]
pub struct OwnedSendPermit<A>
where
    A: Actor,
{
    pub(super) permit: mpsc::OwnedPermit<Envelope<A>>,
}

impl<A> OwnedSendPermit<A>
where
    A: Actor,
{
    /// Sends a message to an actor using the permit and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the
    /// message response.
    ///
    /// This method will consume the permit.
    pub fn send<M, EP>(self, msg: M) -> oneshot::Receiver<M::Result>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)));
        rx
    }

    /// Sends a message to an actor using the permit without expecting a response.
    ///
    /// This method will consume the permit.
    pub fn do_send<M, EP>(self, msg: M)
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP>,
    {
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None));
    }
}

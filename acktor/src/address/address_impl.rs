use std::any::Any;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::pin::Pin;

use futures_util::{FutureExt, TryFutureExt};
use tokio::sync::{
    mpsc::{
        self, OwnedPermit,
        error::{SendError, TrySendError},
    },
    oneshot,
};

use super::recipient::Recipient;
use super::sender::{SendResult, Sender, SenderId, TrySendResult};
use crate::actor::{Actor, ActorId};
use crate::envelope::{Envelope, FromEnvelope, ToEnvelope};
use crate::message::Message;
use crate::utils::create_actor_id;

/// A type which is used to send messages to an actor.
pub struct Address<A>
where
    A: Actor,
{
    index: ActorId,
    tx: mpsc::Sender<Envelope<A>>,
}

impl<A> Debug for Address<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("Address<{}>", crate::utils::type_name::<A>()))
            .field(&self.index)
            .finish()
    }
}

impl<A> Clone for Address<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            tx: self.tx.clone(),
        }
    }
}

impl<A> PartialEq for Address<A>
where
    A: Actor,
{
    fn eq(&self, other: &Self) -> bool {
        self.index.eq(&other.index)
    }
}

impl<A> Eq for Address<A> where A: Actor {}

impl<A> Hash for Address<A>
where
    A: Actor,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.index.hash(state);
    }
}

impl<A> Address<A>
where
    A: Actor,
{
    /// Wraps a message sender to form an address.
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self {
            index: create_actor_id(),
            tx,
        }
    }

    /// Returns the index of the address.
    pub fn index(&self) -> ActorId {
        self.index
    }

    /// Checks if the mailbox of the actor is closed.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Returns the capacity of the mailbox of the actor.
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    /// Converts this address into a recipient of a specific message type.
    pub fn recipient<M, EP>(self) -> Recipient<M, EP>
    where
        M: Message<EP>,
        EP: 'static,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.into()
    }

    /// Sends a message to an actor and returns a [`oneshot::Receiver`] which could be used to
    /// receive the response.
    pub fn send<M, EP>(&self, msg: M) -> impl Future<Output = SendResult<M, M::Result>> + Send + '_
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map(|result| match result {
                Ok(_) => Ok(rx),
                Err(e) => Err(SendError(<A::Context as FromEnvelope<A, M, EP>>::unpack(
                    e.0,
                ))),
            })
    }

    /// Sends a message to an actor without expecting a response.
    pub fn do_send<M, EP>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<(), SendError<M>>> + Send + '_
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.tx
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Attempts to send a message to an actor and returns a [`oneshot::Receiver`] which could be
    /// used to receive the response.
    pub fn try_send<M, EP>(&self, msg: M) -> TrySendResult<M, M::Result>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map_err(|e| match e {
                TrySendError::Closed(e) => {
                    TrySendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e))
                }
                TrySendError::Full(e) => {
                    TrySendError::Full(<A::Context as FromEnvelope<A, M, EP>>::unpack(e))
                }
            })?;

        Ok(rx)
    }

    /// Attempts to send a message to an actor without expecting a response.
    pub fn try_do_send<M, EP>(&self, msg: M) -> Result<(), TrySendError<M>>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.tx
            .try_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| match e {
                TrySendError::Closed(e) => {
                    TrySendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e))
                }
                TrySendError::Full(e) => {
                    TrySendError::Full(<A::Context as FromEnvelope<A, M, EP>>::unpack(e))
                }
            })?;

        Ok(())
    }

    /// Sends a message to an actor and returns a [`oneshot::Receiver`] which could be used to
    /// receive the response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    pub fn blocking_send<M, EP>(&self, msg: M) -> SendResult<M, M::Result>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .blocking_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map_err(|e| SendError(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))?;

        Ok(rx)
    }

    /// Sends a message to an actor without expecting a response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    pub fn blocking_do_send<M, EP>(&self, msg: M) -> Result<(), SendError<M>>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.tx
            .blocking_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))?;

        Ok(())
    }
}

impl<A> SenderId for Address<A>
where
    A: Actor,
{
    fn index(&self) -> ActorId {
        self.index
    }
}

impl<A, M, EP> Sender<M, EP> for Address<A>
where
    A: Actor,
    M: Message<EP>,
    EP: 'static,
    A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
{
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    fn send(&self, msg: M) -> Pin<Box<dyn Future<Output = SendResult<M, M::Result>> + Send + '_>> {
        self.send(msg).boxed()
    }

    fn do_send(
        &self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + '_>> {
        self.do_send(msg).boxed()
    }

    fn try_send(&self, msg: M) -> TrySendResult<M, M::Result> {
        self.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> Result<(), TrySendError<M>> {
        self.try_do_send(msg)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M, M::Result> {
        self.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> Result<(), SendError<M>> {
        self.blocking_do_send(msg)
    }

    fn as_any(&self) -> Option<&(dyn Any + 'static)> {
        Some(self)
    }
}

/// Reserved permit to send a message to an actor.
#[derive(Debug)]
pub struct ReservedSendPermit<A>
where
    A: Actor,
{
    permit: OwnedPermit<Envelope<A>>,
}

impl<A> ReservedSendPermit<A>
where
    A: Actor,
{
    /// Sends a message to an actor use the permit and returns a [`oneshot::Receiver`] which
    /// could be used to receive the response.
    ///
    /// This method will consume the permit.
    pub fn send<M, EP>(self, msg: M) -> Result<oneshot::Receiver<M::Result>, SendError<M>>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)));

        Ok(rx)
    }

    /// Sends a message to an actor use the permit without expecting a response.
    ///
    /// This method will consume the permit.
    pub fn do_send<M, EP>(self, msg: M) -> Result<(), SendError<M>>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.permit
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None));

        Ok(())
    }
}

impl<A> Address<A>
where
    A: Actor,
{
    /// Reserves a permit to send a message to an actor.
    pub fn reserve(
        &self,
    ) -> impl Future<Output = Result<ReservedSendPermit<A>, SendError<()>>> + Send + '_ {
        self.tx
            .clone()
            .reserve_owned()
            .map_ok(|permit| ReservedSendPermit { permit })
    }

    /// Attempts to reserve a permit to send a message to an actor.
    pub fn try_reserve(
        &self,
    ) -> Result<ReservedSendPermit<A>, TrySendError<mpsc::Sender<Envelope<A>>>> {
        Ok(ReservedSendPermit {
            permit: self.tx.clone().try_reserve_owned()?,
        })
    }
}

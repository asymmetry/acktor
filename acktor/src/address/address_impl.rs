use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};

#[cfg(feature = "erased-recipient")]
use std::any::Any;

use futures_util::{FutureExt, TryFutureExt};

use super::permit::{OwnedSendPermit, SendPermit};
use super::recipient::Recipient;
use super::sender::{Sender, SenderId};
use crate::actor::{Actor, ActorId};
use crate::channel::{mpsc, oneshot};
use crate::envelope::{Envelope, FromEnvelope, ToEnvelope};
use crate::errors::{DoSendResult, DoSendResultFuture, SendError, SendResult, SendResultFuture};
use crate::message::Message;
use crate::utils::create_actor_id;

#[cfg(feature = "erased-recipient")]
use crate::actor::ErasedRecipientFn;

/// The address of an actor.
///
/// It is used to send messages to an actor.
pub struct Address<A>
where
    A: Actor,
{
    index: ActorId,
    tx: mpsc::Sender<Envelope<A>>,
    /// Optional conversion function pointer baked in at construction (see
    /// [`Actor::erased_recipient_fn`]).
    #[cfg(feature = "erased-recipient")]
    erased_recipient_fn: Option<ErasedRecipientFn<A>>,
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
            #[cfg(feature = "erased-recipient")]
            erased_recipient_fn: self.erased_recipient_fn,
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
    /// Constructs a new [`Address`] from a [`mpsc::Sender`].
    ///
    /// Triggers [`Actor::erased_recipient_fn`] once (if the feature `erased-recipient` is
    /// enabled) and stores the result.
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self {
            index: create_actor_id(),
            tx,
            #[cfg(feature = "erased-recipient")]
            erased_recipient_fn: A::erased_recipient_fn(),
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

    /// Sends a message to an actor and returns a [`Receiver`][crate::channel::oneshot::Receiver]
    /// which could be used to receive the message response.
    pub fn send<M, EP>(&self, msg: M) -> impl Future<Output = SendResult<M, EP>> + Send + '_
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map_ok(|_| rx)
            .map_err(|e| SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Sends a message to an actor without expecting a response.
    pub fn do_send<M, EP>(&self, msg: M) -> impl Future<Output = DoSendResult<M>> + Send + '_
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.tx
            .send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Attempts to send a message to an actor and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which could be used to receive the
    /// message response.
    pub fn try_send<M, EP>(&self, msg: M) -> SendResult<M, EP>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = <A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx));
        self.tx.try_send(envelope).map(|_| rx).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(envelope) => {
                SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
            mpsc::error::TrySendError::Full(envelope) => {
                SendError::Full(<A::Context as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
        })
    }

    /// Attempts to send a message to an actor without expecting a response.
    pub fn try_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let envelope = <A::Context as ToEnvelope<A, M, EP>>::pack(msg, None);
        self.tx.try_send(envelope).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(envelope) => {
                SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
            mpsc::error::TrySendError::Full(envelope) => {
                SendError::Full(<A::Context as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
        })
    }

    /// Sends a message to an actor and returns a [`Receiver`][crate::channel::oneshot::Receiver]
    /// which could be used to receive the message response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    pub fn blocking_send<M, EP>(&self, msg: M) -> SendResult<M, EP>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .blocking_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map(|_| rx)
            .map_err(|e| SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Sends a message to an actor without expecting a response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    pub fn blocking_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message<EP>,
        A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    {
        self.tx
            .blocking_send(<A::Context as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError::Closed(<A::Context as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Reserves channel capacity to send one message to an actor.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub fn reserve(
        &self,
    ) -> impl Future<Output = Result<SendPermit<'_, A>, SendError<()>>> + Send + '_ {
        self.tx
            .reserve()
            .map_ok(|permit| SendPermit { permit })
            .map_err(Into::into)
    }

    /// Attempts to reserve channel capacity to send one message to an actor.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub fn try_reserve(&self) -> Result<SendPermit<'_, A>, SendError<()>> {
        self.tx
            .try_reserve()
            .map(|permit| SendPermit { permit })
            .map_err(Into::into)
    }

    /// Reserves channel capacity to send one message to an actor.
    ///
    /// This method clones the internal [`mpsc::Sender`] and returns a [`OwnedSendPermit`].
    pub fn reserve_owned(
        &self,
    ) -> impl Future<Output = Result<OwnedSendPermit<A>, SendError<()>>> + Send + '_ {
        self.tx
            .clone()
            .reserve_owned()
            .map_ok(|permit| OwnedSendPermit { permit })
            .map_err(Into::into)
    }

    /// Attempts to reserve channel capacity to send one message to an actor.
    ///
    /// This method clones the internal [`mpsc::Sender`] and returns a [`OwnedSendPermit`].
    pub fn try_reserve_owned(&self) -> Result<OwnedSendPermit<A>, SendError<()>> {
        self.tx
            .clone()
            .try_reserve_owned()
            .map(|permit| OwnedSendPermit { permit })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Closed(_) => SendError::Closed(()),
                mpsc::error::TrySendError::Full(_) => SendError::Full(()),
            })
    }

    /// If the actor backing this sender opted into the conversion via
    /// [`Actor::erased_recipient_fn`][crate::actor::Actor::erased_recipient_fn], returns the
    /// resulting type-erased trait object. Downstream crates can then downcast this into the
    /// concrete type they used to override the
    /// [`Actor::erased_recipient_fn`][crate::actor::Actor::erased_recipient_fn] method.
    #[cfg(feature = "erased-recipient")]
    #[cfg_attr(docsrs, doc(cfg(feature = "erased-recipient")))]
    pub fn erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.erased_recipient_fn.map(|f| f(self))
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

    fn send(&self, msg: M) -> SendResultFuture<'_, M, EP> {
        self.send(msg).boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.do_send(msg).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M, EP> {
        self.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.try_do_send(msg)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M, EP> {
        self.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.blocking_do_send(msg)
    }

    #[cfg(feature = "erased-recipient")]
    fn erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.erased_recipient()
    }
}

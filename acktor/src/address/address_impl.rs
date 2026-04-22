use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[cfg(feature = "type-erased-recipient-hook")]
use std::any::Any;

use futures_util::{FutureExt, TryFutureExt};
use tokio::time::Duration;

use super::permit::{OwnedSendPermit, SendPermit};
use super::recipient::Recipient;
use super::sender::{
    ClosedResultFuture, DoSendResult, DoSendResultFuture, SendResult, SendResultFuture, Sender,
    SenderId,
};
use crate::actor::{Actor, ActorId};
use crate::channel::{mpsc, oneshot};
use crate::envelope::{Envelope, FromEnvelope, ToEnvelope};
use crate::errors::SendError;
use crate::message::{Handler, Message};
use crate::utils::create_actor_id;

#[cfg(feature = "type-erased-recipient-hook")]
use crate::actor::TypeErasedRecipientFn;

/// The address of an actor.
///
/// It is used to send messages to an actor.
pub struct Address<A>
where
    A: Actor,
{
    index: ActorId,
    tx: mpsc::Sender<Envelope<A>>,
    /// Opt-in conversion hook baked in at construction, see [`Actor::type_erased_recipient_fn`]
    /// for details.
    #[cfg(feature = "type-erased-recipient-hook")]
    type_erased_recipient_fn: Option<TypeErasedRecipientFn<A>>,
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
            #[cfg(feature = "type-erased-recipient-hook")]
            type_erased_recipient_fn: self.type_erased_recipient_fn,
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
    /// Triggers [`Actor::type_erased_recipient_fn`] once (if the feature
    /// `type-erased-recipient-hook` is enabled) and stores the result.
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self {
            index: create_actor_id(),
            tx,
            #[cfg(feature = "type-erased-recipient-hook")]
            type_erased_recipient_fn: A::type_erased_recipient_fn(),
        }
    }

    /// Returns the index of the address.
    pub fn index(&self) -> ActorId {
        self.index
    }

    /// Completes when the mailbox of the actor has been closed.
    pub fn closed(&self) -> impl Future<Output = ()> + Send {
        self.tx.closed()
    }

    /// Checks if the mailbox of the actor is closed.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Returns the current capacity of the mailbox of the actor.
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    /// Sends a message, waiting until there is capacity, and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    ///  response.
    pub fn send<M, EP>(&self, msg: M) -> impl Future<Output = SendResult<M>> + Send + '_
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(<A as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map_ok(|_| rx)
            .map_err(|e| SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Sends a message, waiting until there is capacity, without expecting a response.
    pub fn do_send<M, EP>(&self, msg: M) -> impl Future<Output = DoSendResult<M>> + Send + '_
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        self.tx
            .send(<A as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Attempts to immediately send a message and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    pub fn try_send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = <A as ToEnvelope<A, M, EP>>::pack(msg, Some(tx));
        self.tx.try_send(envelope).map(|_| rx).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(envelope) => {
                SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
            mpsc::error::TrySendError::Full(envelope) => {
                SendError::Full(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
        })
    }

    /// Attempts to immediately send a message without expecting a response.
    pub fn try_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        let envelope = <A as ToEnvelope<A, M, EP>>::pack(msg, None);
        self.tx.try_send(envelope).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(envelope) => {
                SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
            mpsc::error::TrySendError::Full(envelope) => {
                SendError::Full(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
            }
        })
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    pub fn send_timeout<M, EP>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = SendResult<M>> + Send + '_
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_timeout(<A as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)), timeout)
            .map_ok(|_| rx)
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Closed(envelope) => {
                    SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
                }
                mpsc::error::SendTimeoutError::Timeout(envelope) => {
                    SendError::Timeout(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
                }
            })
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, without
    /// expecting a response.
    pub fn do_send_timeout<M, EP>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = DoSendResult<M>> + Send + '_
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        self.tx
            .send_timeout(<A as ToEnvelope<A, M, EP>>::pack(msg, None), timeout)
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Closed(envelope) => {
                    SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
                }
                mpsc::error::SendTimeoutError::Timeout(envelope) => {
                    SendError::Timeout(<A as FromEnvelope<A, M, EP>>::unpack(envelope))
                }
            })
    }

    /// Blocking send to call outside of asynchronous contexts.
    ///
    /// This method is intended for use cases where you are sending from synchronous code to
    /// asynchronous code.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .blocking_send(<A as ToEnvelope<A, M, EP>>::pack(msg, Some(tx)))
            .map(|_| rx)
            .map_err(|e| SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Blocking do_send to call outside of asynchronous contexts.
    ///
    /// This method is intended for use cases where you are sending from synchronous code to
    /// asynchronous code.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        A: Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
        M: Message,
    {
        self.tx
            .blocking_send(<A as ToEnvelope<A, M, EP>>::pack(msg, None))
            .map_err(|e| SendError::Closed(<A as FromEnvelope<A, M, EP>>::unpack(e.0)))
    }

    /// Reserves channel capacity to send one message.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub fn reserve(&self) -> impl Future<Output = Result<SendPermit<'_, A>, SendError<()>>> + Send {
        self.tx
            .reserve()
            .map_ok(|permit| SendPermit { permit })
            .map_err(Into::into)
    }

    /// Attempts to reserve channel capacity to send one message.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub fn try_reserve(&self) -> Result<SendPermit<'_, A>, SendError<()>> {
        self.tx
            .try_reserve()
            .map(|permit| SendPermit { permit })
            .map_err(Into::into)
    }

    /// Reserves channel capacity to send one message.
    ///
    /// This method clones the internal [`mpsc::Sender`] and returns a [`OwnedSendPermit`].
    pub fn reserve_owned(
        &self,
    ) -> impl Future<Output = Result<OwnedSendPermit<A>, SendError<()>>> + Send {
        self.tx
            .clone()
            .reserve_owned()
            .map_ok(|permit| OwnedSendPermit { permit })
            .map_err(Into::into)
    }

    /// Attempts to reserve channel capacity to send one message.
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

    /// Returns a type-erased trait object which can be downcast into a concrete
    /// [`Recipient<M>`][super::recipient::Recipient], where `M` is a specific message type chosen
    /// by the user who overrides the
    /// [`Actor::type_erased_recipient_fn`][Actor::type_erased_recipient_fn] method.
    ///
    /// This method triggers the conversion function baked in this address at construction, and
    /// returns the resulting type-erased trait object.
    #[cfg(feature = "type-erased-recipient-hook")]
    pub fn type_erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.type_erased_recipient_fn.map(|f| f(self))
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
    A: Actor + Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    M: Message,
{
    fn closed(&self) -> ClosedResultFuture<'_> {
        self.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        self.send(msg).boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.do_send(msg).boxed()
    }

    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M> {
        self.send_timeout(msg, timeout).boxed()
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.do_send_timeout(msg, timeout).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        self.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.try_do_send(msg)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        self.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.blocking_do_send(msg)
    }

    #[cfg(feature = "type-erased-recipient-hook")]
    fn type_erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.type_erased_recipient()
    }
}

impl<A, M, EP> From<Address<A>> for Recipient<M, EP>
where
    A: Actor + Handler<M> + ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
    M: Message,
{
    fn from(addr: Address<A>) -> Self {
        Self::new(Arc::new(addr))
    }
}

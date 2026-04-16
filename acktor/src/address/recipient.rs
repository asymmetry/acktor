use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[cfg(feature = "erased-recipient")]
use std::any::Any;

use futures_util::future::{FutureExt, TryFutureExt};

use super::address_impl::Address;
use super::sender::{Sender, SenderId};
use crate::actor::{Actor, ActorId};
use crate::channel::{mpsc, oneshot};
use crate::envelope::{DefaultEnvelopeProxy, FromEnvelope, ToEnvelope};
use crate::errors::{DoSendResult, DoSendResultFuture, SendResult, SendResultFuture};
use crate::message::Message;
use crate::utils::create_actor_id;

/// A type which is used to send a specific message type to an actor.
///
/// It is typed by the message type it can send, and is not tied to any specific actor type.
/// [`Recipient`]s backed by different actor types can be put in the same collection as long as
/// they can be used to send the same message type.
///
/// A `Recipient` can be converted from an [`Address`] or created with the
/// [`create`][Recipient::create] method. Note that the [`create`][Recipient::create] method is
/// only available for messages with empty [`Message::Result`].
pub struct Recipient<M, EP = DefaultEnvelopeProxy<M>>(pub Arc<dyn Sender<M, EP> + Send + Sync>)
where
    M: Message<EP>;

impl<M, EP> Debug for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("Recipient<{}>", crate::utils::type_name::<M>()))
            .field(&self.0.index())
            .finish()
    }
}

impl<M, EP> Clone for Recipient<M, EP>
where
    M: Message<EP>,
    EP: 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M, EP> PartialEq for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.index().eq(&other.0.index())
    }
}

impl<M, EP> Eq for Recipient<M, EP> where M: Message<EP> {}

impl<M, EP> Hash for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.index().hash(state)
    }
}

impl<M, EP> Recipient<M, EP>
where
    M: Message<EP>,
{
    /// Constructs a recipient from a trait object of [`Sender`].
    pub fn new(tx: Arc<dyn Sender<M, EP> + Send + Sync>) -> Self {
        Self(tx)
    }
}

impl<M> Recipient<M>
where
    M: Message<Result = ()>,
{
    /// Creates a [`mpsc::channel`], use the sender to constructs a recipient.
    ///
    /// This recipient is not backed by any actor, so it can only be used to send messages with
    /// empty [`Message::Result`].
    pub fn create(capacity: usize) -> (Self, mpsc::Receiver<M>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self(Arc::new(RecipientProxy {
                index: create_actor_id(),
                tx,
            })),
            rx,
        )
    }
}

impl<A, M, EP> From<Address<A>> for Recipient<M, EP>
where
    A: Actor,
    M: Message<EP>,
    EP: 'static,
    A::Context: ToEnvelope<A, M, EP> + FromEnvelope<A, M, EP>,
{
    fn from(addr: Address<A>) -> Self {
        Self::new(Arc::new(addr))
    }
}

impl<M, EP> SenderId for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn index(&self) -> ActorId {
        self.0.index()
    }
}

impl<M, EP> Sender<M, EP> for Recipient<M, EP>
where
    M: Message<EP>,
    EP: 'static,
{
    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    fn capacity(&self) -> usize {
        self.0.capacity()
    }

    fn send(&self, msg: M) -> SendResultFuture<'_, M, EP> {
        self.0.send(msg)
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.0.do_send(msg)
    }

    fn try_send(&self, msg: M) -> SendResult<M, EP> {
        self.0.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.0.try_do_send(msg)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M, EP> {
        self.0.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.0.blocking_do_send(msg)
    }

    #[cfg(feature = "erased-recipient")]
    fn erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        self.0.erased_recipient()
    }
}

#[derive(Debug)]
struct RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    index: ActorId,
    tx: mpsc::Sender<M>,
}

impl<M> Clone for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            tx: self.tx.clone(),
        }
    }
}

impl<M> PartialEq for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn eq(&self, other: &Self) -> bool {
        self.index.eq(&other.index)
    }
}

impl<M> Eq for RecipientProxy<M> where M: Message<Result = ()> {}

impl<M> Hash for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.index.hash(state)
    }
}

impl<M> SenderId for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn index(&self) -> ActorId {
        self.index
    }
}

impl<M> Sender<M> for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        self.tx
            .send(msg)
            .map_ok(|_| {
                // return a pre-resolved receiver to satisfy the FutureSendResult return type
                // since M::Result is (), the response is immediately available
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(());
                rx
            })
            .map_err(Into::into)
            .boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.tx.send(msg).map_err(Into::into).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        self.tx
            .try_send(msg)
            .map(|_| {
                // return a pre-resolved receiver to satisfy the SendResult return type
                // since M::Result is (), the response is immediately available
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(());
                rx
            })
            .map_err(Into::into)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.try_send(msg).map_err(Into::into)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        self.tx
            .blocking_send(msg)
            .map(|_| {
                // return a pre-resolved receiver to satisfy the SendResult return type
                // since M::Result is (), the response is immediately available
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(());
                rx
            })
            .map_err(Into::into)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.blocking_send(msg).map_err(Into::into)
    }
}

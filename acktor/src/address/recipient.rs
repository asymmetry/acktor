use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;

use futures_util::future::FutureExt;
use tokio::sync::mpsc::{
    self,
    error::{SendError, TrySendError},
};

use super::address_impl::Address;
use super::sender::{SendResult, Sender, SenderIndex, TrySendResult};
use crate::actor::Actor;
use crate::envelope::{DefaultEnvelopeProxy, FromEnvelope, ToEnvelope};
use crate::message::Message;
use crate::utils::new_address_id;

/// A type which is used to send a specific message to an actor.
///
/// This type allows you to save the sender without knowing the actor type.
pub struct Recipient<M, EP = DefaultEnvelopeProxy<M>>(pub Box<dyn Sender<M, EP> + Send + Sync>)
where
    M: Message<EP>;

impl<M, EP> fmt::Debug for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&format!(
            "Recipient<{}>",
            std::any::type_name::<M>()
                .rsplit("::")
                .next()
                .ok_or(fmt::Error)?
        ))
        .field("index", &self.0.index())
        .finish()
    }
}

impl<M, EP> Clone for Recipient<M, EP>
where
    M: Message<EP>,
    EP: 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.boxed())
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
    pub fn new(tx: Box<dyn Sender<M, EP> + Send + Sync>) -> Self {
        Self(tx)
    }
}

impl<M> Recipient<M>
where
    M: Message<Result = ()>,
{
    /// Constructs a recipient from a [`mpsc::Sender`].
    pub fn from_sender(tx: mpsc::Sender<M>) -> Self {
        Self(Box::new(RecipientProxy {
            tx,
            index: new_address_id(),
        }))
    }

    /// Creates a [`mpsc::channel`], use the sender to constructs a recipient.
    pub fn create(capacity: usize) -> (Self, mpsc::Receiver<M>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self(Box::new(RecipientProxy {
                tx,
                index: new_address_id(),
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
        Self::new(Box::new(addr))
    }
}

impl<M, EP> SenderIndex for Recipient<M, EP>
where
    M: Message<EP>,
{
    fn index(&self) -> usize {
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

    fn send(&self, msg: M) -> Pin<Box<dyn Future<Output = SendResult<M, M::Result>> + Send + '_>> {
        self.0.send(msg)
    }

    fn do_send(
        &self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + '_>> {
        self.0.do_send(msg)
    }

    fn try_send(&self, msg: M) -> TrySendResult<M, M::Result> {
        self.0.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> Result<(), TrySendError<M>> {
        self.0.try_do_send(msg)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M, M::Result> {
        self.0.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> Result<(), SendError<M>> {
        self.0.blocking_do_send(msg)
    }

    fn boxed(&self) -> Box<dyn Sender<M, EP> + Send + Sync> {
        self.0.boxed()
    }
}

#[derive(Debug)]
struct RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    index: usize,
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

impl<M> SenderIndex for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn index(&self) -> usize {
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

    fn send(&self, _: M) -> Pin<Box<dyn Future<Output = SendResult<M, M::Result>> + Send>> {
        unimplemented!("recipient not link to an actor should not be used to expect a response");
    }

    fn do_send(
        &self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + '_>> {
        self.tx.send(msg).boxed()
    }

    fn try_send(&self, _: M) -> TrySendResult<M, M::Result> {
        unimplemented!("recipient not link to an actor should not be used to expect a response");
    }

    fn try_do_send(&self, msg: M) -> Result<(), TrySendError<M>> {
        self.tx.try_send(msg)
    }

    fn blocking_send(&self, _: M) -> SendResult<M, M::Result> {
        unimplemented!("recipient not link to an actor should not be used to expect a response");
    }

    fn blocking_do_send(&self, msg: M) -> Result<(), SendError<M>> {
        self.tx.blocking_send(msg)
    }

    fn boxed(&self) -> Box<dyn Sender<M> + Send + Sync> {
        Box::new(self.clone())
    }
}

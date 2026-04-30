use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use futures_util::{FutureExt, TryFutureExt};
use tokio::time::Duration;

use super::next_actor_id;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderId,
};
use crate::actor::ActorId;
#[cfg(feature = "type-erased-recipient-hook")]
use crate::actor::TypeErasedRecipient;
use crate::channel::{mpsc, oneshot};
use crate::envelope::DefaultEnvelopeProxy;
use crate::message::Message;
use crate::utils::ShortName;

/// A type which is used to send a specific message type to an actor.
///
/// It is typed by the message type it can send, and is not tied to any specific actor type.
/// [`Recipient`]s backed by different actor types can be put in the same collection as long as
/// they can be used to send the same message type.
///
/// A `Recipient` can be converted from an [`Address`][super::Address] or created with the
/// [`create`][Recipient::create] method. Note that the [`create`][Recipient::create] method is
/// only available for messages with empty [`Message::Result`].
pub struct Recipient<M, EP = DefaultEnvelopeProxy<M>>(Arc<dyn Sender<M, EP> + Send + Sync>)
where
    M: Message;

impl<M, EP> Debug for Recipient<M, EP>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("Recipient<{}>", ShortName::of::<M>()))
            .field(&self.0.index())
            .finish()
    }
}

impl<M, EP> Clone for Recipient<M, EP>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M, EP> PartialEq for Recipient<M, EP>
where
    M: Message,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.index().eq(&other.0.index())
    }
}

impl<M, EP> Eq for Recipient<M, EP> where M: Message {}

impl<M, EP> Hash for Recipient<M, EP>
where
    M: Message,
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
    M: Message,
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
                index: next_actor_id(),
                tx,
            })),
            rx,
        )
    }
}

impl<M, EP> SenderId for Recipient<M, EP>
where
    M: Message,
{
    fn index(&self) -> ActorId {
        self.0.index()
    }
}

impl<M, EP> Sender<M, EP> for Recipient<M, EP>
where
    M: Message,
{
    fn closed(&self) -> EmptyFuture<'_> {
        self.0.closed()
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    fn capacity(&self) -> usize {
        self.0.capacity()
    }

    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        self.0.send(msg)
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.0.do_send(msg)
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        self.0.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.0.try_do_send(msg)
    }

    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M> {
        self.0.send_timeout(msg, timeout)
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.0.do_send_timeout(msg, timeout)
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        self.0.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.0.blocking_do_send(msg)
    }

    #[cfg(feature = "type-erased-recipient-hook")]
    fn type_erased_recipient(&self) -> Option<TypeErasedRecipient> {
        self.0.type_erased_recipient()
    }
}

#[derive(Debug)]
struct RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    index: u64,
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

impl<M> RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    const fn index(&self) -> ActorId {
        ActorId::new(self.index)
    }
}

impl<M> SenderId for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn index(&self) -> ActorId {
        self.index()
    }
}

impl<M> Sender<M> for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn closed(&self) -> EmptyFuture<'_> {
        self.tx.closed().boxed()
    }

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

    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M> {
        self.tx
            .send_timeout(msg, timeout)
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

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.tx
            .send_timeout(msg, timeout)
            .map_err(Into::into)
            .boxed()
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

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::test_utils::Ping;

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_recipient_proxy() {
        let (tx, _rx) = mpsc::channel::<Ping>(1);
        let proxy = RecipientProxy {
            index: next_actor_id(),
            tx,
        };

        // clone preserves index and tx — clones compare equal
        let clone = proxy.clone();
        assert_eq!(proxy, clone);
        assert_eq!(proxy.index(), clone.index());
        assert_eq!(hash_of(&proxy), hash_of(&clone));

        // distinct proxies with different indices are not equal
        let (tx2, _rx2) = mpsc::channel::<Ping>(1);
        let other = RecipientProxy {
            index: next_actor_id(),
            tx: tx2,
        };
        assert_ne!(proxy, other);
        assert_ne!(proxy.index(), other.index());
    }
}

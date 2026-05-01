use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use futures_util::{FutureExt, TryFutureExt};
use tokio::time::Duration;

use super::next_actor_id;
#[cfg(feature = "ipc")]
use super::remote::{RemoteProxy, RemoteRecipient};
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderMeta,
};
use crate::actor::ActorId;
#[cfg(feature = "ipc")]
use crate::actor::RemoteAccessibleActorHandle;
use crate::channel::{mpsc, oneshot};
#[cfg(feature = "ipc")]
use crate::codec::{Decode, Encode};
use crate::envelope::DefaultEnvelopeProxy;
use crate::message::Message;
#[cfg(feature = "ipc")]
use crate::message::MessageId;
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

#[cfg(feature = "ipc")]
impl<M, EP> Recipient<M, EP>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    /// Constructs a recipient from a [`RemoteProxy`].
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub fn new_remote(index: u64, proxy: Arc<dyn RemoteProxy + Send + Sync>) -> Self {
        RemoteRecipient::new(index, proxy).into()
    }
}

impl<M, EP> SenderMeta for Recipient<M, EP>
where
    M: Message,
{
    fn index(&self) -> ActorId {
        self.0.index()
    }

    fn closed(&self) -> EmptyFuture<'_> {
        self.0.closed()
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[cfg(feature = "ipc")]
    fn remote_accessible_actor_handle(&self) -> Option<RemoteAccessibleActorHandle> {
        self.0.remote_accessible_actor_handle()
    }
}

impl<M, EP> Sender<M, EP> for Recipient<M, EP>
where
    M: Message,
{
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
}

struct RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    index: u64,
    tx: mpsc::Sender<M>,
}

impl<M> Debug for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.index())
            .finish()
    }
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
        self.index().eq(&other.index())
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
        self.index().hash(state)
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

impl<M> SenderMeta for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
    fn index(&self) -> ActorId {
        self.index()
    }

    fn closed(&self) -> EmptyFuture<'_> {
        self.tx.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn capacity(&self) -> usize {
        self.tx.capacity()
    }
}

impl<M> Sender<M> for RecipientProxy<M>
where
    M: Message<Result = ()>,
{
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
    use anyhow::{Context as _, Result};
    use pretty_assertions::assert_eq;
    use tokio::time::{self, Duration};

    use super::*;
    use crate::channel::mpsc;
    use crate::test_utils::{Ping, hash_of, make_address};

    #[tokio::test]
    async fn test_recipient() -> Result<()> {
        // create() delivers to the receiver
        let (recipient, mut rx) = Recipient::<Ping>::create(4);
        recipient.do_send(Ping(1)).await?;
        let msg = rx.recv().await?;
        assert_eq!(msg.0, 1);

        // clone + eq + hash
        let clone = recipient.clone();
        assert_eq!(recipient, clone);
        assert_eq!(recipient.index(), clone.index());
        assert_eq!(hash_of(&recipient), hash_of(&clone));

        // capacity + is_closed + closed
        assert_eq!(recipient.capacity(), 4);
        assert!(!recipient.is_closed());
        drop(rx);
        assert!(recipient.is_closed());
        time::timeout(Duration::from_millis(500), recipient.closed())
            .await
            .context("closed() should resolve after receiver drop")?;

        // send functions
        let (recipient, rx) = Recipient::<Ping>::create(8);
        recipient.send(Ping(2)).await?;
        recipient.do_send(Ping(3)).await?;
        recipient.try_send(Ping(4))?;
        recipient.try_do_send(Ping(5))?;
        recipient
            .send_timeout(Ping(6), Duration::from_millis(10))
            .await?;
        recipient
            .do_send_timeout(Ping(7), Duration::from_millis(10))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            recipient.blocking_send(Ping(8))?;
            recipient.blocking_do_send(Ping(9))?;
            Ok(())
        })
        .await??;
        assert_eq!(rx.len(), 8);

        // From<Address> delivers to the mailbox
        let (a1, m1) = make_address(4);
        let index = a1.index();
        let r1: Recipient<Ping> = a1.into();
        assert_eq!(r1.index(), index);

        // clone + eq + hash
        let clone = r1.clone();
        assert_eq!(r1, clone);
        assert_eq!(r1.index(), clone.index());
        assert_eq!(hash_of(&r1), hash_of(&clone));

        // capacity + is_closed + closed
        assert_eq!(r1.capacity(), 4);
        assert!(!r1.is_closed());
        drop(m1);
        assert!(r1.is_closed());
        time::timeout(Duration::from_millis(500), r1.closed())
            .await
            .context("closed() should resolve after mailbox drop")?;

        // send functions
        let (a1, m1) = make_address(8);
        let r1: Recipient<Ping> = a1.into();
        r1.send(Ping(10)).await?;
        r1.do_send(Ping(11)).await?;
        r1.try_send(Ping(12))?;
        r1.try_do_send(Ping(13))?;
        r1.send_timeout(Ping(14), Duration::from_millis(10)).await?;
        r1.do_send_timeout(Ping(15), Duration::from_millis(10))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            r1.blocking_send(Ping(16))?;
            r1.blocking_do_send(Ping(17))?;
            Ok(())
        })
        .await??;
        assert_eq!(m1.len(), 8);

        Ok(())
    }

    #[test]
    fn test_recipient_proxy() {
        let (tx, _rx) = mpsc::channel::<Ping>(1);
        let proxy = RecipientProxy {
            index: next_actor_id(),
            tx,
        };

        // clone + eq + hash
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

    #[test]
    fn test_debug_fmt() {
        let (recipient, _rx) = Recipient::<Ping>::create(4);
        assert_eq!(
            format!("{:?}", recipient),
            format!("Recipient<Ping>({})", recipient.index())
        );

        let (tx, _rx) = mpsc::channel::<Ping>(1);
        let proxy = RecipientProxy {
            index: next_actor_id(),
            tx,
        };
        assert_eq!(
            format!("{:?}", proxy),
            format!("RecipientProxy<Ping>({})", proxy.index())
        );
    }
}

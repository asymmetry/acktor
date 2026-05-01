use std::fmt::{self, Debug};
use std::sync::Arc;

use futures_util::{FutureExt, TryFutureExt};
use tokio::time::Duration;

use super::next_actor_id;
use super::permit::{OwnedSendPermit, SendPermit};
use super::recipient::Recipient;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderMeta,
};
#[cfg(feature = "ipc")]
use crate::actor::RemoteAccessibleActorHandle;
use crate::actor::{Actor, ActorId};
use crate::channel::{mpsc, oneshot};
use crate::envelope::{Envelope, FromEnvelope, IntoEnvelope};
use crate::error::SendError;
use crate::message::Message;
use crate::utils::ShortName;

/// The address of an actor located in the current process.
pub struct LocalAddress<A>
where
    A: Actor,
{
    index: u64,
    tx: mpsc::Sender<Envelope<A>>,
}

impl<A> Debug for LocalAddress<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.index())
            .finish()
    }
}

impl<A> Clone for LocalAddress<A>
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

impl<A> LocalAddress<A>
where
    A: Actor,
{
    /// Constructs a new [`LocalAddress`] from a [`mpsc::Sender`].
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self {
            index: next_actor_id(),
            tx,
        }
    }

    /// Returns the index of the address.
    pub const fn index(&self) -> ActorId {
        ActorId::new(self.index)
    }

    /// Completes when the mailbox of the actor has been closed.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + '_ {
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
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub fn send<M, EP>(&self, msg: M) -> impl Future<Output = SendResult<M>> + Send + '_
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(msg.pack(Some(tx)))
            .map_ok(|_| rx)
            .map_err(|e| SendError::Closed(M::unpack(e.0)))
    }

    /// Sends a message, waiting until there is capacity, without expecting a response.
    pub fn do_send<M, EP>(&self, msg: M) -> impl Future<Output = DoSendResult<M>> + Send + '_
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        self.tx
            .send(msg.pack(None))
            .map_err(|e| SendError::Closed(M::unpack(e.0)))
    }

    /// Attempts to immediately send a message and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub fn try_send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(msg.pack(Some(tx)))
            .map(|_| rx)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Closed(envelope) => {
                    SendError::Closed(M::unpack(envelope))
                }
                mpsc::error::TrySendError::Full(envelope) => SendError::Full(M::unpack(envelope)),
            })
    }

    /// Attempts to immediately send a message without expecting a response.
    pub fn try_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        self.tx.try_send(msg.pack(None)).map_err(|e| match e {
            mpsc::error::TrySendError::Closed(envelope) => SendError::Closed(M::unpack(envelope)),
            mpsc::error::TrySendError::Full(envelope) => SendError::Full(M::unpack(envelope)),
        })
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub fn send_timeout<M, EP>(
        &self,
        msg: M,
        timeout: Duration,
    ) -> impl Future<Output = SendResult<M>> + Send + '_
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_timeout(msg.pack(Some(tx)), timeout)
            .map_ok(|_| rx)
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Closed(envelope) => {
                    SendError::Closed(M::unpack(envelope))
                }
                mpsc::error::SendTimeoutError::Timeout(envelope) => {
                    SendError::Timeout(M::unpack(envelope))
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
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        self.tx
            .send_timeout(msg.pack(None), timeout)
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Closed(envelope) => {
                    SendError::Closed(M::unpack(envelope))
                }
                mpsc::error::SendTimeoutError::Timeout(envelope) => {
                    SendError::Timeout(M::unpack(envelope))
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
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .blocking_send(msg.pack(Some(tx)))
            .map(|_| rx)
            .map_err(|e| SendError::Closed(M::unpack(e.0)))
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
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        self.tx
            .blocking_send(msg.pack(None))
            .map_err(|e| SendError::Closed(M::unpack(e.0)))
    }

    /// Reserves channel capacity to send one message.
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
    ) -> impl Future<Output = Result<OwnedSendPermit<A>, SendError<()>>> + Send + '_ {
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

    /// Returns a [`RemoteAccessibleActorHandle`], if `A` is a remote accessible actor.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    fn remote_accessible_actor_handle(&self) -> Option<RemoteAccessibleActorHandle> {
        A::remote_accessible_actor_handle()
    }
}

impl<A> SenderMeta for LocalAddress<A>
where
    A: Actor,
{
    fn index(&self) -> ActorId {
        self.index()
    }

    fn closed(&self) -> EmptyFuture<'_> {
        self.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }

    #[cfg(feature = "ipc")]
    fn remote_accessible_actor_handle(&self) -> Option<RemoteAccessibleActorHandle> {
        self.remote_accessible_actor_handle()
    }
}

impl<A, M, EP> Sender<M, EP> for LocalAddress<A>
where
    A: Actor,
    M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
{
    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        self.send(msg).boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.do_send(msg).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        self.try_send(msg)
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.try_do_send(msg)
    }

    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M> {
        self.send_timeout(msg, timeout).boxed()
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.do_send_timeout(msg, timeout).boxed()
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        self.blocking_send(msg)
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.blocking_do_send(msg)
    }
}

impl<A, M, EP> From<LocalAddress<A>> for Recipient<M, EP>
where
    A: Actor,
    M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
{
    fn from(addr: LocalAddress<A>) -> Self {
        Self::new(Arc::new(addr))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Context as _, Result};
    use pretty_assertions::assert_eq;
    use tokio::time::{self, Duration};

    use super::super::Mailbox;
    use super::*;
    use crate::channel::mpsc;
    use crate::envelope::Envelope;
    use crate::error::SendError;
    use crate::test_utils::{Dummy, Ping, hash_of};

    fn make_address(capacity: usize) -> (LocalAddress<Dummy>, Mailbox<Dummy>) {
        let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
        (LocalAddress::new(tx), Mailbox::new(rx))
    }

    #[tokio::test]
    async fn test_address() -> Result<()> {
        // index is unique
        let (a1, _) = make_address(4);
        let (a2, _) = make_address(4);
        assert_ne!(a1.index(), a2.index());

        // clone
        let (a1, m1) = make_address(4);
        let clone = a1.clone();
        assert_eq!(a1.index(), clone.index());

        // capacity + is_closed + closed
        assert_eq!(a1.capacity(), 4);
        assert!(!a1.is_closed());
        drop(m1);
        assert!(a1.is_closed());
        time::timeout(Duration::from_millis(500), a1.closed())
            .await
            .context("closed() should resolve after mailbox drop")?;

        // send
        let (a1, m1) = make_address(1);
        a1.send(Ping(1)).await?;
        assert_eq!(m1.len(), 1);

        // do_send
        let (a1, m1) = make_address(1);
        a1.do_send(Ping(2)).await?;
        assert_eq!(m1.len(), 1);

        // try_send
        let (a1, m1) = make_address(1);
        a1.try_send(Ping(3))?;
        assert_eq!(m1.len(), 1);

        let result = a1.try_send(Ping(4));
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );
        drop(m1);

        let result = a1.try_send(Ping(5));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // try_do_send
        let (a1, m1) = make_address(1);
        a1.try_do_send(Ping(6))?;
        assert_eq!(m1.len(), 1);

        let result = a1.try_do_send(Ping(7));
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );

        drop(m1);
        let result = a1.try_do_send(Ping(8));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // send_timeout
        let (a1, m1) = make_address(1);
        a1.send_timeout(Ping(9), Duration::from_millis(10)).await?;
        assert_eq!(m1.len(), 1);

        let result = a1.send_timeout(Ping(10), Duration::from_millis(10)).await;
        assert!(
            matches!(result, Err(SendError::Timeout(_))),
            "expected Timeout, got {result:?}"
        );

        drop(m1);
        let result = a1.send_timeout(Ping(11), Duration::from_millis(10)).await;
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // do_send_timeout
        let (a1, m1) = make_address(1);
        a1.do_send_timeout(Ping(12), Duration::from_millis(10))
            .await?;
        assert_eq!(m1.len(), 1);

        let result = a1
            .do_send_timeout(Ping(13), Duration::from_millis(10))
            .await;
        assert!(
            matches!(result, Err(SendError::Timeout(_))),
            "expected Timeout, got {result:?}"
        );

        drop(m1);
        let result = a1
            .do_send_timeout(Ping(14), Duration::from_millis(10))
            .await;
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // blocking_send
        let (a1, m1) = make_address(1);
        tokio::task::spawn_blocking(move || -> Result<()> {
            a1.blocking_send(Ping(15))?;
            assert_eq!(m1.len(), 1);
            Ok(())
        })
        .await??;

        // blocking_do_send
        let (a1, m1) = make_address(1);
        tokio::task::spawn_blocking(move || -> Result<()> {
            a1.blocking_do_send(Ping(16))?;
            assert_eq!(m1.len(), 1);
            Ok(())
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_recipient() -> Result<()> {
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
    fn test_debug_fmt() {
        let (address, _) = make_address(4);
        assert_eq!(
            format!("{:?}", address),
            format!("LocalAddress<Dummy>({})", address.index())
        );
    }
}

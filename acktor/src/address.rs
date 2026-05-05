//! Traits and type definitions for actor address.
//!
//! In the actor model, an [`Address`] is a handle to an actor. It is the only way to interact
//! with an actor since the runtime will take the ownership of the actor itself after it is
//! spawned.
//!
//! This module defines the [`Address`] type for an actor. It also provides the [`Sender`] trait
//! and a [`Recipient`] type which are alternative ways to organize the addresses of actors.
//!

use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures_util::FutureExt;
use tokio::time::Duration;

use crate::actor::{Actor, ActorId};
use crate::channel::mpsc;
use crate::envelope::{Envelope, FromEnvelope, IntoEnvelope};
use crate::error::SendError;
use crate::message::Message;
use crate::utils::ShortName;
#[cfg(feature = "ipc")]
use crate::{actor::RemoteAddressable, message::BinaryMessage};

mod local;
use local::LocalAddress;

#[cfg(feature = "ipc")]
mod remote;
#[cfg(feature = "ipc")]
use remote::RemoteAddress;
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use remote::RemoteProxy;

mod permit;
pub use permit::{OwnedSendPermit, SendPermit};

mod sender;
pub use sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderInfo,
};

mod recipient;
pub use recipient::Recipient;

mod mailbox;
pub use mailbox::Mailbox;

static INDEX_GENERATOR: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn next_actor_id() -> u64 {
    INDEX_GENERATOR.fetch_add(1, Ordering::Relaxed)
}

/// A remote mailbox which can be used by the runtime to deliver binary messages to a remote
/// addressable actor.
///
/// It is an alias of the address of the actor in the form of a `Recipient<BinaryMessage>`.
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub type RemoteMailbox = Recipient<BinaryMessage>;

enum Inner<A>
where
    A: Actor,
{
    Local(LocalAddress<A>),
    #[cfg(feature = "ipc")]
    Remote(RemoteAddress),
}

impl<A> Clone for Inner<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        match self {
            Self::Local(address) => Self::Local(address.clone()),
            #[cfg(feature = "ipc")]
            Self::Remote(address) => Self::Remote(address.clone()),
        }
    }
}

/// The address of an actor.
///
/// It is used to send messages to an actor.
#[repr(transparent)]
pub struct Address<A>(Inner<A>)
where
    A: Actor;

impl<A> Debug for Address<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.index())
            .finish()
    }
}

impl<A> Clone for Address<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A> PartialEq for Address<A>
where
    A: Actor,
{
    fn eq(&self, other: &Self) -> bool {
        self.index().eq(&other.index())
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
        self.index().hash(state);
    }
}

impl<A> Address<A>
where
    A: Actor,
{
    /// Constructs a new local [`Address`] from a [`mpsc::Sender`].
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self(Inner::Local(LocalAddress::new(tx)))
    }

    /// Constructs a new remote [`Address`] from a [`RemoteProxy`].
    ///
    /// The `index` parameter is used to identify the actor in another process, usually it is the
    /// index part of its [`ActorId`] in its own process.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub fn new_remote(index: u64, proxy: Arc<dyn RemoteProxy + Send + Sync>) -> Self
    where
        A: RemoteAddressable,
    {
        Self(Inner::Remote(RemoteAddress::new(
            index,
            A::codec_table(),
            proxy,
        )))
    }

    /// Returns the index of the address.
    pub const fn index(&self) -> ActorId {
        match &self.0 {
            Inner::Local(address) => address.index(),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.index(),
        }
    }

    /// Completes when the mailbox of the actor has been closed.
    pub async fn closed(&self) {
        match &self.0 {
            Inner::Local(address) => address.closed().await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.closed().await,
        }
    }

    /// Checks if the mailbox of the actor is closed.
    pub fn is_closed(&self) -> bool {
        match &self.0 {
            Inner::Local(address) => address.is_closed(),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.is_closed(),
        }
    }

    /// Returns the current capacity of the mailbox of the actor.
    pub fn capacity(&self) -> usize {
        match &self.0 {
            Inner::Local(address) => address.capacity(),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.capacity(),
        }
    }

    /// Sends a message, waiting until there is capacity, and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    pub async fn send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.send(msg).await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.send(msg).await,
        }
    }

    /// Sends a message, waiting until there is capacity, without expecting a response.
    pub async fn do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.do_send(msg).await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.do_send(msg).await,
        }
    }

    /// Attempts to immediately send a message and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    pub fn try_send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.try_send(msg),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.try_send(msg),
        }
    }

    /// Attempts to immediately send a message without expecting a response.
    pub fn try_do_send<M, EP>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.try_do_send(msg),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.try_do_send(msg),
        }
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    pub async fn send_timeout<M, EP>(&self, msg: M, timeout: Duration) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.send_timeout(msg, timeout).await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.send_timeout(msg, timeout).await,
        }
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, without
    /// expecting a response.
    pub async fn do_send_timeout<M, EP>(&self, msg: M, timeout: Duration) -> DoSendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => address.do_send_timeout(msg, timeout).await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.do_send_timeout(msg, timeout).await,
        }
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
        match &self.0 {
            Inner::Local(address) => address.blocking_send(msg),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.blocking_send(msg),
        }
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
        match &self.0 {
            Inner::Local(address) => address.blocking_do_send(msg),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.blocking_do_send(msg),
        }
    }

    /// Reserves channel capacity to send one message.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub async fn reserve(&self) -> Result<SendPermit<'_, A>, SendError<()>> {
        match &self.0 {
            Inner::Local(address) => address.reserve().await,
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => Err(SendError::other(
                "remote address does not support reserve",
                (),
            )),
        }
    }

    /// Attempts to reserve channel capacity to send one message.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub fn try_reserve(&self) -> Result<SendPermit<'_, A>, SendError<()>> {
        match &self.0 {
            Inner::Local(address) => address.try_reserve(),
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => Err(SendError::other(
                "remote address does not support reserve",
                (),
            )),
        }
    }

    /// Reserves channel capacity to send one message.
    ///
    /// This method clones the internal [`mpsc::Sender`] and returns a [`OwnedSendPermit`].
    pub async fn reserve_owned(&self) -> Result<OwnedSendPermit<A>, SendError<()>> {
        match &self.0 {
            Inner::Local(address) => address.reserve_owned().await,
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => Err(SendError::other(
                "remote address does not support reserve",
                (),
            )),
        }
    }

    /// Attempts to reserve channel capacity to send one message.
    ///
    /// This method clones the internal [`mpsc::Sender`] and returns a [`OwnedSendPermit`].
    pub fn try_reserve_owned(&self) -> Result<OwnedSendPermit<A>, SendError<()>> {
        match &self.0 {
            Inner::Local(address) => address.try_reserve_owned(),
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => Err(SendError::other(
                "remote address does not support reserve",
                (),
            )),
        }
    }

    /// Returns a [`RemoteMailbox`], if the actor is a remote addressable actor, and this address
    /// is a local address.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    fn remote_addressable(&self) -> Option<RemoteMailbox> {
        match &self.0 {
            Inner::Local(_) => A::remote_mailbox(self.clone()),
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => None,
        }
    }
}

impl<A> SenderInfo for Address<A>
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
    fn remote_mailbox(&self) -> Option<RemoteMailbox> {
        self.remote_addressable()
    }
}

impl<A, M, EP> Sender<M, EP> for Address<A>
where
    A: Actor,
    M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    EP: 'static,
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

impl<A> From<LocalAddress<A>> for Address<A>
where
    A: Actor,
{
    fn from(addr: LocalAddress<A>) -> Self {
        Self(Inner::Local(addr))
    }
}

#[cfg(feature = "ipc")]
impl<A> From<RemoteAddress> for Address<A>
where
    A: Actor,
{
    fn from(addr: RemoteAddress) -> Self {
        Self(Inner::Remote(addr))
    }
}

impl<A, M, EP> From<Address<A>> for Recipient<M, EP>
where
    A: Actor,
    M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    EP: 'static,
{
    fn from(addr: Address<A>) -> Self {
        Self::new(Arc::new(addr))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Context as _, Result};
    use pretty_assertions::{assert_eq, assert_ne};
    use tokio::time::{self, Duration};

    #[cfg(feature = "ipc")]
    use super::SenderInfo;
    use crate::test_utils::{Ping, hash_of, make_address};

    #[tokio::test]
    async fn test_address() -> Result<()> {
        // index is unique
        let (a1, _) = make_address(4);
        let (a2, _) = make_address(4);
        assert_ne!(a1, a2);
        assert_ne!(a1.index(), a2.index());
        #[cfg(feature = "ipc")]
        assert!(!a1.is_remote());

        // clone + eq + hash
        let (a1, m1) = make_address(4);
        let clone = a1.clone();
        assert_eq!(a1, clone);
        assert_eq!(a1.index(), clone.index());
        assert_eq!(hash_of(&a1), hash_of(&clone));

        // capacity + is_closed + closed
        assert_eq!(a1.capacity(), 4);
        assert!(!a1.is_closed());
        drop(m1);
        assert!(a1.is_closed());
        time::timeout(Duration::from_millis(500), a1.closed())
            .await
            .context("closed() should resolve after mailbox drop")?;

        // send functions
        let (a1, m1) = make_address(8);
        a1.send(Ping(1)).await?;
        a1.do_send(Ping(2)).await?;
        a1.try_send(Ping(3))?;
        a1.try_do_send(Ping(4))?;
        a1.send_timeout(Ping(5), Duration::from_millis(100)).await?;
        a1.do_send_timeout(Ping(6), Duration::from_millis(100))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            a1.blocking_send(Ping(7))?;
            a1.blocking_do_send(Ping(8))?;
            Ok(())
        })
        .await??;
        assert_eq!(m1.len(), 8);

        Ok(())
    }

    #[cfg(feature = "ipc")]
    #[tokio::test]
    async fn test_remote_address() -> Result<()> {
        use std::num::NonZeroU64;

        use crate::actor::ActorId;
        use crate::error::SendError;
        use crate::test_utils::{Dummy, DummyProxy};

        let proxy = DummyProxy::new();
        let address = super::Address::<Dummy>::new_remote(7, proxy.clone());

        // index reflects the remote (local_index, proxy_index) pair
        assert_eq!(
            address.index(),
            ActorId::new_remote(7, NonZeroU64::new(42).unwrap())
        );
        assert!(address.is_remote());

        // clone + eq + hash
        let clone = address.clone();
        assert_eq!(address, clone);
        assert_eq!(address.index(), clone.index());
        assert_eq!(hash_of(&address), hash_of(&clone));

        // capacity + is_closed + closed
        assert_eq!(address.capacity(), usize::MAX);
        assert!(!address.is_closed());
        time::timeout(Duration::from_millis(500), address.closed()).await?;

        // send functions
        address.send(Ping(1)).await?.await?;
        address.do_send(Ping(2)).await?;
        address.try_send(Ping(3))?.await?;
        address.try_do_send(Ping(4))?;
        address
            .send_timeout(Ping(5), Duration::from_millis(100))
            .await?
            .await?;
        address
            .do_send_timeout(Ping(6), Duration::from_millis(100))
            .await?;
        let address = tokio::task::spawn_blocking(move || -> Result<_> {
            address.blocking_send(Ping(7))?.blocking_recv()?;
            address.blocking_do_send(Ping(8))?;
            Ok(address)
        })
        .await??;

        // reserve* are not supported on remote addresses.
        assert!(matches!(address.reserve().await, Err(SendError::Other(..))));
        assert!(matches!(address.try_reserve(), Err(SendError::Other(..))));
        assert!(matches!(
            address.reserve_owned().await,
            Err(SendError::Other(..))
        ));
        assert!(matches!(
            address.try_reserve_owned(),
            Err(SendError::Other(..))
        ));

        // remote addresses do not expose a `RemoteMailbox`.
        assert!(address.remote_addressable().is_none());

        Ok(())
    }

    #[test]
    fn test_debug_fmt() {
        let (address, _) = make_address(4);
        assert_eq!(
            format!("{:?}", address),
            format!("Address<Dummy>({})", address.index())
        );
    }
}

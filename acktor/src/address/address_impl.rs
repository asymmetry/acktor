use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
#[cfg(feature = "ipc")]
use std::num::NonZeroU64;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::time::Duration;

use super::next_actor_id;
use super::permit::{OwnedSendPermit, SendPermit};
use super::recipient::Recipient;
#[cfg(feature = "ipc")]
use super::remote::RemoteAddress;
#[cfg(feature = "ipc")]
use super::remote_proxy::RemoteProxy;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderId,
};
use crate::actor::{Actor, ActorId};
#[cfg(feature = "type-erased-recipient-hook")]
use crate::actor::{AddressToTypeErasedRecipientFn, TypeErasedRecipient};
use crate::channel::{mpsc, oneshot};
#[cfg(feature = "ipc")]
use crate::codec::HasCodecTable;
use crate::envelope::{Envelope, FromEnvelope, IntoEnvelope};
use crate::error::SendError;
use crate::message::Message;
use crate::utils::ShortName;

pub struct LocalAddress<A>
where
    A: Actor,
{
    index: u64,
    tx: mpsc::Sender<Envelope<A>>,
    #[cfg(feature = "type-erased-recipient-hook")]
    into_type_erased_recipient: Option<AddressToTypeErasedRecipientFn<A>>,
}

impl<A> Clone for LocalAddress<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            tx: self.tx.clone(),
            #[cfg(feature = "type-erased-recipient-hook")]
            into_type_erased_recipient: self.into_type_erased_recipient,
        }
    }
}

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
        f.debug_tuple(&format!("{}", ShortName::of::<Self>()))
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
    /// Constructs a new [`Address`] from a [`mpsc::Sender`].
    ///
    /// Triggers [`Actor::type_erased_recipient_hook`] once (if the feature
    /// `type-erased-recipient-hook` is enabled) and stores the result.
    pub fn new(tx: mpsc::Sender<Envelope<A>>) -> Self {
        Self(Inner::Local(LocalAddress {
            index: next_actor_id(),
            tx,
            #[cfg(feature = "type-erased-recipient-hook")]
            into_type_erased_recipient: A::type_erased_recipient_hook(),
        }))
    }

    /// Constructs a new [`Address`] of a remote actor.
    ///
    /// The `index` parameter is used to identify the remote actor, which is usually the local
    /// actor id of the remote actor in the remote process. The `remote_index` parameter is used
    /// to identify the remote session which the remote actor belongs to.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub fn new_remote(
        index: u64,
        remote_index: NonZeroU64,
        proxy: Arc<dyn RemoteProxy + Send + Sync>,
    ) -> Self
    where
        A: HasCodecTable,
    {
        Self(Inner::Remote(RemoteAddress::new(
            ActorId::new_remote(index, remote_index),
            proxy,
            <A as HasCodecTable>::codec_table(),
        )))
    }

    /// Returns the index of the address.
    pub const fn index(&self) -> ActorId {
        match &self.0 {
            Inner::Local(address) => ActorId::new(address.index),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.index(),
        }
    }

    /// Completes when the mailbox of the actor has been closed.
    pub async fn closed(&self) {
        match &self.0 {
            Inner::Local(address) => address.tx.closed().await,
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.closed().await,
        }
    }

    /// Checks if the mailbox of the actor is closed.
    pub fn is_closed(&self) -> bool {
        match &self.0 {
            Inner::Local(address) => address.tx.is_closed(),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.is_closed(),
        }
    }

    /// Returns the current capacity of the mailbox of the actor.
    pub fn capacity(&self) -> usize {
        match &self.0 {
            Inner::Local(address) => address.tx.capacity(),
            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.capacity(),
        }
    }

    /// Sends a message, waiting until there is capacity, and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub async fn send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => {
                let (result_tx, result_rx) = oneshot::channel();

                address
                    .tx
                    .send(msg.pack(Some(result_tx)))
                    .await
                    .map_err(|e| SendError::Closed(M::unpack(e.0)))?;

                Ok(result_rx)
            }

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
            Inner::Local(address) => address
                .tx
                .send(msg.pack(None))
                .await
                .map_err(|e| SendError::Closed(M::unpack(e.0))),

            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.do_send(msg).await,
        }
    }

    /// Attempts to immediately send a message and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub fn try_send<M, EP>(&self, msg: M) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => {
                let (result_tx, result_rx) = oneshot::channel();

                address
                    .tx
                    .try_send(msg.pack(Some(result_tx)))
                    .map(|_| result_rx)
                    .map_err(|e| match e {
                        mpsc::error::TrySendError::Closed(envelope) => {
                            SendError::Closed(M::unpack(envelope))
                        }
                        mpsc::error::TrySendError::Full(envelope) => {
                            SendError::Full(M::unpack(envelope))
                        }
                    })
            }

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
            Inner::Local(address) => address.tx.try_send(msg.pack(None)).map_err(|e| match e {
                mpsc::error::TrySendError::Closed(envelope) => {
                    SendError::Closed(M::unpack(envelope))
                }
                mpsc::error::TrySendError::Full(envelope) => SendError::Full(M::unpack(envelope)),
            }),

            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.try_do_send(msg),
        }
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub async fn send_timeout<M, EP>(&self, msg: M, timeout: Duration) -> SendResult<M>
    where
        M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    {
        match &self.0 {
            Inner::Local(address) => {
                let (result_tx, result_rx) = oneshot::channel();

                address
                    .tx
                    .send_timeout(msg.pack(Some(result_tx)), timeout)
                    .await
                    .map_err(|e| match e {
                        mpsc::error::SendTimeoutError::Closed(envelope) => {
                            SendError::Closed(M::unpack(envelope))
                        }
                        mpsc::error::SendTimeoutError::Timeout(envelope) => {
                            SendError::Timeout(M::unpack(envelope))
                        }
                    })?;

                Ok(result_rx)
            }

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
            Inner::Local(address) => address
                .tx
                .send_timeout(msg.pack(None), timeout)
                .await
                .map_err(|e| match e {
                    mpsc::error::SendTimeoutError::Closed(envelope) => {
                        SendError::Closed(M::unpack(envelope))
                    }
                    mpsc::error::SendTimeoutError::Timeout(envelope) => {
                        SendError::Timeout(M::unpack(envelope))
                    }
                }),

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
            Inner::Local(address) => {
                let (result_tx, result_rx) = oneshot::channel();

                address
                    .tx
                    .blocking_send(msg.pack(Some(result_tx)))
                    .map_err(|e| SendError::Closed(M::unpack(e.0)))?;

                Ok(result_rx)
            }

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
            Inner::Local(address) => address
                .tx
                .blocking_send(msg.pack(None))
                .map_err(|e| SendError::Closed(M::unpack(e.0))),

            #[cfg(feature = "ipc")]
            Inner::Remote(address) => address.blocking_do_send(msg),
        }
    }

    /// Reserves channel capacity to send one message.
    ///
    /// This method borrows the internal [`mpsc::Sender`] and returns a [`SendPermit`].
    pub async fn reserve(&self) -> Result<SendPermit<'_, A>, SendError<()>> {
        match &self.0 {
            Inner::Local(address) => address
                .tx
                .reserve()
                .await
                .map(|permit| SendPermit { permit })
                .map_err(Into::into),

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
            Inner::Local(address) => address
                .tx
                .try_reserve()
                .map(|permit| SendPermit { permit })
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Closed(_) => SendError::Closed(()),
                    mpsc::error::TrySendError::Full(_) => SendError::Full(()),
                }),

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
            Inner::Local(address) => address
                .tx
                .clone()
                .reserve_owned()
                .await
                .map(|permit| OwnedSendPermit { permit })
                .map_err(Into::into),

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
            Inner::Local(address) => address
                .tx
                .clone()
                .try_reserve_owned()
                .map(|permit| OwnedSendPermit { permit })
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Closed(_) => SendError::Closed(()),
                    mpsc::error::TrySendError::Full(_) => SendError::Full(()),
                }),

            #[cfg(feature = "ipc")]
            Inner::Remote(_) => Err(SendError::other(
                "remote address does not support reserve",
                (),
            )),
        }
    }

    /// Returns a [`TypeErasedRecipient`] which can be downcast to a concrete
    /// [`Recipient<M>`][super::recipient::Recipient], where `M` is a specific message type picked
    /// by the user who overrides the [`Actor::type_erased_recipient_hook`] method.
    ///
    /// See [`Actor::type_erased_recipient_hook`] for more details.
    #[cfg(feature = "type-erased-recipient-hook")]
    #[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
    pub fn type_erased_recipient(&self) -> Option<TypeErasedRecipient> {
        match &self.0 {
            Inner::Local(address) => address.into_type_erased_recipient.map(|f| f(self)),
            #[cfg(feature = "ipc")]
            Inner::Remote(_) => None,
        }
    }
}

impl<A> SenderId for Address<A>
where
    A: Actor,
{
    fn index(&self) -> ActorId {
        self.index()
    }
}

impl<A, M, EP> Sender<M, EP> for Address<A>
where
    A: Actor,
    M: Message + IntoEnvelope<A, EP> + FromEnvelope<A, EP>,
    EP: 'static,
{
    fn closed(&self) -> EmptyFuture<'_> {
        self.closed().boxed()
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }

    fn capacity(&self) -> usize {
        self.capacity()
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
    fn type_erased_recipient(&self) -> Option<TypeErasedRecipient> {
        self.type_erased_recipient()
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

#[cfg(feature = "ipc")]
impl<A> From<RemoteAddress> for Address<A>
where
    A: Actor + HasCodecTable,
{
    fn from(addr: RemoteAddress) -> Self {
        Self(Inner::Remote(addr))
    }
}

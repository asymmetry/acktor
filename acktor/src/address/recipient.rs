use std::fmt::{self, Debug};
use std::future;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use futures_util::{FutureExt, TryFutureExt};
use tokio::time::Duration;
#[cfg(feature = "ipc")]
use tracing::{Instrument, debug};

use super::next_actor_id;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderInfo,
};
#[cfg(feature = "ipc")]
use super::{
    RemoteMailbox,
    remote::{RemoteProxy, RemoteRecipient},
};
use crate::actor::ActorId;
use crate::channel::mpsc;
use crate::envelope::DefaultEnvelopeProxy;
use crate::error::SendError;
use crate::message::Message;
use crate::utils::ShortName;
#[cfg(feature = "ipc")]
use crate::{
    codec::{Decode, Encode},
    message::{BinaryMessage, MessageId},
};

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
    /// Constructs a recipient from a [`Sender`].
    pub fn new(tx: Arc<dyn Sender<M, EP> + Send + Sync>) -> Self {
        Self(tx)
    }
}

#[cfg(feature = "ipc")]
impl<M, EP> Recipient<M, EP>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    /// Constructs a recipient from an index and a [`RemoteProxy`].
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    pub fn new_remote(index: u64, proxy: Arc<dyn RemoteProxy + Send + Sync>) -> Self {
        RemoteRecipient::new(index, proxy).into()
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
            Self::new(Arc::new(SenderProxy::new(next_actor_id(), tx))),
            rx,
        )
    }
}

#[cfg(feature = "ipc")]
impl<M> Recipient<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    /// Creates a [`mpsc::channel`], use the sender to constructs a recipient.
    ///
    /// Since it can provide a [`RemoteMailbox`], This recipient can be sent to an
    /// actor in another process and be used to receive messages from that actor.
    ///
    /// This recipient is not backed by any actor, so it can only be used to send messages with
    /// empty [`Message::Result`].
    pub fn create_remote(capacity: usize) -> (Self, mpsc::Receiver<M>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self::new(Arc::new(RemoteAddressableProxy::new(next_actor_id(), tx))),
            rx,
        )
    }
}

impl<M, EP> SenderInfo for Recipient<M, EP>
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
    fn remote_mailbox(&self) -> Option<RemoteMailbox> {
        self.0.remote_mailbox()
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

#[derive(Debug)]
struct SenderProxy<M>
where
    M: Message<Result = ()>,
{
    index: u64,
    tx: mpsc::Sender<M>,
}

impl<M> Clone for SenderProxy<M>
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

impl<M> PartialEq for SenderProxy<M>
where
    M: Message<Result = ()>,
{
    fn eq(&self, other: &Self) -> bool {
        self.index().eq(&other.index())
    }
}

impl<M> Eq for SenderProxy<M> where M: Message<Result = ()> {}

impl<M> Hash for SenderProxy<M>
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

impl<M> SenderProxy<M>
where
    M: Message<Result = ()>,
{
    const fn new(index: u64, tx: mpsc::Sender<M>) -> Self {
        Self { index, tx }
    }

    const fn index(&self) -> ActorId {
        ActorId::new(self.index)
    }
}

impl<M> SenderInfo for SenderProxy<M>
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

impl<M, EP> Sender<M, EP> for SenderProxy<M>
where
    M: Message<Result = ()>,
{
    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        future::ready(Err(SendError::other(
            "Recipient created with Recipient::create does not support send",
            msg,
        )))
        .boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.tx.send(msg).map_err(Into::into).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        Err(SendError::other(
            "Recipient created with Recipient::create does not support try_send",
            msg,
        ))
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.try_send(msg).map_err(Into::into)
    }

    fn send_timeout(&self, msg: M, _: Duration) -> SendResultFuture<'_, M> {
        future::ready(Err(SendError::other(
            "Recipient created with Recipient::create does not support send_timeout",
            msg,
        )))
        .boxed()
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.tx
            .send_timeout(msg, timeout)
            .map_err(Into::into)
            .boxed()
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        Err(SendError::other(
            "Recipient created with Recipient::create does not support blocking_send",
            msg,
        ))
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.blocking_send(msg).map_err(Into::into)
    }
}

// RemoteAddressableProxy is exactly the same as SenderPorxy, except for the trait bounds
// we can not merge them since specialization is not yet in stable Rust
#[cfg(feature = "ipc")]
#[derive(Debug)]
struct RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    index: u64,
    tx: mpsc::Sender<M>,
}

#[cfg(feature = "ipc")]
impl<M> Clone for RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            tx: self.tx.clone(),
        }
    }
}

#[cfg(feature = "ipc")]
impl<M> PartialEq for RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    fn eq(&self, other: &Self) -> bool {
        self.index().eq(&other.index())
    }
}

#[cfg(feature = "ipc")]
impl<M> Eq for RemoteAddressableProxy<M> where M: Message<Result = ()> + MessageId + Decode {}

#[cfg(feature = "ipc")]
impl<M> Hash for RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.index().hash(state)
    }
}

#[cfg(feature = "ipc")]
impl<M> RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    const fn new(index: u64, tx: mpsc::Sender<M>) -> Self {
        Self { index, tx }
    }

    const fn index(&self) -> ActorId {
        ActorId::new(self.index)
    }
}

#[cfg(feature = "ipc")]
impl<M> SenderInfo for RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
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

    fn remote_mailbox(&self) -> Option<RemoteMailbox> {
        let index = self.index;
        let tx = self.tx.clone();
        let (raw_tx, mut raw_rx) = mpsc::channel(self.tx.max_capacity());

        tokio::spawn(
            async move {
                loop {
                    let message = tokio::select! {
                        result = raw_rx.recv() => match result {
                            Ok(message) => message,
                            Err(_) => {
                                // inbound channel is closed
                                break;
                            }
                        },
                        _ = tx.closed() => {
                            // outbound channel is closed
                            break;
                        }
                    };

                    let BinaryMessage {
                        actor_id,
                        message_id,
                        bytes,
                        result_tx,
                        decode_msg_ctx,
                        ..
                    } = message;

                    if actor_id != index || message_id != M::ID || result_tx.is_some() {
                        // message is not for this recipient, ignore it
                        continue;
                    }

                    match M::decode(bytes, decode_msg_ctx.as_deref().map(|c| c as _)) {
                        Ok(message) => {
                            if tx.send(message).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            debug!("Could not decode message: {:?}", e);
                            continue;
                        }
                    }
                }
            }
            .in_current_span(),
        );

        let proxy = SenderProxy::new(index, raw_tx);
        Some(RemoteMailbox::new(Arc::new(proxy)))
    }
}

#[cfg(feature = "ipc")]
impl<M, EP> Sender<M, EP> for RemoteAddressableProxy<M>
where
    M: Message<Result = ()> + MessageId + Decode,
{
    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        future::ready(Err(SendError::other(
            "Recipient created with Recipient::create does not support send",
            msg,
        )))
        .boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        self.tx.send(msg).map_err(Into::into).boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        Err(SendError::other(
            "Recipient created with Recipient::create does not support try_send",
            msg,
        ))
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.try_send(msg).map_err(Into::into)
    }

    fn send_timeout(&self, msg: M, _: Duration) -> SendResultFuture<'_, M> {
        future::ready(Err(SendError::other(
            "Recipient created with Recipient::create does not support send_timeout",
            msg,
        )))
        .boxed()
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        self.tx
            .send_timeout(msg, timeout)
            .map_err(Into::into)
            .boxed()
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        Err(SendError::other(
            "Recipient created with Recipient::create does not support blocking_send",
            msg,
        ))
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        self.tx.blocking_send(msg).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Context as _, Result};
    use pretty_assertions::{assert_eq, assert_ne};
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
        assert!(recipient.send(Ping(2)).await.is_err());
        recipient.do_send(Ping(3)).await?;
        assert!(recipient.try_send(Ping(4)).is_err());
        recipient.try_do_send(Ping(5))?;
        assert!(
            recipient
                .send_timeout(Ping(6), Duration::from_millis(100))
                .await
                .is_err()
        );
        recipient
            .do_send_timeout(Ping(7), Duration::from_millis(100))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            assert!(recipient.blocking_send(Ping(8)).is_err());
            recipient.blocking_do_send(Ping(9))?;
            Ok(())
        })
        .await??;
        assert_eq!(rx.len(), 4);

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
        r1.send_timeout(Ping(14), Duration::from_millis(100))
            .await?;
        r1.do_send_timeout(Ping(15), Duration::from_millis(100))
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

    #[cfg(feature = "ipc")]
    #[tokio::test]
    async fn test_remote_recipient() -> Result<()> {
        // create() delivers to the receiver
        let (recipient, mut rx) = Recipient::<Ping>::create_remote(4);
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
        let (recipient, rx) = Recipient::<Ping>::create_remote(8);
        assert!(recipient.send(Ping(2)).await.is_err());
        recipient.do_send(Ping(3)).await?;
        assert!(recipient.try_send(Ping(4)).is_err());
        recipient.try_do_send(Ping(5))?;
        assert!(
            recipient
                .send_timeout(Ping(6), Duration::from_millis(100))
                .await
                .is_err()
        );
        recipient
            .do_send_timeout(Ping(7), Duration::from_millis(100))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            assert!(recipient.blocking_send(Ping(8)).is_err());
            recipient.blocking_do_send(Ping(9))?;
            Ok(())
        })
        .await??;
        assert_eq!(rx.len(), 4);

        use crate::message::BinaryMessage;
        // remote mailbox
        let (recipient, mut rx) = Recipient::<Ping>::create_remote(8);
        let mailbox = recipient
            .remote_mailbox()
            .context("remote_mailbox should return Some for RemoteAddressableProxy")?;
        let message = BinaryMessage {
            actor_id: recipient.index().as_local(),
            message_id: Ping::ID,
            bytes: 42_u32.to_le_bytes().to_vec().into(),
            result_tx: None,
            decode_msg_ctx: None,
            encode_res_ctx: None,
        };
        mailbox.do_send(message).await?;
        // send twice
        let message = BinaryMessage {
            actor_id: recipient.index().as_local(),
            message_id: Ping::ID,
            bytes: 42_u32.to_le_bytes().to_vec().into(),
            result_tx: None,
            decode_msg_ctx: None,
            encode_res_ctx: None,
        };
        mailbox.do_send(message).await?;
        let msg = rx.recv().await?;
        assert_eq!(msg.0, 42);
        let msg = rx.recv().await?;
        assert_eq!(msg.0, 42);

        Ok(())
    }

    #[test]
    fn test_proxy() {
        let (tx, _rx) = mpsc::channel::<Ping>(1);
        let proxy = SenderProxy::new(next_actor_id(), tx);

        // clone + eq + hash
        let clone = proxy.clone();
        assert_eq!(proxy, clone);
        assert_eq!(proxy.index(), clone.index());
        assert_eq!(hash_of(&proxy), hash_of(&clone));

        // distinct proxies with different indices are not equal
        let (tx2, _rx2) = mpsc::channel::<Ping>(1);
        let other = SenderProxy::new(next_actor_id(), tx2);
        assert_ne!(proxy, other);
        assert_ne!(proxy.index(), other.index());
    }

    #[cfg(feature = "ipc")]
    #[test]
    fn test_remote_addressable_proxy() {
        let (tx, _rx) = mpsc::channel::<Ping>(1);
        let proxy = RemoteAddressableProxy::new(next_actor_id(), tx);

        // clone + eq + hash
        let clone = proxy.clone();
        assert_eq!(proxy, clone);
        assert_eq!(proxy.index(), clone.index());
        assert_eq!(hash_of(&proxy), hash_of(&clone));

        // distinct proxies with different indices are not equal
        let (tx2, _rx2) = mpsc::channel::<Ping>(1);
        let other = RemoteAddressableProxy::new(next_actor_id(), tx2);
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
    }
}

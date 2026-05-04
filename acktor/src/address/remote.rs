use std::any::TypeId;
use std::fmt::{self, Debug};
use std::future;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{FutureExt, TryFutureExt};
use tokio::{runtime, time::Duration};

use super::recipient::Recipient;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderInfo,
};
use crate::actor::ActorId;
use crate::channel::oneshot;
use crate::codec::{
    CodecTable, Decode, DecodeContext, DecodeError, Encode, EncodeContext, MessageCodec,
};
use crate::error::SendError;
use crate::message::{BinaryMessage, Message, MessageId};
use crate::utils::ShortName;

/// Interface for sending binary messages to an actor in another process.
pub trait RemoteProxy {
    /// Returns the Tokio runtime handle associated with this proxy.
    fn runtime(&self) -> runtime::Handle;

    /// Returns the context used to encode outbound messages, if any.
    fn encode_context(&self) -> Option<&dyn EncodeContext>;

    /// Returns the context used to decode inbound messages, if any.
    fn decode_context(&self) -> Option<&dyn DecodeContext>;

    /// Returns the index of the proxy, which is used to identify the remote session.
    fn index(&self) -> NonZeroU64;

    /// Returns a future that resolves when the proxy is closed.
    fn closed(&self) -> EmptyFuture<'_>;

    /// Returns `true` if the proxy is already closed.
    fn is_closed(&self) -> bool;

    /// Returns the remaining capacity of the proxy.
    fn capacity(&self) -> usize;

    /// Sends an binary message, waiting until there is capacity.
    fn do_send(&self, msg: BinaryMessage) -> DoSendResultFuture<'_, BinaryMessage>;

    /// Attempts to immediately send an binary message.
    fn try_do_send(&self, msg: BinaryMessage) -> DoSendResult<BinaryMessage>;

    /// Sends an binary message, waiting until there is capacity, but only for a limited time.
    fn do_send_timeout(
        &self,
        msg: BinaryMessage,
        timeout: Duration,
    ) -> DoSendResultFuture<'_, BinaryMessage>;

    /// Blocking send to call outside of asynchronous contexts.
    ///
    /// This method is intended for use cases where you are sending from synchronous code to
    /// asynchronous code.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn blocking_do_send(&self, msg: BinaryMessage) -> DoSendResult<BinaryMessage>;
}

struct Inner {
    codec_table: &'static CodecTable,
    proxy: Arc<dyn RemoteProxy + Send + Sync>,
}

/// The address of an actor located in another process.
pub struct RemoteAddress {
    actor_id: ActorId,
    inner: Arc<Inner>,
}

impl Debug for RemoteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.index())
            .finish()
    }
}

impl Clone for RemoteAddress {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id,
            inner: self.inner.clone(),
        }
    }
}

async fn decode_res<M>(
    raw_rx: oneshot::Receiver<Bytes>,
    mut tx: oneshot::Sender<M::Result>,
    proxy: Arc<dyn RemoteProxy + Send + Sync>,
    codec: MessageCodec,
) where
    M: Message,
{
    let result = tokio::select! {
        result = raw_rx => match result {
            Ok(bytes) => (codec.decode_res)(bytes, proxy.decode_context()),
            Err(e) => {
                let _ = tx.send_err(e);
                return;
            }
        },

        // if the sender dropped the response rx, we can stop this task early
        _ = tx.closed() => return,
    };

    match result {
        Ok(boxed) => match boxed.downcast::<M::Result>() {
            Ok(res) => {
                let _ = tx.send(*res);
            }
            Err(_) => {
                // unreachable!();
                let _ = tx.send_err(DecodeError::other("downcast failed"));
            }
        },
        Err(e) => {
            let _ = tx.send_err(DecodeError::other(e));
        }
    }
}

impl RemoteAddress {
    /// Constructs a new [`RemoteAddress`].
    pub fn new(
        index: u64,
        codec_table: &'static CodecTable,
        proxy: Arc<dyn RemoteProxy + Send + Sync>,
    ) -> Self {
        Self {
            actor_id: ActorId::new_remote(index, proxy.index()),
            inner: Arc::new(Inner { codec_table, proxy }),
        }
    }

    #[inline]
    fn codec_table(&self) -> &'static CodecTable {
        self.inner.codec_table
    }

    #[inline]
    fn proxy(&self) -> &Arc<dyn RemoteProxy + Send + Sync> {
        &self.inner.proxy
    }

    /// Returns the index of the address.
    #[inline]
    pub const fn index(&self) -> ActorId {
        self.actor_id
    }

    /// Completes when the proxy of the actor has been closed.
    #[inline]
    pub fn closed(&self) -> impl Future<Output = ()> + Send + '_ {
        self.proxy().closed()
    }

    /// Checks if the proxy of the actor is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.proxy().is_closed()
    }

    /// Returns the current capacity of the proxy of the actor.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.proxy().capacity()
    }

    /// Sends a message, waiting until there is capacity, and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub async fn send<M>(&self, msg: M) -> SendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy()
                        .do_send(BinaryMessage::send(
                            self.index(),
                            codec.message_id,
                            bytes,
                            raw_tx,
                        ))
                        .await
                        .map_err(|e| e.with_msg(msg))?;

                    let (tx, rx) = oneshot::channel();
                    let proxy = self.proxy().clone();

                    tokio::spawn(decode_res::<M>(raw_rx, tx, proxy, *codec));

                    Ok(rx)
                }

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }

    pub async fn do_send<M>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => self
                    .proxy()
                    .do_send(BinaryMessage::do_send(
                        self.index(),
                        codec.message_id,
                        bytes,
                    ))
                    .await
                    .map_err(|e| e.with_msg(msg)),

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }

    /// Attempts to immediately send a message and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub fn try_send<M>(&self, msg: M) -> SendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy()
                        .try_do_send(BinaryMessage::send(
                            self.index(),
                            codec.message_id,
                            bytes,
                            raw_tx,
                        ))
                        .map_err(|e| e.with_msg(msg))?;

                    let (tx, rx) = oneshot::channel();
                    let proxy = self.proxy().clone();

                    tokio::spawn(decode_res::<M>(raw_rx, tx, proxy, *codec));

                    Ok(rx)
                }

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }

    /// Attempts to immediately send a message without expecting a response.
    pub fn try_do_send<M>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => self
                    .proxy()
                    .try_do_send(BinaryMessage::do_send(
                        self.index(),
                        codec.message_id,
                        bytes,
                    ))
                    .map_err(|e| e.with_msg(msg)),

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub async fn send_timeout<M>(&self, msg: M, timeout: Duration) -> SendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy()
                        .do_send_timeout(
                            BinaryMessage::send(self.index(), codec.message_id, bytes, raw_tx),
                            timeout,
                        )
                        .await
                        .map_err(|e| e.with_msg(msg))?;

                    let (tx, rx) = oneshot::channel();
                    let proxy = self.proxy().clone();

                    tokio::spawn(decode_res::<M>(raw_rx, tx, proxy, *codec));

                    Ok(rx)
                }

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }

    /// Sends a message, waiting until there is capacity, but only for a limited time, without
    /// expecting a response.
    pub async fn do_send_timeout<M>(&self, msg: M, timeout: Duration) -> DoSendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => self
                    .proxy()
                    .do_send_timeout(
                        BinaryMessage::do_send(self.index(), codec.message_id, bytes),
                        timeout,
                    )
                    .await
                    .map_err(|e| e.with_msg(msg)),

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
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
    pub fn blocking_send<M>(&self, msg: M) -> SendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy()
                        .blocking_do_send(BinaryMessage::send(
                            self.index(),
                            codec.message_id,
                            bytes,
                            raw_tx,
                        ))
                        .map_err(|e| e.with_msg(msg))?;

                    let (tx, rx) = oneshot::channel();
                    let proxy = self.proxy().clone();
                    let runtime = self.proxy().runtime();

                    runtime.spawn(decode_res::<M>(raw_rx, tx, proxy, *codec));

                    Ok(rx)
                }

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
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
    pub fn blocking_do_send<M>(&self, msg: M) -> DoSendResult<M>
    where
        M: Message,
    {
        match self.codec_table().get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy().encode_context()) {
                Ok(bytes) => self
                    .proxy()
                    .blocking_do_send(BinaryMessage::do_send(
                        self.index(),
                        codec.message_id,
                        bytes,
                    ))
                    .map_err(|e| e.with_msg(msg)),

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }
}

/// A type which is used to send a specific message type to an actor in another process.
pub struct RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    actor_id: ActorId,
    proxy: Arc<dyn RemoteProxy + Send + Sync>,
    _marker: PhantomData<fn(M) -> M>,
}

impl<M> Debug for RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&ShortName::of::<Self>().to_string())
            .field(&self.index())
            .finish()
    }
}

impl<M> Clone for RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id,
            proxy: self.proxy.clone(),
            _marker: PhantomData,
        }
    }
}

impl<M> RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    /// Constructs a new [`RemoteRecipient`].
    pub fn new(index: u64, proxy: Arc<dyn RemoteProxy + Send + Sync>) -> Self {
        Self {
            actor_id: ActorId::new_remote(index, proxy.index()),
            proxy,
            _marker: PhantomData,
        }
    }

    /// Returns the index of the recipient.
    pub const fn index(&self) -> ActorId {
        self.actor_id
    }
}

impl<M> SenderInfo for RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn index(&self) -> ActorId {
        self.actor_id
    }

    fn closed(&self) -> EmptyFuture<'_> {
        self.proxy.closed()
    }

    fn is_closed(&self) -> bool {
        self.proxy.is_closed()
    }

    fn capacity(&self) -> usize {
        self.proxy.capacity()
    }
}

async fn decode_res_with_bound<M>(
    raw_rx: oneshot::Receiver<Bytes>,
    mut tx: oneshot::Sender<M::Result>,
    proxy: Arc<dyn RemoteProxy + Send + Sync>,
) where
    M: Message,
    M::Result: Decode,
{
    // `M::Result` is bounded by `Decode` trait so we do not need to dynamically dispatch it with the codec table.

    let result = tokio::select! {
        result = raw_rx => match result {
            Ok(bytes) => M::Result::decode(bytes, proxy.decode_context()),
            Err(e) => {
                let _ = tx.send_err(e);
                return;
            }
        },

        // if the sender dropped the response rx, we can stop this task early
        _ = tx.closed() => return,
    };

    match result {
        Ok(result) => {
            let _ = tx.send(result);
        }
        Err(e) => {
            let _ = tx.send_err(DecodeError::other(e));
        }
    }
}

impl<M, EP> Sender<M, EP> for RemoteRecipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        let (raw_tx, raw_rx) = oneshot::channel();
        let (tx, rx) = oneshot::channel();

        let proxy = self.proxy.clone();

        tokio::spawn(decode_res_with_bound::<M>(raw_rx, tx, proxy));

        self.proxy
            .do_send(BinaryMessage::send(self.index(), M::ID, bytes, raw_tx))
            .map(|result| match result {
                Ok(_) => Ok(rx),
                Err(e) => Err(e.with_msg(msg)),
            })
            .boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        self.proxy
            .do_send(BinaryMessage::do_send(self.index(), M::ID, bytes))
            .map_err(|e| e.with_msg(msg))
            .boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        let (raw_tx, raw_rx) = oneshot::channel();
        let (tx, rx) = oneshot::channel();

        let proxy = self.proxy.clone();

        tokio::spawn(decode_res_with_bound::<M>(raw_rx, tx, proxy));

        match self
            .proxy
            .try_do_send(BinaryMessage::send(self.index(), M::ID, bytes, raw_tx))
        {
            Ok(_) => Ok(rx),
            Err(e) => Err(e.with_msg(msg)),
        }
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        self.proxy
            .try_do_send(BinaryMessage::do_send(self.index(), M::ID, bytes))
            .map_err(|e| e.with_msg(msg))
    }

    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        let (raw_tx, raw_rx) = oneshot::channel();
        let (tx, rx) = oneshot::channel();

        let proxy = self.proxy.clone();

        tokio::spawn(decode_res_with_bound::<M>(raw_rx, tx, proxy));

        self.proxy
            .do_send_timeout(
                BinaryMessage::send(self.index(), M::ID, bytes, raw_tx),
                timeout,
            )
            .map(|result| match result {
                Ok(_) => Ok(rx),
                Err(e) => Err(e.with_msg(msg)),
            })
            .boxed()
    }

    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        self.proxy
            .do_send_timeout(BinaryMessage::do_send(self.index(), M::ID, bytes), timeout)
            .map_err(|e| e.with_msg(msg))
            .boxed()
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        let (raw_tx, raw_rx) = oneshot::channel();
        let (tx, rx) = oneshot::channel();

        let proxy = self.proxy.clone();
        let runtime = proxy.runtime();

        runtime.spawn(decode_res_with_bound::<M>(raw_rx, tx, proxy));

        match self
            .proxy
            .blocking_do_send(BinaryMessage::send(self.index(), M::ID, bytes, raw_tx))
        {
            Ok(_) => Ok(rx),
            Err(e) => Err(e.with_msg(msg)),
        }
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        let bytes = match msg.encode_to_bytes(self.proxy.encode_context()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        self.proxy
            .blocking_do_send(BinaryMessage::do_send(self.index(), M::ID, bytes))
            .map_err(|e| e.with_msg(msg))
    }
}

impl<M, EP> From<RemoteRecipient<M>> for Recipient<M, EP>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn from(recipient: RemoteRecipient<M>) -> Self {
        Self::new(Arc::new(recipient))
    }
}

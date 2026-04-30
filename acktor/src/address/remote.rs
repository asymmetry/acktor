use std::any::TypeId;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use tokio::time::Duration;

use super::recipient::Recipient;
use super::remote_proxy::RemoteProxy;
use super::sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderId,
};
use crate::actor::ActorId;
#[cfg(feature = "type-erased-recipient-hook")]
use crate::actor::TypeErasedRecipient;
use crate::channel::oneshot;
use crate::codec::{CodecTable, DecodeError, MessageCodec};
use crate::error::SendError;
use crate::message::Message;

/// The address of an actor located in a different process.
pub struct RemoteAddress {
    index: ActorId,
    proxy: Arc<dyn RemoteProxy + Send + Sync>,
    codec: &'static CodecTable,
}

impl Clone for RemoteAddress {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            proxy: self.proxy.clone(),
            codec: self.codec,
        }
    }
}

async fn decode_and_forward<M>(
    raw_rx: oneshot::Receiver<Bytes>,
    mut tx: oneshot::Sender<M::Result>,
    address: RemoteAddress,
    codec: MessageCodec,
) where
    M: Message,
{
    let result = tokio::select! {
        result = raw_rx => match result {
            Ok(bytes) => (codec.decode_res)(bytes, address.proxy.decode_context()),
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
    pub const fn new(
        index: ActorId,
        proxy: Arc<dyn RemoteProxy + Send + Sync>,
        codec: &'static CodecTable,
    ) -> Self {
        Self {
            index,
            proxy,
            codec,
        }
    }

    /// Returns the index of the address.
    pub const fn index(&self) -> ActorId {
        self.index
    }

    /// Completes when the proxy of the actor has been closed.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + '_ {
        self.proxy.closed()
    }

    /// Checks if the proxy of the actor is closed.
    pub fn is_closed(&self) -> bool {
        self.proxy.is_closed()
    }

    /// Returns the current capacity of the proxy of the actor.
    pub fn capacity(&self) -> usize {
        self.proxy.capacity()
    }

    /// Sends a message, waiting until there is capacity, and returns a
    /// [`Receiver`][oneshot::Receiver] which can be used to receive the message response.
    pub async fn send<M>(&self, msg: M) -> SendResult<M>
    where
        M: Message,
    {
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy
                        .do_send(self.index.as_u64(), codec.message_id, bytes, Some(raw_tx))
                        .await
                        .map_err(|e| e.with_msg(msg))?;

                    let (result_tx, result_rx) = oneshot::channel();
                    let address = self.clone();

                    tokio::spawn(decode_and_forward::<M>(raw_rx, result_tx, address, *codec));

                    Ok(result_rx)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => self
                    .proxy
                    .do_send(self.index.as_u64(), codec.message_id, bytes, None)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy
                        .try_do_send(self.index.as_u64(), codec.message_id, bytes, Some(raw_tx))
                        .map_err(|e| e.with_msg(msg))?;

                    let (result_tx, result_rx) = oneshot::channel();
                    let address = self.clone();

                    tokio::spawn(decode_and_forward::<M>(raw_rx, result_tx, address, *codec));

                    Ok(result_rx)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => self
                    .proxy
                    .try_do_send(self.index.as_u64(), codec.message_id, bytes, None)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy
                        .do_send_timeout(
                            self.index.as_u64(),
                            codec.message_id,
                            bytes,
                            timeout,
                            Some(raw_tx),
                        )
                        .await
                        .map_err(|e| e.with_msg(msg))?;

                    let (result_tx, result_rx) = oneshot::channel();
                    let address = self.clone();

                    tokio::spawn(decode_and_forward::<M>(raw_rx, result_tx, address, *codec));

                    Ok(result_rx)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => self
                    .proxy
                    .do_send_timeout(self.index.as_u64(), codec.message_id, bytes, timeout, None)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => {
                    let (raw_tx, raw_rx) = oneshot::channel();
                    self.proxy
                        .blocking_do_send(
                            self.index.as_u64(),
                            codec.message_id,
                            bytes,
                            Some(raw_tx),
                        )
                        .map_err(|e| e.with_msg(msg))?;

                    let (result_tx, result_rx) = oneshot::channel();
                    let address = self.clone();
                    let runtime = self.proxy.runtime();

                    runtime.spawn(decode_and_forward::<M>(raw_rx, result_tx, address, *codec));

                    Ok(result_rx)
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
        match self.codec.get(&TypeId::of::<M>()) {
            Some(codec) => match (codec.encode_msg)(&msg, self.proxy.encode_context()) {
                Ok(bytes) => self
                    .proxy
                    .blocking_do_send(self.index.as_u64(), codec.message_id, bytes, None)
                    .map_err(|e| e.with_msg(msg)),

                Err(e) => Err(SendError::Other(e.into(), msg)),
            },

            None => Err(SendError::NoEncodeFn(msg)),
        }
    }
}

impl SenderId for RemoteAddress {
    fn index(&self) -> ActorId {
        self.index()
    }
}

impl<M, EP> Sender<M, EP> for RemoteAddress
where
    M: Message,
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
        None
    }
}

impl<M, EP> From<RemoteAddress> for Recipient<M, EP>
where
    M: Message,
    EP: 'static,
{
    fn from(addr: RemoteAddress) -> Self {
        Self::new(Arc::new(addr))
    }
}

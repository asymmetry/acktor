use std::pin::Pin;

use tokio::time::Duration;

use crate::actor::ActorId;
#[cfg(feature = "ipc")]
use crate::actor::RemoteMailbox;
use crate::channel::oneshot::Receiver;
use crate::envelope::DefaultEnvelopeProxy;
use crate::error::SendError;
use crate::message::Message;

pub type SendResult<M> = Result<Receiver<<M as Message>::Result>, SendError<M>>;
pub type SendResultFuture<'a, M> = Pin<
    Box<dyn Future<Output = Result<Receiver<<M as Message>::Result>, SendError<M>>> + Send + 'a>,
>;

pub type DoSendResult<M> = Result<(), SendError<M>>;
pub type DoSendResultFuture<'a, M> =
    Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + 'a>>;

pub type EmptyFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Describes some basic information of a sender.
pub trait SenderInfo {
    /// Returns the index of the sender.
    fn index(&self) -> ActorId;

    /// Returns `true` if this sender is a remote address, which means the receiver is located in
    /// another process.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    #[inline]
    fn is_remote(&self) -> bool {
        self.index().is_remote()
    }

    /// Completes when the underlying channel has been closed.
    fn closed(&self) -> EmptyFuture<'_>;

    /// Checks if the underlying channel has been closed.
    fn is_closed(&self) -> bool;

    /// Returns the current capacity of the underlying channel.
    fn capacity(&self) -> usize;

    /// Returns a [`RemoteMailbox`], if this sender is remote addressable and it is not a remote
    /// address.
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    fn remote_mailbox(&self) -> Option<RemoteMailbox> {
        None
    }
}

/// Describes how to send a message to a receiver.
pub trait Sender<M, EP = DefaultEnvelopeProxy<M>>: SenderInfo
where
    M: Message,
{
    /// Sends a message, waiting until there is capacity, and returns a [`Receiver`] which can be
    /// used to receive the message response.
    fn send(&self, msg: M) -> SendResultFuture<'_, M>;

    /// Sends a message, waiting until there is capacity, without expecting a response.
    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M>;

    /// Attempts to immediately send a message and returns a [`Receiver`] which can be used to
    /// receive the message response.
    fn try_send(&self, msg: M) -> SendResult<M>;

    /// Attempts to immediately send a message without expecting a response.
    fn try_do_send(&self, msg: M) -> DoSendResult<M>;

    /// Sends a message, waiting until there is capacity, but only for a limited time, and returns
    /// a [`Receiver`] which can be used to receive the message response.
    fn send_timeout(&self, msg: M, timeout: Duration) -> SendResultFuture<'_, M>;

    /// Sends a message, waiting until there is capacity, but only for a limited time, without
    /// expecting a response.
    fn do_send_timeout(&self, msg: M, timeout: Duration) -> DoSendResultFuture<'_, M>;

    /// Blocking send to call outside of asynchronous contexts.
    ///
    /// This method is intended for use cases where you are sending from synchronous code to
    /// asynchronous code.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn blocking_send(&self, msg: M) -> SendResult<M>;

    /// Blocking do_send to call outside of asynchronous contexts.
    ///
    /// This method is intended for use cases where you are sending from synchronous code to
    /// asynchronous code.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn blocking_do_send(&self, msg: M) -> DoSendResult<M>;
}

use std::pin::Pin;

use tokio::sync::{
    mpsc::error::{SendError, TrySendError},
    oneshot,
};

use crate::envelope::DefaultEnvelopeProxy;
use crate::message::Message;

pub(crate) type SendResult<M, R> = Result<oneshot::Receiver<R>, SendError<M>>;
pub(crate) type TrySendResult<M, R> = Result<oneshot::Receiver<R>, TrySendError<M>>;

/// Describes how to retrieve the index of a sender.
///
/// Separate from the [`Sender`] trait so we do not need to use fully qualified syntax to use
/// this trait when multiple [`Sender`] traits are in scope.
pub trait SenderIndex {
    /// Returns the index of the sender.
    fn index(&self) -> usize;
}

/// Describes how to send a message.
pub trait Sender<M, EP = DefaultEnvelopeProxy<M>>: SenderIndex
where
    M: Message<EP>,
    EP: 'static,
{
    /// Checks if the channel is closed.
    fn is_closed(&self) -> bool;

    /// Returns the capacity of the channel.
    fn capacity(&self) -> usize;

    /// Sends a message and returns a [`oneshot::Receiver`] which could be used to receive
    /// the response.
    fn send(&self, msg: M) -> Pin<Box<dyn Future<Output = SendResult<M, M::Result>> + Send + '_>>;

    /// Sends a message without expecting a response.
    fn do_send(
        &self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + '_>>;

    /// Attempts to send a message and returns a [`oneshot::Receiver`] which could be used to
    /// receive the response.
    fn try_send(&self, msg: M) -> TrySendResult<M, M::Result>;

    /// Attempts to send a message without expecting a response.
    fn try_do_send(&self, msg: M) -> Result<(), TrySendError<M>>;

    /// Sends a message and returns a [`oneshot::Receiver`] which could be used to receive
    /// the response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_send(&self, msg: M) -> SendResult<M, M::Result>;

    /// Sends a message to an actor without expecting a response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_do_send(&self, msg: M) -> Result<(), SendError<M>>;
}

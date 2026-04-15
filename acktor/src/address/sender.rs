use std::pin::Pin;

#[cfg(feature = "erased-recipient")]
use std::any::Any;

use crate::actor::ActorId;
use crate::envelope::DefaultEnvelopeProxy;
use crate::errors::{DoSendResult, SendResult};
use crate::message::Message;

/// Describes how to retrieve the index of a sender.
///
/// Separate from the [`Sender`] trait so we do not need to use fully qualified syntax to use
/// this trait when multiple [`Sender`] traits are in scope.
pub trait SenderId {
    /// Returns the index of the sender.
    fn index(&self) -> ActorId;

    /// Returns `true` if this sender refers to an actor in another process.
    ///
    /// The MSB of the ActorId is reserved by [`acktor-ipc`] crate to tag remote
    /// addresses.
    ///
    /// [`acktor-ipc`]: https://docs.rs/acktor-ipc/latest/acktor_ipc
    #[inline]
    fn is_remote(&self) -> bool {
        self.index() >> 63 != 0
    }
}

impl SenderId for ActorId {
    #[inline]
    fn index(&self) -> ActorId {
        *self
    }
}

/// Describes how to send a message.
pub trait Sender<M, EP = DefaultEnvelopeProxy<M>>: SenderId
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
    fn send(&self, msg: M) -> Pin<Box<dyn Future<Output = SendResult<M, EP>> + Send + '_>>;

    /// Sends a message without expecting a response.
    fn do_send(&self, msg: M) -> Pin<Box<dyn Future<Output = DoSendResult<M>> + Send + '_>>;

    /// Attempts to send a message and returns a [`oneshot::Receiver`] which could be used to
    /// receive the response.
    fn try_send(&self, msg: M) -> SendResult<M, EP>;

    /// Attempts to send a message without expecting a response.
    fn try_do_send(&self, msg: M) -> DoSendResult<M>;

    /// Sends a message and returns a [`oneshot::Receiver`] which could be used to receive
    /// the response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_send(&self, msg: M) -> SendResult<M, EP>;

    /// Sends a message to an actor without expecting a response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_do_send(&self, msg: M) -> DoSendResult<M>;

    /// If the actor backing this sender opted into the conversion via
    /// [`Actor::erased_recipient_fn`][crate::actor::Actor::erased_recipient_fn], returns the
    /// resulting type-erased trait object. Downstream crates can then downcast this into the
    /// concrete type they used to override the
    /// [`Actor::erased_recipient_fn`][crate::actor::Actor::erased_recipient_fn] method.
    #[cfg(feature = "erased-recipient")]
    #[cfg_attr(docsrs, doc(cfg(feature = "erased-recipient")))]
    fn erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }
}

use std::pin::Pin;

#[cfg(feature = "type-erased-recipient-hook")]
use std::any::Any;

use crate::actor::ActorId;
use crate::channel::oneshot::Receiver;
use crate::envelope::DefaultEnvelopeProxy;
use crate::errors::SendError;
use crate::message::Message;

pub type SendResult<M, EP = DefaultEnvelopeProxy<M>> =
    Result<Receiver<<M as Message<EP>>::Result>, SendError<M>>;

pub type SendResultFuture<'a, M, EP = DefaultEnvelopeProxy<M>> =
    Pin<Box<dyn Future<Output = SendResult<M, EP>> + Send + 'a>>;

pub type DoSendResult<M> = Result<(), SendError<M>>;

pub type DoSendResultFuture<'a, M> = Pin<Box<dyn Future<Output = DoSendResult<M>> + Send + 'a>>;

/// Describes how to retrieve the index of a sender.
///
/// It is separated from the [`Sender`] trait so we do not need to use fully qualified syntax
/// to use the [`index`][SenderId::index] method when multiple [`Sender`] traits are in scope.
pub trait SenderId {
    /// Returns the index of the sender.
    fn index(&self) -> ActorId;

    /// Returns `true` if this sender refers to an actor in another process.
    ///
    /// The MSB of the ActorId is reserved by [`acktor-ipc`] crate to tag remote addresses.
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
{
    /// Checks if the channel is closed.
    fn is_closed(&self) -> bool;

    /// Returns the capacity of the channel.
    fn capacity(&self) -> usize;

    /// Sends a message and returns a [`Receiver`][crate::channel::oneshot::Receiver] which can
    /// be used to receive the message response.
    fn send(&self, msg: M) -> SendResultFuture<'_, M, EP>;

    /// Sends a message without expecting a response.
    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M>;

    /// Attempts to send a message and returns a [`Receiver`][crate::channel::oneshot::Receiver]
    /// which can be used to receive the message response.
    fn try_send(&self, msg: M) -> SendResult<M, EP>;

    /// Attempts to send a message without expecting a response.
    fn try_do_send(&self, msg: M) -> DoSendResult<M>;

    /// Sends a message and returns a [`Receiver`][crate::channel::oneshot::Receiver] which can
    /// be used to receive the message response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_send(&self, msg: M) -> SendResult<M, EP>;

    /// Sends a message without expecting a response.
    ///
    /// This method is intended for use cases where you are sending from synchronous code.
    fn blocking_do_send(&self, msg: M) -> DoSendResult<M>;

    /// Returns a type-erased trait object which can be downcast into a concrete
    /// [`Recipient<M>`][super::recipient::Recipient], where `M` is a specific message type chosen
    /// by the user who overrides the
    /// [`Actor::type_erased_recipient_fn`][crate::actor::Actor::type_erased_recipient_fn] method.
    ///
    /// See [`Actor::type_erased_recipient_fn`][crate::actor::Actor::type_erased_recipient_fn] for
    /// details.
    #[cfg(feature = "type-erased-recipient-hook")]
    #[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
    fn type_erased_recipient(&self) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }
}

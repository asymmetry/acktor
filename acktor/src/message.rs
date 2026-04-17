//! Message passing between actors.
//!
//! A [`Message`] is a type that can be sent between actors. It is a trait that can be derived
//! for custom types. A specific message type can be sent to an actor only if the actor implements
//! the corresponding [`Handler`] trait for the message type, which describes the action of the
//! actor when it receives the message.
//!

use std::error::Error;
use std::future::{self, Future};
use std::sync::Arc;

use tracing::debug;

use crate::actor::Actor;
use crate::channel::oneshot;
use crate::envelope::DefaultEnvelopeProxy;
use crate::errors::ErrorReport;

mod result;
pub use result::MessageResult;

mod future_result;
pub use future_result::FutureMessageResult;

/// Types that can be sent between actors.
pub trait Message<EP = DefaultEnvelopeProxy<Self>>: Send + 'static {
    /// The type of the response produced when this message is handled.
    type Result: Send + 'static;
}

/// Describes how an actor handles a specific message type.
pub trait Handler<M, EP = DefaultEnvelopeProxy<M>>: Actor
where
    M: Message<EP>,
{
    /// The return type of the handler, which must implement [`MessageResponse`].
    type Result: MessageResponse<Self, M, EP>;

    /// Handles a message.
    fn handle(
        &mut self,
        msg: M,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Self::Result> + Send;
}

/// Types that can be sent as a response to a message.
pub trait MessageResponse<A, M, EP = DefaultEnvelopeProxy<M>>: Send
where
    A: Actor,
    M: Message<EP>,
{
    /// Handles the response.
    fn handle(
        self,
        ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send;
}

impl Message for () {
    type Result = ();
}

// implement Message trait for a few common wrapper types

impl<M, EP> Message<EP> for Box<M>
where
    M: Message<EP>,
{
    type Result = M::Result;
}

impl<M, EP> Message<EP> for Arc<M>
where
    M: Message<EP> + Sync,
{
    type Result = M::Result;
}

// implement MessageResponse trait for a few common wrapper types

impl<A, M, EP, T, E> MessageResponse<A, M, EP> for Result<T, E>
where
    A: Actor,
    M: Message<EP, Result = Self>,
    T: Send,
    E: Into<Box<dyn Error + Send + Sync>> + Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            if let Err(Err(e)) = tx.send(self) {
                debug!(
                    "Could not send the message response since the channel is closed, log the \
                    dropped response since it is an error: {}",
                    e.into().report()
                );
            }
        }
        // tx is None means the sender does not care about the result, so we simply drop it
        future::ready(())
    }
}

impl<A, M, EP, T> MessageResponse<A, M, EP> for Option<T>
where
    A: Actor,
    M: Message<EP, Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, EP, T> MessageResponse<A, M, EP> for Box<T>
where
    A: Actor,
    M: Message<EP, Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, EP, T> MessageResponse<A, M, EP> for Arc<T>
where
    A: Actor,
    M: Message<EP, Result = Self>,
    T: Send + Sync,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

impl<A, M, EP, T> MessageResponse<A, M, EP> for Vec<T>
where
    A: Actor,
    M: Message<EP, Result = Self>,
    T: Send,
{
    fn handle(
        self,
        _ctx: &mut A::Context,
        tx: Option<oneshot::Sender<M::Result>>,
    ) -> impl Future<Output = ()> + Send {
        if let Some(tx) = tx {
            let _ = tx.send(self);
        }
        future::ready(())
    }
}

macro_rules! impl_message_response_for {
    ($type:ty) => {
        impl<A, M, EP> MessageResponse<A, M, EP> for $type
        where
            A: Actor,
            M: Message<EP, Result = Self>,
        {
            fn handle(
                self,
                _ctx: &mut A::Context,
                tx: Option<oneshot::Sender<M::Result>>,
            ) -> impl Future<Output = ()> + Send {
                if let Some(tx) = tx {
                    let _ = tx.send(self);
                }
                future::ready(())
            }
        }
    };
}

impl_message_response_for!(());
impl_message_response_for!(bool);
impl_message_response_for!(i8);
impl_message_response_for!(i16);
impl_message_response_for!(i32);
impl_message_response_for!(i64);
impl_message_response_for!(isize);
impl_message_response_for!(u8);
impl_message_response_for!(u16);
impl_message_response_for!(u32);
impl_message_response_for!(u64);
impl_message_response_for!(usize);
impl_message_response_for!(f32);
impl_message_response_for!(f64);
impl_message_response_for!(char);
impl_message_response_for!(String);

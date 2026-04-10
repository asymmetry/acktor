//! Traits and type definitions for the envelope of a message.
//!
//! The [`Envelope`] type is a helper which is used to send different message types in a single
//! channel. It is necessary for a statically typed language like Rust.
//!
//! This module provides some traits which are useful when packing and unpacking messages into
//! an envelope for a specific actor.
//!

use std::any::Any;
use std::fmt;
use std::pin::Pin;

use tokio::sync::oneshot;

use crate::actor::Actor;
use crate::message::{Handler, Message};

mod default_proxy;
pub use default_proxy::DefaultEnvelopeProxy;

/// Describes how to pack the message into a proper envelope type for a specific actor type.
pub trait ToEnvelope<A, M, EP = DefaultEnvelopeProxy<M>>
where
    A: Actor,
    M: Message<EP>,
{
    /// Packs the message and the optional response sender into an envelope.
    fn pack(msg: M, tx: Option<oneshot::Sender<M::Result>>) -> Envelope<A>;
}

/// Describes how to unpack the message from the envelope for a specific actor type.
pub trait FromEnvelope<A, M, EP = DefaultEnvelopeProxy<M>>
where
    A: Actor,
    M: Message<EP>,
{
    /// Unpacks the message from the envelope.
    fn unpack(envelope: Envelope<A>) -> M;
}

/// Helps to save a message in an envelope as a boxed trait object.
pub trait EnvelopeProxy<A>: Send + 'static
where
    A: Actor,
{
    /// Handles the message in the envelope.
    fn handle<'a, 'b>(
        &'a mut self,
        actor: &'b mut A,
        ctx: &'b mut A::Context,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        'b: 'a;

    /// Returns the envelope proxy as a reference of a trait object of [`Any`].
    fn as_any(&self) -> &dyn Any;

    /// Returns the envelope proxy as a mutable reference of a trait object of [`Any`].
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// A type of packed message of a specific actor.
///
/// This type is used to store different message types in a single mailbox.
#[repr(transparent)]
pub struct Envelope<A>(Box<dyn EnvelopeProxy<A> + Send + 'static>)
where
    A: Actor;

impl<A> fmt::Debug for Envelope<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("Envelope<{}>", crate::utils::type_name::<A>()))
    }
}

impl<A> Envelope<A>
where
    A: Actor,
{
    /// Constructs a new [`Envelope`] with a message and an optional response sender.
    pub fn new<M>(msg: M, tx: Option<oneshot::Sender<M::Result>>) -> Self
    where
        A: Handler<M>,
        M: Message,
    {
        Self(Box::new(DefaultEnvelopeProxy {
            message: Some(msg),
            tx,
        }))
    }

    /// Constructs a new [`Envelope`] with a boxed trait object of [`EnvelopeProxy`].
    pub fn with_proxy(proxy: Box<dyn EnvelopeProxy<A> + Send>) -> Self {
        Self(proxy)
    }
}

impl<A> EnvelopeProxy<A> for Envelope<A>
where
    A: Actor,
{
    fn handle<'a, 'b>(
        &'a mut self,
        actor: &'b mut A,
        ctx: &'b mut A::Context,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        'b: 'a,
    {
        self.0.handle(actor, ctx)
    }

    fn as_any(&self) -> &dyn Any {
        self.0.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.0.as_any_mut()
    }
}

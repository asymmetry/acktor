//! Traits and type definitions for the envelope of a message.
//!
//! The [`Envelope`] type is a helper which is used to send different message types in a single
//! channel. It is necessary for a statically typed language like Rust.
//!
//! This module provides some traits which are useful when packing and unpacking messages into
//! an envelope for a specific actor.
//!

use std::any::Any;
use std::fmt::{self, Debug};
use std::pin::Pin;

use crate::actor::Actor;
use crate::channel::oneshot;
use crate::message::Message;

mod default_proxy;
pub use default_proxy::DefaultEnvelopeProxy;

mod timed_proxy;
pub use timed_proxy::{Timed, TimedEnvelopeProxy};

/// Describes how to pack the message into a proper envelope type for a specific actor type.
pub trait IntoEnvelope<A, EP = DefaultEnvelopeProxy<Self>>: Message
where
    A: Actor,
{
    /// Packs the message and the optional response sender into an envelope.
    fn pack(self, tx: Option<oneshot::Sender<Self::Result>>) -> Envelope<A>;
}

/// Describes how to unpack the message from the envelope for a specific actor type.
pub trait FromEnvelope<A, EP = DefaultEnvelopeProxy<Self>>: Message
where
    A: Actor,
{
    /// Unpacks the message from the envelope.
    fn unpack(envelope: Envelope<A>) -> Self;
}

/// Helps to save a message in an envelope as a boxed trait object.
pub trait EnvelopeProxy<A>
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
pub struct Envelope<A>(Box<dyn EnvelopeProxy<A> + Send>)
where
    A: Actor;

impl<A> Debug for Envelope<A>
where
    A: Actor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "Envelope<{}>",
            crate::utils::ShortName::of::<A>()
        ))
    }
}

impl<A> Envelope<A>
where
    A: Actor,
{
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::test_utils::{Dummy, Ping};

    #[test]
    fn test_debug_fmt() {
        let budget = Duration::from_secs(1);

        // DefaultEnvelopeProxy
        let default_proxy = DefaultEnvelopeProxy {
            message: Some(Ping(42)),
            tx: None,
        };
        assert_eq!(format!("{default_proxy:?}"), "DefaultEnvelopeProxy<Ping>");

        // TimedEnvelopeProxy
        let timed_proxy = TimedEnvelopeProxy {
            message: Some(Ping(42)),
            tx: None,
            budget,
        };
        assert_eq!(format!("{timed_proxy:?}"), "TimedEnvelopeProxy<Ping>");

        // Timed
        let timed = Timed::new(Ping(42), budget);
        assert_eq!(format!("{timed:?}"), format!("Timed<Ping>({budget:?})"));

        // Envelope
        let envelope = Envelope::<Dummy>::with_proxy(Box::new(default_proxy));
        assert_eq!(format!("{envelope:?}"), "Envelope<Dummy>");

        let trait_object = envelope.as_any();
        assert!(trait_object.is::<DefaultEnvelopeProxy<Ping>>());

        let envelope = Envelope::<Dummy>::with_proxy(Box::new(timed_proxy));
        let trait_object = envelope.as_any();
        assert!(trait_object.is::<TimedEnvelopeProxy<Ping>>());
    }
}

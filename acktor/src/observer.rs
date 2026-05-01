//! Observer pattern for actors.
//!
//! This module provides a way to implement the observer pattern for actors.
//!

use std::fmt::{self, Debug};
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};

use ahash::HashSet;
use tracing::{debug, warn};

use crate::actor::{Actor, ActorContext};
use crate::address::{Recipient, Sender, SenderMeta};
use crate::message::{Handler, Message};
use crate::utils::{ShortName, debug_trace};

/// Notifies all observers with the given event asynchronously, cleaning up closed observers.
#[doc(hidden)]
#[macro_export]
macro_rules! __notify_observers {
    ($observers:expr, $event:expr) => {
        let mut should_clean = false;
        for observer in $observers.iter() {
            #[cfg(feature = "bottleneck-warning")]
            if observer.capacity() == 0 {
                tracing::debug!("Actor {} is full", observer.index());
            }
            if observer.do_send($event.clone()).await.is_err() {
                should_clean = true;
            }
        }
        if should_clean {
            $observers.retain(|observer| !observer.is_closed())
        }
    };
}

/// Tries to notify all observers with the given event, cleaning up closed observers.
#[doc(hidden)]
#[macro_export]
macro_rules! __try_notify_observers {
    ($observers:expr, $event:expr) => {
        let mut should_clean = false;
        for observer in $observers.iter() {
            #[cfg(feature = "bottleneck-warning")]
            if observer.capacity() == 0 {
                tracing::debug!("Actor {} is full", observer.index());
            }
            if let Err($crate::error::SendError::Closed(_)) = observer.try_do_send($event.clone()) {
                should_clean = true;
            }
        }
        if should_clean {
            $observers.retain(|observer| !observer.is_closed())
        }
    };
}

#[doc(inline)]
pub use crate::__notify_observers as notify_observers;
#[doc(inline)]
pub use crate::__try_notify_observers as try_notify_observers;

/// Container for observers.
///
/// Use [`register_observer`][SubjectActor::register_observer]/
/// [`unregister_observer`][SubjectActor::unregister_observer] to insert into or remove from this
/// set, otherwise the tracing log is bypassed.
#[derive(Debug)]
#[repr(transparent)]
pub struct ObserverSet<Event>(HashSet<Recipient<Event>>)
where
    Event: Message;

impl<Event> Default for ObserverSet<Event>
where
    Event: Message,
{
    fn default() -> Self {
        Self(HashSet::default())
    }
}

impl<Event> ObserverSet<Event>
where
    Event: Message,
{
    /// Constructs a new empty [`ObserverSet`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Event> Deref for ObserverSet<Event>
where
    Event: Message,
{
    type Target = HashSet<Recipient<Event>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Event> DerefMut for ObserverSet<Event>
where
    Event: Message,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Describes the behavior of an actor which works as a subject of the given event type.
///
/// An actor could be the subject of multiple event types, just implement this trait multiple times.
pub trait SubjectActor<Event>: Actor
where
    Event: Message + Clone,
{
    /// Returns a mutable reference to the set of observers for this event type.
    fn observers_mut(&mut self) -> &mut ObserverSet<Event>;

    /// Registers an observer.
    fn register_observer(&mut self, observer: Recipient<Event>) {
        debug!("Register actor {} as observer", observer.index());

        self.observers_mut().insert(observer);
    }

    /// Unregisters an observer.
    fn unregister_observer(&mut self, observer: Recipient<Event>) {
        if self.observers_mut().remove(&observer) {
            debug!("Unregister actor {} as observer", observer.index());
        }
    }

    /// Notifies all observers.
    ///
    /// This method will wait until there is capacity in the mailbox of the observer.
    fn notify_observers(&mut self, event: Event) -> impl Future<Output = ()> + Send {
        async move {
            notify_observers!(self.observers_mut(), event);
        }
    }

    /// Notifies all observers.
    ///
    /// This method will return immediately if there is no capacity in the mailbox of the observer.
    fn try_notify_observers(&mut self, event: Event) {
        try_notify_observers!(self.observers_mut(), event);
    }
}

/// A message which is used to register/unregister an observer.
///
/// `Handler<Observer<M>>` is implemented for all actors which implement `SubjectActor<M>`
///  automatically.
pub enum Observer<M>
where
    M: Message,
{
    /// Register an observer.
    Register(Recipient<M>),
    /// Unregister an observer.
    Unregister(Recipient<M>),
}

impl<M> Debug for Observer<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Observer::Register(recipient) => f
                .debug_tuple(&format!("{}::Register", ShortName::of::<Self>()))
                .field(&recipient.index())
                .finish(),
            Observer::Unregister(recipient) => f
                .debug_tuple(&format!("{}::Unregister", ShortName::of::<Self>()))
                .field(&recipient.index())
                .finish(),
        }
    }
}

impl<M> Message for Observer<M>
where
    M: Message,
{
    type Result = ();
}

impl<A, M> Handler<Observer<M>> for A
where
    A: SubjectActor<M>,
    M: Message + Clone,
{
    type Result = ();

    fn handle(
        &mut self,
        msg: Observer<M>,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Self::Result> + Send {
        debug_trace!("Handle command {:?}", msg);

        match msg {
            Observer::Register(recipient) => {
                if recipient.index() == ctx.index() {
                    warn!("Could not register the actor itself as its observer");
                    return future::ready(());
                }
                self.register_observer(recipient);
            }
            Observer::Unregister(recipient) => {
                self.unregister_observer(recipient);
            }
        }

        future::ready(())
    }
}

#[cfg(feature = "identifier")]
impl<M> crate::stable_type_id::HasStableTypeId for Observer<M>
where
    M: Message + crate::stable_type_id::HasStableTypeId,
{
    const STABLE_TYPE_ID: crate::stable_type_id::StableTypeId =
        crate::stable_type_id::StableTypeId::from_stable_type_name(concat!(
            module_path!(),
            "::",
            "Observer"
        ))
        .combine(M::STABLE_TYPE_ID.as_bytes());
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::test_utils::Ping;

    #[test]
    fn test_debug_fmt() {
        let (recipient, _rx) = Recipient::<Ping>::create(1);
        let recipient_index = recipient.index();

        let cmd: Observer<Ping> = Observer::Register(recipient.clone());
        assert_eq!(
            format!("{cmd:?}"),
            format!("Observer<Ping>::Register({recipient_index})")
        );

        let cmd: Observer<Ping> = Observer::Unregister(recipient);
        assert_eq!(
            format!("{cmd:?}"),
            format!("Observer<Ping>::Unregister({recipient_index})")
        );
    }
}

//! Observer pattern for actors.
//!
//! This module provides a way to implement the observer pattern for actors.
//!

use std::fmt;
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};

use rustc_hash::FxHashSet as HashSet;
use tracing::{debug, warn};

use acktor_macros::debug_trace;
pub use acktor_macros::{notify_observers, try_notify_observers};

use crate::actor::{Actor, ActorContext};
use crate::address::{Recipient, Sender, SenderIndex};
use crate::message::{Handler, Message};

/// Container for observers.
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
    /// Returns a mutable reference to the observers.
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
pub enum Observer<M>
where
    M: Message,
{
    /// Register an observer.
    Register(Recipient<M>),
    /// Unregister an observer.
    Unregister(Recipient<M>),
}

impl<M> fmt::Debug for Observer<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Observer::Register(recipient) => f.debug_tuple("Register").field(&recipient).finish(),
            Observer::Unregister(recipient) => {
                f.debug_tuple("Unregister").field(&recipient).finish()
            }
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

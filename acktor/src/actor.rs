use std::any::Any;
use std::fmt::Display;
use std::panic::{self, AssertUnwindSafe};

use futures_util::future::FutureExt;
use tracing::{Instrument, Span, debug, error, error_span, warn};

use crate::address::{Address, Mailbox, Recipient, Sender};
use crate::errors::{BoxError, ErrorReport};
use crate::supervisor::SupervisionEvent;
use crate::utils::panic_info_to_string;

/// Actor index type.
pub type ActorId = u64;

pub use tokio::task::JoinHandle;

/// State of an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ActorState {
    Unstarted,
    Starting,
    Running,
    Stopping,
    Stopped,
}

impl TryFrom<u8> for ActorState {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ActorState::Unstarted),
            1 => Ok(ActorState::Starting),
            2 => Ok(ActorState::Running),
            3 => Ok(ActorState::Stopping),
            4 => Ok(ActorState::Stopped),
            _ => Err(()),
        }
    }
}

/// Return value of [`Actor::stopping`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Stopping {
    /// The actor could not resume by itself. Stop the actor.
    Stop,
    /// The actor could resume by itself.
    Continue,
}

/// An actor.
pub trait Actor: Sized + Send + 'static {
    /// The execution context type for this actor.
    type Context: ActorContext<Self>;
    // NOTE: this bound is chosen to be compatible with `StdError`, `Box<dyn Error>` and
    // `anyhow::Error`
    /// The error type returned by lifecycle hooks and message handlers.
    type Error: Into<BoxError> + Display + Send + 'static;

    /// Invoked before an actor is spawned into the tokio runtime. The actor should be in
    /// [`Unstarted`][ActorState::Unstarted] state.
    ///
    /// This method is used to perform initialization tasks or spawn child actors. In the default
    /// [`Context`][crate::context::Context] implementation, it is not spawned into the tokio
    /// runtime and it is outside of the processing loop. Thus it will be invoked only once
    /// synchronously. The actor will enter the [`Starting`][ActorState::Starting] state after
    /// this method returns.
    ///
    /// Panics in this method propagate to the caller of [`run`][Actor::run].
    #[allow(unused_variables)]
    fn pre_start(&mut self, ctx: &mut Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked after an actor is spawned into the tokio runtime. The actor should be in
    /// [`Starting`][ActorState::Starting] state.
    ///
    /// This method is used to perform additional initialization. In the default
    /// [`Context`][crate::context::Context] implementation, it is spawned into the tokio runtime
    /// and it is outside of the processing loop, which means it will be invoked once and only
    /// once, asynchronously. The actor will enter the [`Running`][ActorState::Running] state
    /// after this method returns.
    ///
    /// Panics in this method terminates the actor immediately and will be notified to the
    /// supervisor if there is one.
    #[allow(unused_variables)]
    fn post_start(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Invoked when an actor is being stopped. The actor should be in
    /// [`Stopping`][ActorState::Stopping] state.
    ///
    /// This method is used to make decisions about whether to stop or to restart the actor.
    #[allow(unused_variables)]
    fn stopping(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<Stopping, Self::Error>> + Send {
        std::future::ready(Ok(Stopping::Stop))
    }

    /// Invoked after an actor is stopped. The actor should be in [`Stopped`][ActorState::Stopped]
    /// state.
    ///
    /// This method is used to perform cleanup tasks or spawn new actors.
    #[allow(unused_variables)]
    fn post_stop(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Starts an actor and spawns it to the tokio runtime, returns its actor address and the
    /// join handle.
    fn run<S>(self, label: S) -> Result<(Address<Self>, JoinHandle<()>), Self::Error>
    where
        S: AsRef<str>,
    {
        let ctx = Self::Context::new(label.as_ref().to_string());
        let span = error_span!("Actor", id = ctx.address().index(), label = ctx.label());
        ctx.run(self, span)
    }

    /// Creates a new actor, starts it and spawns it to the tokio runtime, returns its actor
    /// address and the join handle.
    fn create<S, F>(label: S, f: F) -> Result<(Address<Self>, JoinHandle<()>), Self::Error>
    where
        S: AsRef<str>,
        F: FnOnce(&mut Self::Context) -> Result<Self, Self::Error>,
    {
        let mut ctx = Self::Context::new(label.as_ref().to_string());
        let span = error_span!("Actor", id = ctx.address().index(), label = ctx.label());
        let actor = {
            let _enter = span.enter();
            f(&mut ctx)?
        };
        ctx.run(actor, span)
    }

    /// Like [`create`][Self::create] but allows the caller to specify the parent tracing span.
    ///
    /// - `Some(&span)` — use `span` as the parent.
    /// - `None` — create the span as a new root (no parent).
    ///
    /// Use this when you want to control an actor's span hierarchy independently of whatever
    /// span happens to be entered at the call site.
    fn create_in_span<S, F>(
        label: S,
        parent_span: Option<&Span>,
        f: F,
    ) -> Result<(Address<Self>, JoinHandle<()>), Self::Error>
    where
        S: AsRef<str>,
        F: FnOnce(&mut Self::Context) -> Result<Self, Self::Error>,
    {
        let mut ctx = Self::Context::new(label.as_ref().to_string());
        let parent_span = parent_span.and_then(|s| s.id());
        let span = error_span!(
            parent: parent_span,
            "Actor",
            id = ctx.address().index(),
            label = ctx.label(),
        );
        let actor = {
            let _enter = span.enter();
            f(&mut ctx)?
        };
        ctx.run(actor, span)
    }

    /// Opt-in hook that turns an [`Address<A>`] into a type-erased trait object which can be
    /// downcast into a concrete [`Recipient<M>`], where `M` is a specific message type
    /// chosen in the overridden implementation of this method.
    ///
    /// Sometimes users may need to convert a `Recipient<N>` backed by an `Address<A>` into a
    /// `Recipient<M>`. If the actor type `A` is known, users can retrieve the `Address<A>` by
    /// downcasting the trait object in the `Recipient<N>`, and then the `Address<A>` can be
    /// converted into a `Recipient<M>`. However, if the concrete actor type `A` is not known
    /// (e.g., in a function receives a `Recipient<N>` backed by several different actor types),
    /// this approach does not work.
    ///
    /// This hook allows users to provide a function `f`, which defines a two-step conversion from
    /// an `Address<A>` to a `Recipient<M>` first, with `M` being a specific message type chosen
    /// by the user, and then to a type-erased `Box<dyn Any + Send + Sync>`. Returning `Some(f)`
    /// causes [`Address::new`] to bake `f` into every address for this actor. To convert a
    /// `Recipient<N>` into a `Recipient<M>`, users can use the [`Sender::type_erased_recipient`]
    /// method, which will invoke the function `f` and return the type-erased trait object, and
    /// convert the type-erased trait object back into a `Recipient<M>` by downcasting.
    ///
    /// Crates that extend actors with extra capabilities based on this feature can provide a
    /// macro which overrides this method automatically for their users to avoid boilerplate.
    #[cfg(feature = "type-erased-recipient-hook")]
    #[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
    fn type_erased_recipient_fn() -> Option<TypeErasedRecipientFn<Self>> {
        None
    }
}

/// The execution context of an actor.
///
/// Each actor is associated with a context which manages its lifecycle and communication
/// channels. The actor's associated type [`Context`][Actor::Context] defines the specific context
/// type to use. A context type must implement this trait.
pub trait ActorContext<A>: Sized + Send + 'static
where
    A: Actor<Context = Self>,
{
    // required methods

    /// Constructs a new actor context.
    fn new(label: String) -> Self;

    /// Returns the index of the actor.
    fn index(&self) -> ActorId;

    /// Returns the label of the actor.
    fn label(&self) -> &str;

    /// Returns the address of the actor.
    fn address(&self) -> Address<A>;

    /// Moves the mailbox of the actor out of the context, leaving `None` in its place.
    ///
    /// Typically the address and the mailbox are created together in the constructor of the
    /// context. However, since the [`process`][Self::process] method consumes the mailbox, the
    /// context needs to provide a way to move the mailbox out of itself so that it can be
    /// passed into the [`process`][Self::process] method.
    ///
    /// # Example
    ///
    /// A typical implementation stores the mailbox as an `Option<Mailbox<A>>` field and
    /// delegates to [`Option::take`]:
    ///
    /// ```ignore
    /// struct MyContext<A: Actor<Context = Self>> {
    ///     mailbox: Option<Mailbox<A>>,
    ///     // ... other fields (address, state, etc.)
    /// }
    ///
    /// impl<A: Actor<Context = Self>> ActorContext<A> for MyContext<A> {
    ///     fn take_mailbox(&mut self) -> Option<Mailbox<A>> {
    ///         self.mailbox.take()
    ///     }
    ///
    ///     // ... other trait methods
    /// }
    /// ```
    ///
    /// The first call returns `Some(mailbox)`; subsequent calls return `None`.
    fn take_mailbox(&mut self) -> Option<Mailbox<A>>;

    /// Returns the state of the actor.
    fn state(&self) -> ActorState;

    /// Sets the state of the actor.
    fn set_state(&mut self, state: ActorState);

    /// The message processing loop of the actor.
    ///
    /// This method is invoked by [`process`][Self::process]. It is responsible for
    /// receiving messages from the mailbox and handling them.
    fn process_loop(
        &mut self,
        actor: &mut A,
        mailbox: &mut Mailbox<A>,
    ) -> impl Future<Output = Result<(), A::Error>> + Send;

    // provided methods

    /// Stops the actor.
    ///
    /// This method will switch the actor to the [`Stopping`][ActorState::Stopping] state.
    fn stop(&mut self) {
        self.set_state(ActorState::Stopping);
    }

    /// Terminates the actor.
    ///
    /// This method will switch the actor to the [`Stopped`][ActorState::Stopped] state.
    fn terminate(&mut self) {
        self.set_state(ActorState::Stopped);
    }

    /// Returns a reference to the supervisor of the actor, if any.
    ///
    /// Override the [`supervisor`][ActorContext::supervisor] method and the
    /// [`set_supervisor`][ActorContext::set_supervisor] method to opt-in the supervisor feature.
    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<A>>> {
        None
    }

    /// Sets a supervisor.
    ///
    /// Override the [`supervisor`][ActorContext::supervisor] method and the
    /// [`set_supervisor`][ActorContext::set_supervisor] method to opt-in the supervisor feature.
    #[allow(unused_variables)]
    fn set_supervisor(&mut self, supervisor: Option<Recipient<SupervisionEvent<A>>>) {}

    /// Notifies the supervisor for an event.
    ///
    /// This method will wait until there is capacity in the mailbox of the supervisor.
    fn notify_supervisor(&mut self, event: SupervisionEvent<A>) -> impl Future<Output = ()> + Send {
        async move {
            if let Some(supervisor) = self.supervisor() {
                let _ = supervisor.do_send(event).await;
            } else {
                match event {
                    SupervisionEvent::Warn(actor, e) => {
                        warn!("Actor {} error: {}", actor.index(), e.into().report());
                    }
                    SupervisionEvent::Terminated(actor, Some(e)) => {
                        error!("Actor {} error: {}", actor.index(), e.into().report());
                    }
                    _ => {}
                }
            }
        }
    }

    /// Notifies the supervisor for an event.
    ///
    /// This method will return immediately if there is no capacity in the mailbox of the
    /// supervisor.
    fn try_notify_supervisor(&mut self, event: SupervisionEvent<A>) {
        if let Some(supervisor) = self.supervisor() {
            let _ = supervisor.try_do_send(event);
        } else {
            match event {
                SupervisionEvent::Warn(actor, e) => {
                    warn!("Actor {} error: {}", actor.index(), e.into().report());
                }
                SupervisionEvent::Terminated(actor, Some(e)) => {
                    error!("Actor {} error: {}", actor.index(), e.into().report());
                }
                _ => {}
            }
        }
    }

    /// Starts the actor and returns its address and a join handle.
    ///
    /// This method consumes the context and the actor.
    fn run(mut self, mut actor: A, span: Span) -> Result<(Address<A>, JoinHandle<()>), A::Error> {
        let address = self.address();

        let mailbox = self.take_mailbox().expect(
            "ActorContext::take_mailbox() returned None on first call to run(); \
             custom ActorContext implementations must provide a mailbox in new()",
        );

        let index = self.index();
        #[cfg(feature = "tokio-tracing")]
        let label = self.label().to_string();

        {
            let _enter = span.enter();
            let result = panic::catch_unwind(AssertUnwindSafe(|| actor.pre_start(&mut self)));
            match result {
                Ok(r) => r?,
                Err(info) => {
                    let msg: String = panic_info_to_string(&*info);
                    error!("Actor {} panicked in pre_start: {}", index, msg);
                    panic::resume_unwind(info);
                }
            }
            self.set_state(ActorState::Starting);
        }

        let future = async move {
            match self.process(&mut actor, mailbox).await {
                Ok(Ok(_)) => {
                    self.try_notify_supervisor(SupervisionEvent::Terminated(self.address(), None));
                }
                Ok(Err(e)) => {
                    self.try_notify_supervisor(SupervisionEvent::Terminated(
                        self.address(),
                        Some(e),
                    ));
                }
                Err(e) => {
                    let msg: String = panic_info_to_string(&*e);
                    self.try_notify_supervisor(SupervisionEvent::Panicked(self.address(), msg));
                }
            }

            debug!("Actor {} is stopped", index);
        }
        .instrument(span.or_current())
        .boxed();

        #[cfg(not(feature = "tokio-tracing"))]
        let join_handle = tokio::spawn(future);
        #[cfg(feature = "tokio-tracing")]
        let join_handle = tokio::task::Builder::new()
            .name(&label)
            .spawn(future)
            .unwrap();

        Ok((address, join_handle))
    }

    /// The main processing flow of the actor.
    ///
    /// This method is invoked by [`run`][Self::run]. It is responsible for invoking the
    /// [`post_start`][Actor::post_start] and the [`post_stop`][Actor::post_stop] lifecycle
    /// hooks. It is the user's responsibility to choose where to invoke the
    /// [`stopping`][Actor::stopping] lifecycle hook. The default implementation handles the
    /// [`stopping`][Actor::stopping] in the [`process_loop`][Self::process_loop], but users can
    /// handle it here by overriding the default implementation.
    ///
    /// # Return value
    ///
    /// The nested `Result<Result<(), A::Error>, Box<dyn Any + Send>>` encodes three outcomes:
    ///
    /// - `Ok(Ok(()))` — the actor started, ran, and stopped cleanly.
    /// - `Ok(Err(error))` — a lifecycle method returned an [`Actor::Error`] (from
    ///   [`post_start`][Actor::post_start], [`process_loop`][Self::process_loop], or
    ///   [`post_stop`][Actor::post_stop]).
    /// - `Err(panic_payload)` — a lifecycle method panicked. The payload is the value caught by
    ///   [`catch_unwind`][panic::catch_unwind]. When [`process_loop`][Self::process_loop] panics,
    ///   [`post_stop`][Actor::post_stop] is skipped because the actor's state is not assumed to
    ///   be safe to observe after a panic.
    ///
    /// If both [`process_loop`][Self::process_loop] and [`post_stop`][Actor::post_stop] return
    /// errors (neither panics), the `process_loop` error is returned and the `post_stop` error
    /// is discarded.
    fn process(
        &mut self,
        actor: &mut A,
        mut mailbox: Mailbox<A>,
    ) -> impl Future<Output = Result<Result<(), A::Error>, Box<dyn Any + Send>>> + Send {
        async move {
            let result = AssertUnwindSafe(actor.post_start(self))
                .catch_unwind()
                .await
                .inspect_err(|e| {
                    let msg: String = panic_info_to_string(e);
                    error!("Actor {} panicked in post_start: {}", self.index(), msg);
                });

            if !matches!(result, Ok(Ok(()))) {
                self.set_state(ActorState::Stopped);
                return result;
            }

            debug!("Actor {} is started", self.index());
            self.set_state(ActorState::Running);

            let result = AssertUnwindSafe(self.process_loop(actor, &mut mailbox))
                .catch_unwind()
                .await
                .inspect_err(|e| {
                    let msg: String = panic_info_to_string(e);
                    error!("Actor {} panicked in process_loop: {}", self.index(), msg);
                });

            if self.state() != ActorState::Stopped {
                self.set_state(ActorState::Stopped);
            }

            // drop mailbox so any actor holds the address of this actor will not be able to send
            // messages
            drop(mailbox);

            // if the process_loop panicked, post_stop is skipped since the actor's state is not
            // predictable once a panic happens, and we don't want to make it worse by invoking
            // user code in post_stop
            let result = result?;

            let result_post_stop = AssertUnwindSafe(actor.post_stop(self))
                .catch_unwind()
                .await
                .inspect_err(|e| {
                    let msg: String = panic_info_to_string(e);
                    error!("Actor {} panicked in post_stop: {}", self.index(), msg);
                })?;

            match (result, result_post_stop) {
                (Err(e), Err(post_stop_err)) => {
                    debug!(
                        "Actor {} post_stop error discarded: {}",
                        self.index(),
                        post_stop_err,
                    );
                    Ok(Err(e))
                }
                (Err(e), Ok(())) => Ok(Err(e)),
                (Ok(()), result_post_stop) => Ok(result_post_stop),
            }
        }
    }
}

/// Return type of [`Sender::type_erased_recipient`] and [`TypeErasedRecipientFn`].
///
/// It wraps a type-erased trait object with its original type name for better error messages when
/// downcasting fails.
#[cfg(feature = "type-erased-recipient-hook")]
#[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
pub struct TypeErasedRecipient {
    inner: Box<dyn Any + Send + Sync>,
    type_name: &'static str,
}

#[cfg(feature = "type-erased-recipient-hook")]
#[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
impl TypeErasedRecipient {
    /// Constructs a new [`TypeErasedRecipient`] from a concrete value.
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            inner: Box::new(value),
            type_name: std::any::type_name::<T>(),
        }
    }

    /// Attempts to downcast the type-erased recipient to a concrete type.
    pub fn downcast<T>(self) -> Result<Box<T>, (Self, String)>
    where
        T: Any + Send + Sync,
    {
        self.inner.downcast::<T>().map_err(|inner| {
            let error_msg = format!(
                "Could not downcast TypeErasedRecipient: expected type {}, actual type {}",
                crate::utils::ShortName::of::<T>(),
                crate::utils::ShortName(self.type_name),
            );
            (
                Self {
                    inner,
                    type_name: self.type_name,
                },
                error_msg,
            )
        })
    }
}

/// Function-pointer type returned by [`Actor::type_erased_recipient_fn`], which converts an
/// [`Address<A>`] into a type-erased trait object that can be downcast into a concrete
/// [`Recipient<M>`].
#[cfg(feature = "type-erased-recipient-hook")]
#[cfg_attr(docsrs, doc(cfg(feature = "type-erased-recipient-hook")))]
pub type TypeErasedRecipientFn<A> = fn(&Address<A>) -> TypeErasedRecipient;

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_actor_state() {
        assert_eq!(ActorState::try_from(0), Ok(ActorState::Unstarted));
        assert_eq!(ActorState::try_from(1), Ok(ActorState::Starting));
        assert_eq!(ActorState::try_from(2), Ok(ActorState::Running));
        assert_eq!(ActorState::try_from(3), Ok(ActorState::Stopping));
        assert_eq!(ActorState::try_from(4), Ok(ActorState::Stopped));
        assert_eq!(ActorState::try_from(5), Err(()));
    }
}

#[cfg(feature = "erased-recipient")]
use std::any::Any;
use std::error::Error;
use std::fmt::Display;
use std::panic::AssertUnwindSafe;

use futures_util::future::{FutureExt, TryFutureExt};
use tokio::task::JoinHandle;
use tracing::{Instrument, Span, debug, error, error_span, warn};

use acktor_macros::report;

use crate::address::{Address, Mailbox, Recipient, Sender};
use crate::supervisor::SupervisionEvent;

/// Actor index type.
pub type ActorId = u64;

/// Function-pointer type returned by [`Actor::erased_recipient_fn`], which converts an
/// [`Address<A>`] into a type-erased trait object which can be downcast into a concrete
/// [`Recipient<M>`].
#[cfg(feature = "erased-recipient")]
#[cfg_attr(docsrs, doc(cfg(feature = "erased-recipient")))]
pub type ErasedRecipientFn<A> = fn(&Address<A>) -> Box<dyn Any + Send + Sync>;

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

/// Describes an actor.
pub trait Actor: Sized + Send + 'static {
    /// The execution context type for this actor.
    type Context: ActorContext<Self>;
    // NOTE: this bound is chosen to be compatible with `std::error::Error`, `Box<dyn Error>`
    // and `anyhow::Error`
    /// The error type returned by lifecycle hooks and message handlers.
    type Error: Into<Box<dyn Error + Send + Sync>> + Display + Send + 'static;

    /// Invoked before an actor is spawned into the tokio runtime.
    /// The actor should be in [`Unstarted`][ActorState::Unstarted] state.
    ///
    /// This method is used to perform initialization tasks or spawn child actors.
    /// In the default [`Context`][crate::context::Context] implementation, it is not spawned
    /// into the tokio runtime and it is outside of the processing loop. Thus it will be invoked
    /// only once synchronously. The actor will enter the [`Starting`][ActorState::Starting] state
    /// after this method returns.
    ///
    /// Panics in this method will prevent the actor being spawned into the runtime.
    /// [`run`][Actor::run] will return an error to the caller in this case.
    #[allow(unused_variables)]
    fn pre_start(&mut self, ctx: &mut Self::Context) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Invoked after an actor is spawned into the tokio runtime.
    /// The actor should be in [`Starting`][ActorState::Starting] state.
    ///
    /// This method is used to perform additional initialization.
    /// In the default [`Context`][crate::context::Context] implementation, it is spawned into
    /// the tokio runtime and it is outside of the processing loop. Thus it will be invoked only
    /// once asynchronously. The actor will enter the [`Running`][ActorState::Running] state
    /// after this method returns.
    ///
    /// Panics in this method will be notified to the supervisor if there is one.
    #[allow(unused_variables)]
    fn post_start(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Invoked when an actor is being stopped.
    /// The actor should be in [`Stopping`][ActorState::Stopping] state.
    ///
    /// This method is used to make decisions about whether to stop or to restart the actor.
    #[allow(unused_variables)]
    fn stopping(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<Stopping, Self::Error>> + Send {
        std::future::ready(Ok(Stopping::Stop))
    }

    /// Invoked after an actor is stopped.
    /// The actor should be in [`Stopped`][ActorState::Stopped] state.
    ///
    /// This method is used to perform cleanup tasks or spawn new actors.
    #[allow(unused_variables)]
    fn post_stop(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    /// Starts an actor and spawns it to the tokio runtime,
    /// returns its actor address and the join handle.
    fn run<S>(self, label: S) -> Result<(Address<Self>, JoinHandle<()>), Self::Error>
    where
        S: AsRef<str>,
    {
        let ctx = Self::Context::new(label.as_ref().to_string());
        let span = error_span!("Actor", id = ctx.address().index(), label = ctx.label());
        ctx.run(self, span)
    }

    /// Constructs a new actor, starts it and spawns it to the tokio runtime,
    /// returns its actor address and the join handle.
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

    /// Optional conversion hook that turns an [`Address<A>`] into a type-erased trait object
    /// which can be downcast into a concrete [`Recipient<M>`].
    ///
    /// It is intended for use cases where you want to convert from any [`Recipient<N>`] into
    /// a [`Recipient<M>`] without knowing the concrete actor type `A`. Here `N` can be any
    /// message type which the actor has implemented [`Handler<N>`][crate::message::Handler<N>]
    /// for, and `M` is the type chosen by the implementor when they override this method. The
    /// [`ErasedRecipientFn`] will convert an [`Address<A>`] into [`Recipient<M>`] first, and then
    /// type-erase it into a `Box<dyn Any + Send + Sync>`.
    ///
    /// Returning `Some(f)` causes [`Address::new`] to bake `f` into every address for this
    /// actor; Returning `None` (the default) means this actor type does not opt into any
    /// such conversion.
    ///
    /// Crates that extend actors with extra capabilities based on this feature should ship
    /// an attribute macro that generates the overridden method.
    #[cfg(feature = "erased-recipient")]
    #[cfg_attr(docsrs, doc(cfg(feature = "erased-recipient")))]
    fn erased_recipient_fn() -> Option<ErasedRecipientFn<Self>> {
        None
    }
}

/// Describes the execution context of an actor.
pub trait ActorContext<A>: Sized + Send + 'static
where
    A: Actor<Context = Self>,
{
    // required methods

    /// Constructs a new context.
    fn new(label: String) -> Self;

    /// Returns the index of the actor.
    fn index(&self) -> ActorId;

    /// Returns the label of the actor.
    fn label(&self) -> &str;

    /// Returns the address of the actor.
    fn address(&self) -> Address<A>;

    /// Returns the mailbox of the actor.
    fn take_mailbox(&mut self) -> Option<Mailbox<A>>;

    /// Returns the state of the actor.
    fn state(&self) -> ActorState;

    /// Sets the state of the actor.
    fn set_state(&mut self, state: ActorState);

    /// Runs the main processing loop of the actor.
    ///
    /// This method is called after [`post_start`][Actor::post_start] and drives the actor until
    /// it stops. It is responsible for receiving messages from the mailbox and dispatching them
    /// to the actor.
    fn processing(
        &mut self,
        actor: &mut A,
        mailbox: Mailbox<A>,
    ) -> impl Future<Output = Result<(), A::Error>> + Send;

    // provided methods

    /// Returns the address of the supervisor.
    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<A>>> {
        None
    }

    /// Sets a supervisor.
    #[allow(unused_variables)]
    fn set_supervisor(&mut self, supervisor: Option<Recipient<SupervisionEvent<A>>>) {}

    /// Sets an error during message processing.
    #[allow(unused_variables)]
    fn set_error(&mut self, error: A::Error) {}

    /// Drains the mailbox of the actor.
    ///
    /// The default implementation does nothing. Users who need this functionality should
    /// implement it in their own context.
    fn drain_mailbox(&mut self) {}

    /// Stops the actor.
    ///
    /// This method will switch the actor to the [`Stopping`][ActorState::Stopping] state.
    fn stop(&mut self) {
        self.set_state(ActorState::Stopping);
    }

    /// Stops the actor and saves the error for reporting.
    ///
    /// This method will switch the actor to the [`Stopping`][ActorState::Stopping] state.
    fn stop_with_error(&mut self, error: A::Error) {
        self.set_error(error);
        self.stop();
    }

    /// Terminates the actor.
    ///
    /// This method will switch the actor to the [`Stopped`][ActorState::Stopped] state.
    fn terminate(&mut self) {
        self.set_state(ActorState::Stopped);
    }

    /// Terminates the actor and saves the error for reporting.
    ///
    /// This method will switch the actor to the [`Stopped`][ActorState::Stopped] state.
    fn terminate_with_error(&mut self, error: A::Error) {
        self.set_error(error);
        self.terminate();
    }

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
                        warn!("Actor {} error: {}", actor.index(), report!(e.into()));
                    }
                    SupervisionEvent::Terminated(actor, Some(e)) => {
                        error!("Actor {} error: {}", actor.index(), report!(e.into()));
                    }
                    _ => {}
                }
            }
        }
    }

    /// Notifies the supervisor for an event.
    ///
    /// This method will return immediately if there is no capacity in the mailbox of
    /// the supervisor.
    fn try_notify_supervisor(&mut self, event: SupervisionEvent<A>) {
        if let Some(supervisor) = self.supervisor() {
            let _ = supervisor.try_do_send(event);
        } else {
            match event {
                SupervisionEvent::Warn(actor, e) => {
                    warn!("Actor {} error: {}", actor.index(), report!(e.into()));
                }
                SupervisionEvent::Terminated(actor, Some(e)) => {
                    error!("Actor {} error: {}", actor.index(), report!(e.into()));
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

        // unwrap() is safe
        // Context is always created with a mailbox, so when run() is called, mailbox is always Some
        // run() consumes the mailbox, so it will not be able to be used again
        let mailbox = self.take_mailbox().unwrap();

        {
            let _enter = span.enter();
            actor.pre_start(&mut self)?;
            self.set_state(ActorState::Starting);
        }

        let index = self.index();
        #[cfg(feature = "tokio-tracing")]
        let label = self.label().to_string();

        let future = async move {
            match self.processing(&mut actor, mailbox).await {
                Ok(_) => {
                    self.try_notify_supervisor(SupervisionEvent::Terminated(self.address(), None));
                }
                Err(e) => {
                    self.try_notify_supervisor(SupervisionEvent::Terminated(
                        self.address(),
                        Some(e),
                    ));
                }
            }

            debug!("Actor {} is stopped", index);
        };

        let future = AssertUnwindSafe(future)
            .catch_unwind()
            .unwrap_or_else(move |e| match e.downcast::<String>() {
                Ok(panic_msg) => error!("Actor {} is panicked: {}", index, panic_msg),
                Err(e) => match e.downcast::<&str>() {
                    Ok(panic_msg) => error!("Actor {} is panicked: {}", index, panic_msg),
                    Err(_) => {
                        error!(
                            "Actor {} is panicked: could not capture the panic message",
                            index
                        );
                    }
                },
            })
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
}

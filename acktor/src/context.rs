use tracing::{debug, warn};

use crate::actor::{Actor, ActorContext, ActorId, ActorState, Stopping};
use crate::address::{Address, Mailbox, Recipient, SenderInfo};
use crate::channel::mpsc;
use crate::drain::{DrainPolicy, Drainable, NoDrain};
use crate::envelope::EnvelopeProxy;
use crate::supervisor::SupervisionEvent;

/// The default mailbox capacity for actors.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 8;

/// The default implementation of an actor context.
///
/// The `D` type parameter selects the mailbox draining behavior. It defaults to [`NoDrain`],
/// which compiles the draining machinery out entirely, use
/// [`Context<Self, Drainable>`][crate::drain::Drainable] to gain
/// [`drain_mailbox`][Context::drain_mailbox].
#[derive(Debug)]
pub struct Context<A, D = NoDrain>
where
    A: Actor<Context = Self>,
    D: DrainPolicy,
{
    label: String,
    state: ActorState,
    doorplate: Address<A>,
    mailbox: Option<Mailbox<A>>,
    drain: D::State,
    error: Option<A::Error>, // error happened in message handlers
    supervisor: Option<Recipient<SupervisionEvent<A>>>,
}

impl<A, D> Context<A, D>
where
    A: Actor<Context = Self>,
    D: DrainPolicy,
{
    /// Constructs a new [`Context`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
            drain: D::State::default(),
            supervisor: None,
            error: None,
        }
    }

    /// Saves an error in message handlers.
    ///
    /// The actor will enter the [`Stopping`][ActorState::Stopping] state after processing
    /// the current message.
    pub fn save_error(&mut self, error: A::Error) {
        self.error = Some(error);
    }

    fn take_error(&mut self) -> Result<(), A::Error> {
        match self.error.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<A> Context<A, Drainable>
where
    A: Actor<Context = Self>,
{
    /// Schedules a one-time discard of messages already queued in the mailbox.
    ///
    /// It should be called from a message handler. The length of the mailbox is snapshotted
    /// right before the message handler is invoked, which means messages received during the
    /// current message handler will not be discarded.
    ///
    /// This method is only available on a context using the [`Drainable`] policy.
    pub fn drain_mailbox(&mut self) {
        self.drain.request();
    }
}

impl<A, D> ActorContext<A> for Context<A, D>
where
    A: Actor<Context = Self>,
    D: DrainPolicy,
{
    fn new(label: String) -> Self {
        Self::with_capacity(label, DEFAULT_MAILBOX_CAPACITY)
    }

    fn index(&self) -> ActorId {
        self.doorplate.index()
    }

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn address(&self) -> Address<A> {
        self.doorplate.clone()
    }

    fn take_mailbox(&mut self) -> Option<Mailbox<A>> {
        self.mailbox.take()
    }

    fn state(&self) -> ActorState {
        self.state
    }

    fn set_state(&mut self, state: ActorState) {
        self.state = state;
        self.try_notify_supervisor(SupervisionEvent::State(self.address(), state));
    }

    async fn run_loop(&mut self, actor: &mut A, mailbox: &mut Mailbox<A>) -> Result<(), A::Error> {
        while self.state() == ActorState::Running {
            match mailbox.recv().await {
                Ok(mut envelope) => {
                    D::snapshot(&mut self.drain, mailbox);
                    envelope.handle(actor, self).await;
                    D::apply(&mut self.drain, mailbox);
                    if self.error.is_some() && self.state() == ActorState::Running {
                        self.set_state(ActorState::Stopping);
                    }
                }
                Err(_) => {
                    warn!("Mailbox is dropped, terminate the actor");
                    self.set_state(ActorState::Stopped);
                }
            };

            match self.state() {
                ActorState::Stopping => {
                    let result = self.take_error();
                    // if `stopping` returns `Err`, the actor will stop, if there is a saved error,
                    // the error is returned, otherwise the error from `stopping` is returned
                    match actor.stopping(self).await {
                        Ok(Stopping::Stop) => return result,
                        Ok(Stopping::Continue) => {
                            // resumed by the actor itself
                            if let Err(e) = result {
                                self.try_notify_supervisor(SupervisionEvent::Warn(
                                    self.address(),
                                    e,
                                ))
                            };
                            self.set_state(ActorState::Running);
                        }
                        Err(e) => return result.or(Err(e)),
                    }
                }
                ActorState::Stopped => {
                    return self.take_error();
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<A>>> {
        self.supervisor.as_ref()
    }

    fn set_supervisor(&mut self, supervisor: Option<Recipient<SupervisionEvent<A>>>) {
        match supervisor {
            Some(supervisor) => {
                if supervisor.index() == self.index() {
                    warn!("Could not set the actor itself as its supervisor");
                    return;
                }
                debug!("Set actor {} as supervisor", supervisor.index());
                self.supervisor = Some(supervisor);
            }
            None => {
                if self.supervisor.take().is_some() {
                    debug!("Unset supervisor");
                }
            }
        }
    }
}

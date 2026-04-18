use tracing::{debug, warn};

use crate::actor::{Actor, ActorContext, ActorId, ActorState, Stopping};
use crate::address::{Address, Mailbox, Recipient, SenderId};
use crate::channel::mpsc;
use crate::envelope::EnvelopeProxy;
use crate::supervisor::SupervisionEvent;

/// The default mailbox capacity for actors.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 8;

/// The default implementation of an actor context.
#[derive(Debug)]
pub struct Context<A>
where
    A: Actor<Context = Self>,
{
    label: String,
    state: ActorState,
    doorplate: Address<A>,
    mailbox: Option<Mailbox<A>>,
    drain_mailbox: bool,
    error: Option<A::Error>, // error happened in message handlers
    supervisor: Option<Recipient<SupervisionEvent<A>>>,
}

impl<A> Context<A>
where
    A: Actor<Context = Self>,
{
    /// Constructs a new [`Context`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
            drain_mailbox: false,
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

    /// Terminates the actor and saves the error.
    ///
    /// The actor will enter the [`Stopped`][ActorState::Stopped] state after processing the
    /// current message.
    pub fn terminate_with_error(&mut self, error: A::Error) {
        self.save_error(error);
        self.terminate();
    }

    /// Schedules a one-time discard of messages already queued in the mailbox.
    ///
    /// Sets a flag; the processing loop acts on it on its next iteration by snapshotting
    /// `mailbox.len()` and discarding exactly that many messages. Messages enqueued after
    /// the snapshot are delivered normally.
    pub fn drain_mailbox(&mut self) {
        self.drain_mailbox = true;
    }

    fn take_error(&mut self) -> Result<(), A::Error> {
        match self.error.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<A> ActorContext<A> for Context<A>
where
    A: Actor<Context = Self>,
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

    async fn process_loop(
        &mut self,
        actor: &mut A,
        mailbox: &mut Mailbox<A>,
    ) -> Result<(), A::Error> {
        while self.state() == ActorState::Running {
            if self.drain_mailbox {
                let count = mailbox.len();
                for _ in 0..count {
                    // the mailbox contains `count` messages, so try_recv never fail
                    let _ = mailbox.try_recv();
                }
                self.drain_mailbox = false;
            }

            match mailbox.recv().await {
                Some(mut envelope) => {
                    envelope.handle(actor, self).await;
                    if self.error.is_some() && self.state() == ActorState::Running {
                        self.set_state(ActorState::Stopping);
                    }
                }
                None => {
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

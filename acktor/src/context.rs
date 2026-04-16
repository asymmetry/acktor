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

    /// Saves an error in message handlers. The actor will enter the
    /// [`Stopping`][ActorState::Stopping] state after processing the current message.
    pub fn save_error(&mut self, error: A::Error) {
        self.error = Some(error);
    }

    /// Terminates the actor and saves the error.
    ///
    /// This method will switch the actor to the [`Stopped`][ActorState::Stopped] state.
    pub fn terminate_with_error(&mut self, error: A::Error) {
        self.save_error(error);
        self.terminate();
    }

    /// Drains the mailbox of the actor.
    ///
    /// The mailbox is not reachable after the actor starts. This method only sets a flag, the
    /// processing loop will drain the mailbox based on this flag.
    pub fn drain_mailbox(&mut self) {
        self.drain_mailbox = true;
    }

    async fn processing_loop(
        &mut self,
        actor: &mut A,
        mailbox: &mut Mailbox<A>,
    ) -> Result<(), A::Error> {
        while self.state() == ActorState::Running {
            if self.drain_mailbox {
                let count = mailbox.len();
                for _ in 0..count {
                    if mailbox.try_recv().is_err() {
                        // the mailbox contains `count` messages, so try_recv should never fail
                        warn!("Unexpected error when draining the mailbox, terminate the actor");
                        self.set_state(ActorState::Stopped);

                        break;
                    }
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
                    let result = match self.error.take() {
                        Some(e) => Err(e),
                        None => Ok(()),
                    };
                    match actor.stopping(self).await? {
                        Stopping::Stop => {
                            return result;
                        }
                        Stopping::Continue => {
                            // resumed by the actor itself
                            if let Err(e) = result {
                                self.try_notify_supervisor(SupervisionEvent::Warn(
                                    self.address(),
                                    e,
                                ))
                            };
                            self.set_state(ActorState::Running);
                        }
                    }
                }
                ActorState::Stopped => {
                    return match self.error.take() {
                        Some(e) => Err(e),
                        None => Ok(()),
                    };
                }
                _ => {}
            }
        }

        Ok(())
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

    async fn processing(&mut self, actor: &mut A, mut mailbox: Mailbox<A>) -> Result<(), A::Error> {
        actor.post_start(self).await?;

        debug!("Actor {} is started", self.index());
        self.set_state(ActorState::Running);

        let result = self.processing_loop(actor, &mut mailbox).await;

        if self.state() != ActorState::Stopped {
            self.set_state(ActorState::Stopped);
        }

        // drop mailbox so any actor holds the address of this actor will not be able to send messages
        // after it is stopped
        drop(mailbox);

        let result_post_stop = actor.post_stop(self).await;

        result?;
        result_post_stop?;

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

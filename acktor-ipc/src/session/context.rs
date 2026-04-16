use tracing::{debug, warn};

use acktor::{
    Actor, ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, ErrorReport, Recipient,
    SenderId, address::Mailbox, channel::mpsc, envelope::EnvelopeProxy,
    supervisor::SupervisionEvent,
};

use super::Session;
use crate::errors::SessionError;

pub struct SessionContext {
    label: String,
    state: ActorState,
    doorplate: Address<Session>,
    mailbox: Option<Mailbox<Session>>,
    supervisor: Option<Recipient<SupervisionEvent<Session>>>,
}

impl SessionContext {
    /// Constructs a new [`SessionContext`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
            supervisor: None,
        }
    }

    async fn processing_loop(
        &mut self,
        actor: &mut Session,
        mailbox: &mut Mailbox<Session>,
    ) -> Result<(), SessionError> {
        while self.state() == ActorState::Running {
            tokio::select! {
                envelope = mailbox.recv() => {
                    match envelope {
                        Some(mut envelope) => {
                            envelope.handle(actor, self).await;
                        }
                        None => {
                            warn!("Mailbox is dropped, terminate the actor");
                            self.set_state(ActorState::Stopped);
                        }
                    }
                }
                received = actor.connection.recv() => {
                    match received {
                        Ok(msg) => {
                            if let Err(e) = actor.handle_ipc_message(msg, self).await {
                                warn!("Could not handle IPC message: {}", e.report());
                            }
                        }
                        Err(e) => {
                            warn!("Could not receive IPC message, terminate the actor: {}", e.report());
                            self.set_state(ActorState::Stopped);

                            return Err(SessionError::IoError(e));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl ActorContext<Session> for SessionContext {
    fn new(label: String) -> Self {
        Self::with_capacity(label, DEFAULT_MAILBOX_CAPACITY)
    }

    fn index(&self) -> u64 {
        self.doorplate.index()
    }

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn address(&self) -> Address<Session> {
        self.doorplate.clone()
    }

    fn take_mailbox(&mut self) -> Option<Mailbox<Session>> {
        self.mailbox.take()
    }

    fn state(&self) -> ActorState {
        self.state
    }

    fn set_state(&mut self, state: ActorState) {
        self.state = state;
    }

    async fn processing(
        &mut self,
        actor: &mut Session,
        mut mailbox: Mailbox<Session>,
    ) -> Result<(), SessionError> {
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

    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<Session>>> {
        self.supervisor.as_ref()
    }

    fn set_supervisor(&mut self, supervisor: Option<Recipient<SupervisionEvent<Session>>>) {
        match supervisor {
            Some(supervisor) => {
                if supervisor.index() == self.index() {
                    warn!("Could not set the actor itself as its supervisor");
                    return;
                }
                debug!("Set Actor {} as supervisor", supervisor.index());
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

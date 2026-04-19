use tracing::{debug, warn};

use acktor::{
    ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, ErrorReport, Recipient, SenderId,
    address::Mailbox, channel::mpsc, envelope::EnvelopeProxy, supervisor::SupervisionEvent,
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

    async fn process_loop(
        &mut self,
        actor: &mut Session,
        mailbox: &mut Mailbox<Session>,
    ) -> Result<(), SessionError> {
        while self.state() == ActorState::Running {
            tokio::select! {
                envelope = mailbox.recv() => {
                    match envelope {
                        Ok(mut envelope) => {
                            envelope.handle(actor, self).await;
                        }
                        _ => {
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
                            warn!(
                                "Could not receive IPC message, terminate the actor: {}",
                                e.report()
                            );
                            self.set_state(ActorState::Stopped);

                            if !matches!(
                                e.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::BrokenPipe
                            ) {
                                return Err(SessionError::IoError(e));
                            }
                        }
                    }
                }
            }
        }

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

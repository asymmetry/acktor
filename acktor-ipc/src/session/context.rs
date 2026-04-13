use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use acktor::{
    Actor, ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, Recipient, SenderId,
    address::Mailbox,
    envelope::{Envelope, EnvelopeProxy},
    macros::report,
    supervisor::SupervisionEvent,
};

use super::Session;

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
        Self::with_channel(label, tx, rx)
    }

    /// Constructs a new [`SessionContext`] with a specific [`channel`][mpsc::channel].
    pub fn with_channel(
        label: String,
        tx: mpsc::Sender<Envelope<Session>>,
        rx: mpsc::Receiver<Envelope<Session>>,
    ) -> Self {
        let address = Address::new(tx);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: address.clone(),
            mailbox: Some(Mailbox::new(rx)),
            supervisor: None,
        }
    }

    async fn processing_loop(
        &mut self,
        actor: &mut Session,
        mailbox: &mut Mailbox<Session>,
    ) -> Result<(), <Session as Actor>::Error> {
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
                                warn!("Could not handle IPC message: {}", report!(e));
                            }
                        }
                        Err(e) => {
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::PermissionDenied
                                    | std::io::ErrorKind::ConnectionRefused
                                    | std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::ConnectionAborted
                                    | std::io::ErrorKind::NotConnected
                                    | std::io::ErrorKind::BrokenPipe
                            ) {
                                info!("Connection is closed, stop the session");
                                self.set_state(ActorState::Stopped);
                            }
                            else {
                                warn!("Could not receive IPC message: {}", e);
                            }
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
    ) -> Result<(), <Session as Actor>::Error> {
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
                debug!("Set Actor {} as supervisor", supervisor.index());

                if supervisor.index() == self.index() {
                    warn!("Could not set the actor itself as its supervisor");
                    return;
                }
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

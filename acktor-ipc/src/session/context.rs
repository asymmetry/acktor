use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use acktor::{
    Actor, ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, Recipient, SenderIndex,
    address::Mailbox,
    envelope::{Envelope, EnvelopeProxy},
    macros::report,
    supervisor::SupervisionEvent,
};

use super::Session;
use crate::codec::DecodeContext;
use crate::ipc_method::IpcConnection;
use crate::remote_address::RemoteSender;

pub struct SessionContext<C>
where
    C: IpcConnection,
{
    label: String,
    state: ActorState,
    doorplate: Address<Session<C>>,
    mailbox: Option<Mailbox<Session<C>>>,
    supervisor: Option<Recipient<SupervisionEvent<Session<C>>>>,
    remote_sender: Arc<dyn RemoteSender + Send + Sync>,
}

impl<C> SessionContext<C>
where
    C: IpcConnection,
{
    /// Constructs a new [`SessionContext`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self::with_channel(label, tx, rx)
    }

    /// Constructs a new [`SessionContext`] with a specific [`channel`][mpsc::channel].
    pub fn with_channel(
        label: String,
        tx: mpsc::Sender<Envelope<Session<C>>>,
        rx: mpsc::Receiver<Envelope<Session<C>>>,
    ) -> Self {
        let address = Address::new(tx);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: address.clone(),
            mailbox: Some(Mailbox::new(rx)),
            supervisor: None,
            remote_sender: Arc::new(address),
        }
    }

    pub fn decode_context(&self) -> DecodeContext {
        self.remote_sender.clone()
    }

    pub fn remote_sender(&self) -> Arc<dyn RemoteSender + Send + Sync> {
        self.remote_sender.clone()
    }

    async fn processing_loop(
        &mut self,
        actor: &mut Session<C>,
        mailbox: &mut Mailbox<Session<C>>,
    ) -> Result<(), <Session<C> as Actor>::Error> {
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

impl<C> ActorContext<Session<C>> for SessionContext<C>
where
    C: IpcConnection,
{
    fn new(label: String) -> Self {
        Self::with_capacity(label, DEFAULT_MAILBOX_CAPACITY)
    }

    fn index(&self) -> usize {
        self.doorplate.index()
    }

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn address(&self) -> Address<Session<C>> {
        self.doorplate.clone()
    }

    fn take_mailbox(&mut self) -> Option<Mailbox<Session<C>>> {
        self.mailbox.take()
    }

    fn state(&self) -> ActorState {
        self.state
    }

    fn set_state(&mut self, state: ActorState) {
        self.state = state;
    }

    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<Session<C>>>> {
        self.supervisor.as_ref()
    }

    fn set_supervisor(&mut self, supervisor: Option<Recipient<SupervisionEvent<Session<C>>>>) {
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

    async fn processing(
        &mut self,
        actor: &mut Session<C>,
        mut mailbox: Mailbox<Session<C>>,
    ) -> Result<(), <Session<C> as Actor>::Error> {
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
}

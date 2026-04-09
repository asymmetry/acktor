use tokio::sync::mpsc;
use tracing::{debug, warn};

use acktor::{
    Actor, ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, Recipient,
    address::Mailbox,
    envelope::{Envelope, EnvelopeProxy},
    macros::report,
    supervisor::SupervisionEvent,
};

use super::Node;
use crate::ipc_method::IpcListener;

pub struct NodeContext<L>
where
    L: IpcListener,
{
    label: String,
    state: ActorState,
    doorplate: Address<Node<L>>,
    mailbox: Option<Mailbox<Node<L>>>,
}

impl<L> NodeContext<L>
where
    L: IpcListener,
{
    /// Constructs a new [`NodeContext`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self::with_channel(label, tx, rx)
    }

    /// Constructs a new [`NodeContext`] with a specific [`channel`][mpsc::channel].
    pub fn with_channel(
        label: String,
        tx: mpsc::Sender<Envelope<Node<L>>>,
        rx: mpsc::Receiver<Envelope<Node<L>>>,
    ) -> Self {
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
        }
    }

    async fn handle_envelope(&mut self, actor: &mut Node<L>, envelope: Option<Envelope<Node<L>>>) {
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

    async fn processing_loop(
        &mut self,
        actor: &mut Node<L>,
        mailbox: &mut Mailbox<Node<L>>,
    ) -> Result<(), <Node<L> as Actor>::Error> {
        while self.state() == ActorState::Running {
            if let Some(listener) = actor.listener.as_ref() {
                tokio::select! {
                    envelope = mailbox.recv() => {
                        self.handle_envelope(actor, envelope).await;
                    }
                    connection = listener.accept() => {
                        match connection {
                            Ok(connection) => {
                                if let Err(e) = actor
                                    .create_session(connection, None, self)
                                    .await
                                {
                                    warn!("Could not create new session: {}", report!(e));
                                }
                            }
                            Err(e) => {
                                warn!("Could not accept connection: {}", report!(e));
                            }
                        }
                    }
                }
            } else {
                self.handle_envelope(actor, mailbox.recv().await).await;
            }
        }

        Ok(())
    }
}

impl<L> ActorContext<Node<L>> for NodeContext<L>
where
    L: IpcListener,
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

    fn address(&self) -> Address<Node<L>> {
        self.doorplate.clone()
    }

    fn take_mailbox(&mut self) -> Option<Mailbox<Node<L>>> {
        self.mailbox.take()
    }

    fn state(&self) -> ActorState {
        self.state
    }

    fn set_state(&mut self, state: ActorState) {
        self.state = state;
    }

    fn supervisor(&self) -> Option<&Recipient<SupervisionEvent<Node<L>>>> {
        None
    }

    fn set_supervisor(&mut self, _supervisor: Option<Recipient<SupervisionEvent<Node<L>>>>) {}

    async fn processing(
        &mut self,
        actor: &mut Node<L>,
        mut mailbox: Mailbox<Node<L>>,
    ) -> Result<(), <Node<L> as Actor>::Error> {
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

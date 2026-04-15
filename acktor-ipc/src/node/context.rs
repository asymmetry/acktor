use std::io::Error as IoError;

use futures_util::future::select_all;
use tracing::{debug, warn};

use acktor::{
    Actor, ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, ErrorReport,
    address::Mailbox,
    channel::mpsc,
    envelope::{Envelope, EnvelopeProxy},
};

use super::Node;
use crate::ipc_method::IpcConnection;

enum LoopEvent {
    Envelope(Option<Envelope<Node>>),
    Accept(Result<Box<dyn IpcConnection>, IoError>, String),
}

pub struct NodeContext {
    label: String,
    state: ActorState,
    doorplate: Address<Node>,
    mailbox: Option<Mailbox<Node>>,
}

impl NodeContext {
    /// Constructs a new [`NodeContext`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self::with_channel(label, tx, rx)
    }

    /// Constructs a new [`NodeContext`] with a specific [`channel`][mpsc::channel].
    pub fn with_channel(
        label: String,
        tx: mpsc::Sender<Envelope<Node>>,
        rx: mpsc::Receiver<Envelope<Node>>,
    ) -> Self {
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
        }
    }

    async fn handle_envelope(&mut self, actor: &mut Node, envelope: Option<Envelope<Node>>) {
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
        actor: &mut Node,
        mailbox: &mut Mailbox<Node>,
    ) -> Result<(), <Node as Actor>::Error> {
        while self.state() == ActorState::Running {
            // Compute the next event in a scoped block so the `select_all` future (which
            // borrows `actor.listeners()`) is dropped before we reborrow `actor` mutably.
            let event = {
                let listeners = actor.listeners();

                if listeners.is_empty() {
                    LoopEvent::Envelope(mailbox.recv().await)
                } else {
                    let accepts = listeners.iter().map(|l| l.accept());

                    tokio::select! {
                        envelope = mailbox.recv() => LoopEvent::Envelope(envelope),
                        (result, index, _) = select_all(accepts) => {
                            let endpoint = listeners[index].local_endpoint();
                            LoopEvent::Accept(result, endpoint.to_string())
                        }
                    }
                }
            };

            match event {
                LoopEvent::Envelope(envelope) => {
                    self.handle_envelope(actor, envelope).await;
                }
                LoopEvent::Accept(Ok(connection), _) => {
                    if let Err(e) = actor.create_session(connection, None, self).await {
                        warn!("Could not create new session: {}", e.report());
                    }
                }
                LoopEvent::Accept(Err(e), endpoint) => {
                    warn!(
                        "Could not accept connection on {}: {}",
                        endpoint,
                        e.report(),
                    );
                }
            }
        }

        Ok(())
    }
}

impl ActorContext<Node> for NodeContext {
    fn new(label: String) -> Self {
        Self::with_capacity(label, DEFAULT_MAILBOX_CAPACITY)
    }

    fn index(&self) -> u64 {
        self.doorplate.index()
    }

    fn label(&self) -> &str {
        self.label.as_str()
    }

    fn address(&self) -> Address<Node> {
        self.doorplate.clone()
    }

    fn take_mailbox(&mut self) -> Option<Mailbox<Node>> {
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
        actor: &mut Node,
        mut mailbox: Mailbox<Node>,
    ) -> Result<(), <Node as Actor>::Error> {
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

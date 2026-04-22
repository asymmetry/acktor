use std::io::Error as IoError;

use futures_util::future::select_all;
use tracing::warn;

use acktor::{
    ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, ErrorReport, RecvError,
    address::Mailbox,
    channel::mpsc,
    envelope::{Envelope, EnvelopeProxy},
};

use super::Node;
use crate::errors::NodeError;
use crate::ipc_method::IpcConnection;

enum LoopEvent {
    Envelope(Result<Envelope<Node>, RecvError>),
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
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
        }
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

    async fn process_loop(
        &mut self,
        actor: &mut Node,
        mailbox: &mut Mailbox<Node>,
    ) -> Result<(), NodeError> {
        while self.state() == ActorState::Running {
            // compute the next event in a scoped block so the `select_all` future (which borrows
            // `actor.listeners()`) is dropped before we reborrow `actor` mutably
            let event = {
                if actor.listeners.is_empty() {
                    LoopEvent::Envelope(mailbox.recv().await)
                } else {
                    let accepts = actor.listeners.iter().map(|l| l.accept());

                    tokio::select! {
                        envelope = mailbox.recv() => LoopEvent::Envelope(envelope),
                        (result, index, _) = select_all(accepts) => {
                            let endpoint = actor.listeners[index].local_endpoint();
                            LoopEvent::Accept(result, endpoint.to_string())
                        }
                    }
                }
            };

            match event {
                LoopEvent::Envelope(envelope) => match envelope {
                    Ok(mut envelope) => {
                        envelope.handle(actor, self).await;
                    }
                    _ => {
                        warn!("Mailbox is dropped, terminate the actor");
                        self.set_state(ActorState::Stopped);
                    }
                },
                LoopEvent::Accept(Ok(connection), _) => {
                    if let Err(e) = actor.create_session(connection, None, self).await {
                        warn!("Could not create new session: {}", e.report());
                    }
                }
                LoopEvent::Accept(Err(e), endpoint) => {
                    warn!(
                        "Could not accept connection from {}: {}",
                        endpoint,
                        e.report(),
                    );
                }
            }
        }

        Ok(())
    }
}

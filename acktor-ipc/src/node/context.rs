use std::io::Error as IoError;

use futures_util::future::select_all;
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, warn};

use acktor::{
    ActorContext, ActorState, Address, DEFAULT_MAILBOX_CAPACITY, ErrorReport, RecvError,
    address::Mailbox,
    channel::mpsc,
    envelope::{Envelope, EnvelopeProxy},
};

use super::Node;
use crate::errors::NodeError;
use crate::ipc_method::{IpcConnection, IpcListener};

enum LoopEvent {
    Envelope(Result<Envelope<Node>, RecvError>),
    Accept(Result<Box<dyn IpcConnection>, IoError>, String),
    ListenerPanicked(JoinError, String),
}

type AcceptTaskJoinHandle = JoinHandle<(
    Result<Box<dyn IpcConnection>, IoError>,
    Box<dyn IpcListener>,
)>;

struct AcceptTask {
    pub label: String,
    pub join_handle: AcceptTaskJoinHandle,
}

pub struct NodeContext {
    label: String,
    state: ActorState,
    doorplate: Address<Node>,
    mailbox: Option<Mailbox<Node>>,
    accept_tasks: Vec<AcceptTask>,
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
            accept_tasks: Vec::new(),
        }
    }

    pub(crate) fn abort_accept_task(&mut self, label: &str) {
        self.accept_tasks.retain(|task| {
            if task.label == label {
                task.join_handle.abort();
                true
            } else {
                false
            }
        });
    }

    async fn recv_or_accept(&mut self, actor: &mut Node, mailbox: &mut Mailbox<Node>) -> LoopEvent {
        // remove non-existed listeners
        self.accept_tasks.retain(|task| {
            if actor.listener_labels.contains(&task.label) {
                true
            } else {
                task.join_handle.abort();
                false
            }
        });
        // spawn new accept tasks if there are new listeners
        let listeners = std::mem::take(&mut actor.listeners);
        for listener in listeners.into_iter() {
            let label = listener.local_endpoint().to_string();
            let join_handle = tokio::task::spawn(async move {
                let result = listener.accept().await;
                (result, listener)
            });
            self.accept_tasks.push(AcceptTask { label, join_handle });
        }

        if self.accept_tasks.is_empty() {
            LoopEvent::Envelope(mailbox.recv().await)
        } else {
            let accept_futs = self
                .accept_tasks
                .iter_mut()
                .map(|task| &mut task.join_handle);

            // task is join_handle, which is cancel safe
            tokio::select! {
                envelope = mailbox.recv() => {
                    LoopEvent::Envelope(envelope)
                }
                (result, index, _) = select_all(accept_futs) => match result {
                    Ok((result, listener)) => {
                        let task = self.accept_tasks.remove(index);
                        let AcceptTask { label, join_handle: _ } = task;

                        // re-spawn new task to replace the completed one
                        let join_handle = tokio::task::spawn(async move {
                            let result = listener.accept().await;
                            (result, listener)
                        });
                        self.accept_tasks.push(AcceptTask { label: label.clone(), join_handle });

                        LoopEvent::Accept(result, label)
                    }
                    Err(e) => {
                        let label = self.accept_tasks.remove(index).label;
                        actor.listener_labels.remove(&label);

                        LoopEvent::ListenerPanicked(e, label)
                    }
                }
            }
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
            let event = self.recv_or_accept(actor, mailbox).await;
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
                LoopEvent::Accept(Ok(connection), label) => {
                    debug!("Accepted a new connection from {}", label);
                    if let Err(e) = actor.create_session(connection, None, self).await {
                        warn!("Could not create new session: {}", e.report());
                    }
                }
                LoopEvent::Accept(Err(e), label) => {
                    // IO error is normal when accepting connections, so we just log it and
                    // continue accepting new connections.
                    warn!("Could not accept connection from {}: {}", label, e.report(),);
                }
                LoopEvent::ListenerPanicked(e, label) => {
                    warn!("Listener {} panicked, terminate the actor: {}", label, e);
                    self.set_state(ActorState::Stopped);
                }
            }
        }

        for task in self.accept_tasks.drain(..) {
            task.join_handle.abort();
        }

        Ok(())
    }
}

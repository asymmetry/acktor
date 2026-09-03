//! Repetitive task execution in an actor.
//!
//! This module provides the traits and a default implementation for an actor which can execute
//! repetitive tasks and its corresponding actor context.
//!

use std::time::Duration;

use tokio::time;
use tracing::{Instrument, debug, warn};

use crate::actor::{Actor, ActorContext, ActorId, ActorState, JoinHandle, Stopping};
use crate::address::{Address, Mailbox, Recipient, SenderInfo};
use crate::channel::mpsc;
use crate::context::DEFAULT_MAILBOX_CAPACITY;
use crate::drain::{DrainPolicy, Drainable, NoDrain};
use crate::envelope::EnvelopeProxy;
use crate::error::RecvError;
use crate::message::{Handler, Message};
use crate::supervisor::SupervisionEvent;
use crate::utils::debug_trace;

/// State of the repetitive task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CronState {
    /// The task is running normally.
    Normal,
    /// The task is paused.
    Paused,
}

/// Describes the behavior of an actor which can execute repetitive tasks.
pub trait CronActor: Actor {
    /// Invoked to execute periodic tasks when an actor is running.
    ///
    /// A [`Duration`] is returned to specify the interval of the next execution.
    ///
    /// Notice the actor will not response to any messages during the execution of this function.
    ///
    /// Returning [`Duration::ZERO`] means "run as frequently as possible", the actor does a
    /// non-blocking `try_recv` on the mailbox and re-invokes `task` immediately. If this
    /// function does little real work and the mailbox stays empty, this will keep a core busy;
    /// return a small positive duration in that case.
    #[allow(unused_variables)]
    fn task(
        &mut self,
        ctx: &mut Self::Context,
    ) -> impl Future<Output = Result<Duration, Self::Error>> + Send;
}

/// Describes the execution context of an actor which can execute repetitive tasks.
pub trait CronActorContext<A>: ActorContext<A>
where
    A: Actor<Context = Self> + CronActor,
{
    /// Pauses the repetitive task execution.
    fn pause_task(&mut self);

    /// Resumes the repetitive task execution.
    fn resume_task(&mut self);
}

/// The default implementation of an actor context which can execute repetitive tasks.
///
/// The `D` type parameter selects the mailbox draining behavior. It defaults to [`NoDrain`],
/// which compiles the draining machinery out entirely; use
/// [`CronContext<Self, Drainable>`][crate::drain::Drainable] to gain
/// [`drain_mailbox`][CronContext::drain_mailbox].
#[derive(Debug)]
pub struct CronContext<A, D = NoDrain>
where
    A: Actor<Context = Self> + CronActor,
    D: DrainPolicy,
{
    label: String,
    state: ActorState,
    doorplate: Address<A>,
    mailbox: Option<Mailbox<A>>,
    drain: D::State,
    cron_state: CronState,
    cron_join_handle: Option<JoinHandle<()>>,
    supervisor: Option<Recipient<SupervisionEvent<A>>>,
    error: Option<A::Error>, // if an error happened during message handling
}

impl<A, D> CronContext<A, D>
where
    A: Actor<Context = Self> + CronActor,
    D: DrainPolicy,
{
    /// Constructs a new [`CronContext`] with a specific capacity.
    pub fn with_capacity(label: String, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            label,
            state: ActorState::Unstarted,
            doorplate: Address::new(tx),
            mailbox: Some(Mailbox::new(rx)),
            drain: D::State::default(),
            cron_state: CronState::Normal,
            cron_join_handle: None,
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

    fn take_error(&mut self) -> Result<(), A::Error> {
        match self.error.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn process_one(
        &mut self,
        actor: &mut A,
        mailbox: &mut Mailbox<A>,
    ) -> Result<(), A::Error> {
        let async_wait = match self.cron_state {
            CronState::Normal => {
                let duration = actor.task(self).await?;
                if duration == Duration::ZERO {
                    // duration is zero, actor should run the task as frequently as possible
                    // actor should check the mailbox without waiting
                    false
                } else {
                    // duration is not zero, spawn a timer to send a resume message later
                    // actor should asynchronously wait for this message (or other messages)
                    self.cron_state = CronState::Paused;
                    let address = self.address();
                    let join_handle = tokio::spawn(
                        async move {
                            time::sleep(duration).await;
                            if let Err(e) = address.do_send(CronSignal::Resume).await {
                                debug!("Could not send Resume signal: {}", e);
                            }
                        }
                        .in_current_span(),
                    );
                    self.cron_join_handle = Some(join_handle);

                    true
                }
            }
            CronState::Paused => {
                // actor should continue to wait for a message
                true
            }
        };

        if async_wait {
            match mailbox.recv().await {
                Ok(mut envelope) => {
                    D::snapshot(&mut self.drain, mailbox);
                    envelope.handle(actor, self).await;
                    D::apply(&mut self.drain, mailbox);
                }
                Err(_) => {
                    warn!("Mailbox is dropped, terminate the actor");
                    self.set_state(ActorState::Stopped);
                }
            };
        } else {
            match mailbox.try_recv() {
                Ok(mut envelope) => {
                    D::snapshot(&mut self.drain, mailbox);
                    envelope.handle(actor, self).await;
                    D::apply(&mut self.drain, mailbox);
                }
                Err(RecvError::Closed) => {
                    warn!("Mailbox is dropped, terminate the actor");
                    self.set_state(ActorState::Stopped);
                }
                _ => tokio::task::consume_budget().await,
            };
        }

        self.take_error()
    }
}

impl<A, D> ActorContext<A> for CronContext<A, D>
where
    A: Actor<Context = Self> + CronActor,
    D: DrainPolicy,
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

    async fn run_loop(&mut self, actor: &mut A, mailbox: &mut Mailbox<A>) -> Result<(), A::Error> {
        while self.state() == ActorState::Running {
            let result = self.process_one(actor, mailbox).await;

            if result.is_err() && self.state() == ActorState::Running {
                self.set_state(ActorState::Stopping);
            }

            match self.state() {
                ActorState::Stopping => {
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
                    return result;
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

impl<A, D> CronActorContext<A> for CronContext<A, D>
where
    A: Actor<Context = Self> + CronActor,
    D: DrainPolicy,
{
    fn pause_task(&mut self) {
        if let Some(join_handle) = self.cron_join_handle.take() {
            join_handle.abort();
        }
        self.cron_state = CronState::Paused;
    }

    fn resume_task(&mut self) {
        if let Some(join_handle) = self.cron_join_handle.take() {
            join_handle.abort();
        }
        self.cron_state = CronState::Normal;
    }
}

impl<A> CronContext<A, Drainable>
where
    A: Actor<Context = Self> + CronActor,
{
    /// Schedules a one-time discard of messages already queued in the mailbox.
    ///
    /// It should be called from a message handler. The length of the mailbox is snapshotted
    /// right before the message handler is invoked, which means messages received during the
    /// current message handler will not be discarded.
    ///
    /// This method is only available on a context using the [`Drainable`] policy.
    pub fn drain_mailbox(&mut self) {
        self.drain.request();
    }
}

/// A message which is used to pause/resume the repetitive task execution.
///
/// `Handler<CronSignal>` is implemented for all actors which implement `CronActor` automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CronSignal {
    /// Pause the repetitive task execution.
    Pause,
    /// Resume the repetitive task execution.
    Resume,
}

impl TryFrom<u8> for CronSignal {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CronSignal::Pause),
            1 => Ok(CronSignal::Resume),
            _ => Err(()),
        }
    }
}

impl Message for CronSignal {
    type Result = ();
}

impl<A> Handler<CronSignal> for A
where
    A: CronActor,
    A::Context: CronActorContext<A>,
{
    type Result = ();

    async fn handle(&mut self, msg: CronSignal, ctx: &mut Self::Context) -> Self::Result {
        debug_trace!("Handle command {:?}", msg);

        match msg {
            CronSignal::Pause => {
                ctx.pause_task();
            }
            CronSignal::Resume => {
                ctx.resume_task();
            }
        }
    }
}

#[cfg(feature = "identifier")]
impl crate::stable_type_id::StableId for CronSignal {
    const TYPE_ID: crate::stable_type_id::StableTypeId =
        crate::stable_type_id::StableTypeId::from_stable_type_name(concat!(
            module_path!(),
            "::",
            "CronSignal"
        ));
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_cron_signal() {
        assert_eq!(CronSignal::try_from(0), Ok(CronSignal::Pause));
        assert_eq!(CronSignal::try_from(1), Ok(CronSignal::Resume));
        assert_eq!(CronSignal::try_from(2), Err(()));
    }
}

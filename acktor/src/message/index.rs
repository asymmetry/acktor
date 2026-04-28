use super::Message;
use crate::actor::Actor;
use crate::signal::Signal;
use crate::stable_type_id::HasStableTypeId;
use crate::supervisor::{SupervisionEvent, Supervisor};

#[cfg(feature = "cron")]
use crate::cron::CronSignal;
#[cfg(feature = "observer")]
use crate::observer::Observer;

/// Message index.
///
/// It is used to identify the type of a message in IPC communication.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(MessageId)]`][acktor_derive::MessageId], unless you need to overwrite the default
/// `ID` value.
pub trait MessageId: Message + HasStableTypeId {
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
}

impl MessageId for Signal {}

impl<A> MessageId for Supervisor<A> where A: Actor + HasStableTypeId {}

impl<A> MessageId for SupervisionEvent<A> where A: Actor + HasStableTypeId {}

#[cfg(feature = "observer")]
impl<M> MessageId for Observer<M> where M: Message + MessageId {}

#[cfg(feature = "cron")]
impl MessageId for CronSignal {}

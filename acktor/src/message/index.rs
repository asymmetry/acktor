use super::Message;
use crate::StableTypeId;
use crate::actor::Actor;
use crate::signal::Signal;
use crate::stable_type_id::HasStableTypeId;
use crate::supervisor::{SupervisionEvent, Supervisor};

#[cfg(feature = "cron")]
use crate::cron::CronSignal;
#[cfg(feature = "observer")]
use crate::observer::Observer;

/// Message identifier.
///
/// It is used to identify the type of a message in IPC communication.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(MessageId)]`][acktor_derive::MessageId]. By default, the derive macro will emit an
/// implementation of `HasStableTypeId` for the type, and then set the `ID` with
/// `STABLE_TYPE_ID.as_u64()`. If users would like to use a different value for `ID`, they can use
/// the attribute `#[custom_id(<value>)]` to specify one. In this case, it is the users'
/// responsibility to ensure the message identifier is unique across all messages that can be
/// handled by the same actor.
pub trait MessageId: Message {
    const ID: u64;
}

impl MessageId for Signal {
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
}

impl<A> MessageId for Supervisor<A>
where
    A: Actor + HasStableTypeId,
{
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
}

impl<A> MessageId for SupervisionEvent<A>
where
    A: Actor + HasStableTypeId,
{
    const ID: u64 =
        StableTypeId::from_stable_type_name("acktor::supervisor::SupervisionEvent").as_u64();
}

#[cfg(feature = "observer")]
impl<M> MessageId for Observer<M>
where
    M: Message + HasStableTypeId,
{
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
}

#[cfg(feature = "cron")]
impl MessageId for CronSignal {
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
}

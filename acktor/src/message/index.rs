use super::Message;
use crate::actor::Actor;
#[cfg(feature = "cron")]
use crate::cron::CronSignal;
#[cfg(feature = "observer")]
use crate::observer::Observer;
use crate::signal::Signal;
use crate::stable_type_id::HasStableTypeId;
use crate::supervisor::{SupervisionEvent, Supervisor};

/// Message identifier.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(MessageId)]`][acktor_derive::MessageId].
///
/// By default, the derive macro will emit an implementation of [`HasStableTypeId`] for the type,
/// and set the `ID` with [`HasStableTypeId::STABLE_TYPE_ID`] by calling
/// [`as_u64`][crate::stable_type_id::StableTypeId::as_u64] on it.
///
/// If you need to use a different id value, use the attribute `#[custom_id(<value>)]` to specify
/// one. In this case, it is your responsibility to ensure the message identifier is unique across
/// all messages that can be handled by the same actor.
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
    const ID: u64 = Self::STABLE_TYPE_ID.as_u64();
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

use std::fmt::Display;

use bytes::{Bytes, BytesMut};
use prost::Message as _;

use acktor_ipc_proto::control_message as proto;

use super::error::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};
use crate::actor::{Actor, ActorState, RemoteAddressable};
use crate::address::{Address, Recipient, SenderInfo};
#[cfg(feature = "cron")]
use crate::cron::CronSignal;
use crate::message::{Message, MessageId};
#[cfg(feature = "observer")]
use crate::observer::Observer;
use crate::signal::Signal;
use crate::stable_type_id::StableId;
use crate::supervisor::{SupervisionEvent, Supervisor};

impl Encode for Signal {
    #[inline]
    fn encoded_len(&self) -> usize {
        proto::Signal::new(*self as i32).encoded_len()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        proto::Signal::new(*self as i32)
            .encode(buf)
            .map_err(Into::into)
    }
}

impl Decode for Signal {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let signal = proto::Signal::decode(buf)?;
        Signal::try_from(signal.signal as u8)
            .map_err(|_| "invalid signal value in the `Signal` message".into())
    }
}

impl<A> Encode for Supervisor<A>
where
    A: Actor + RemoteAddressable + StableId,
{
    fn encoded_len(&self) -> usize {
        let supervisor = match self {
            Supervisor::Set(recipient) => proto::Supervisor::set(recipient.index().as_local()),
            Supervisor::Unset => proto::Supervisor::unset(),
        };
        supervisor.encoded_len()
    }

    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        let supervisor = match self {
            Supervisor::Set(recipient) => {
                recipient.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
                proto::Supervisor::set(recipient.index().as_local())
            }

            Supervisor::Unset => proto::Supervisor::unset(),
        };
        supervisor.encode(buf).map_err(Into::into)
    }
}

impl<A> Decode for Supervisor<A>
where
    A: Actor + RemoteAddressable + StableId,
{
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let supervisor = proto::Supervisor::decode(buf)?;
        match supervisor.supervisor {
            Some(proto::SupervisorType::Set(actor_id)) => Ok(Supervisor::Set(
                Recipient::new_with_decode_context(actor_id, ctx)?,
            )),
            Some(proto::SupervisorType::Unset(())) => Ok(Supervisor::Unset),
            None => Err("missing field `supervisor` in the `Supervisor` message".into()),
        }
    }
}

#[cfg(feature = "observer")]
impl<M> Encode for Observer<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn encoded_len(&self) -> usize {
        let observer = match self {
            Observer::Register(recipient) => {
                proto::Observer::register(recipient.index().as_local())
            }
            Observer::Unregister(recipient) => {
                proto::Observer::unregister(recipient.index().as_local())
            }
        };
        observer.encoded_len()
    }

    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        let observer = match self {
            Observer::Register(recipient) => {
                recipient.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
                proto::Observer::register(recipient.index().as_local())
            }

            Observer::Unregister(recipient) => {
                recipient.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
                proto::Observer::unregister(recipient.index().as_local())
            }
        };
        observer.encode(buf).map_err(Into::into)
    }
}

#[cfg(feature = "observer")]
impl<M> Decode for Observer<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let observer = proto::Observer::decode(buf)?;
        match observer.observer {
            Some(proto::ObserverType::Register(actor_id)) => Ok(Observer::Register(
                Recipient::new_with_decode_context(actor_id, ctx)?,
            )),
            Some(proto::ObserverType::Unregister(actor_id)) => Ok(Observer::Unregister(
                Recipient::new_with_decode_context(actor_id, ctx)?,
            )),
            None => Err("missing field `observer` in the `Observer` message".into()),
        }
    }
}

fn build_supervision_event_message<A>(
    event: &SupervisionEvent<A>,
) -> (proto::SupervisionEvent, &Address<A>)
where
    A: Actor + RemoteAddressable + StableId,
    A::Error: Display,
{
    match event {
        SupervisionEvent::Warn(address, error) => (
            proto::SupervisionEvent::warn(address.index().as_local(), error.to_string()),
            address,
        ),
        SupervisionEvent::Terminated(address, error) => (
            proto::SupervisionEvent::terminated(
                address.index().as_local(),
                error.as_ref().map(|e| e.to_string()),
            ),
            address,
        ),
        SupervisionEvent::Panicked(address, info) => (
            proto::SupervisionEvent::panicked(address.index().as_local(), info.to_string()),
            address,
        ),
        SupervisionEvent::State(address, state) => (
            proto::SupervisionEvent::state(address.index().as_local(), *state as i32),
            address,
        ),
    }
}

impl<A> Encode for SupervisionEvent<A>
where
    A: Actor + RemoteAddressable + StableId,
    A::Error: Display,
{
    fn encoded_len(&self) -> usize {
        let (event, _) = build_supervision_event_message(self);
        event.encoded_len()
    }

    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        let (event, address) = build_supervision_event_message(self);
        address.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
        event.encode(buf).map_err(Into::into)
    }

    fn encode_to_bytes(&self, ctx: Option<&dyn EncodeContext>) -> Result<Bytes, EncodeError> {
        let (event, address) = build_supervision_event_message(self);
        address.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
        let mut buf = BytesMut::with_capacity(event.encoded_len());
        event.encode(&mut buf)?;
        Ok(buf.freeze())
    }
}

impl<A> Decode for SupervisionEvent<A>
where
    A: Actor + RemoteAddressable + StableId,
    A::Error: From<String>,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let event = proto::SupervisionEvent::decode(buf)?;
        match event.event {
            Some(proto::SupervisionEventType::Warn(warn)) => Ok(SupervisionEvent::<A>::Warn(
                Address::new_with_decode_context(warn.actor_id, ctx)?,
                warn.err.into(),
            )),

            Some(proto::SupervisionEventType::Terminated(terminated)) => {
                Ok(SupervisionEvent::<A>::Terminated(
                    Address::new_with_decode_context(terminated.actor_id, ctx)?,
                    terminated.err.map(|e| e.into()),
                ))
            }

            Some(proto::SupervisionEventType::Panicked(panicked)) => {
                Ok(SupervisionEvent::<A>::Panicked(
                    Address::new_with_decode_context(panicked.actor_id, ctx)?,
                    panicked.info,
                ))
            }

            Some(proto::SupervisionEventType::State(state)) => Ok(SupervisionEvent::<A>::State(
                Address::new_with_decode_context(state.actor_id, ctx)?,
                ActorState::try_from(state.state as u8).map_err(|_| {
                    DecodeError::from("invalid actor state value in the `SupervisionEvent` message")
                })?,
            )),

            None => Err("missing field `event` in the `SupervisionEvent` message".into()),
        }
    }
}

#[cfg(feature = "cron")]
impl Encode for CronSignal {
    #[inline]
    fn encoded_len(&self) -> usize {
        proto::CronSignal::new(*self as i32).encoded_len()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        proto::CronSignal::new(*self as i32)
            .encode(buf)
            .map_err(Into::into)
    }
}

#[cfg(feature = "cron")]
impl Decode for CronSignal {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let cron_signal = proto::CronSignal::decode(buf)?;
        CronSignal::try_from(cron_signal.signal as u8)
            .map_err(|_| "invalid signal value in the `CronSignal` message".into())
    }
}

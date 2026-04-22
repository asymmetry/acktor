use std::fmt::Display;

use bytes::{Bytes, BytesMut};
use prost::Message as _;

use acktor::{
    Actor, ActorState, Address, Message, SenderId, Signal,
    cron::CronSignal,
    observer::Observer,
    supervisor::{SupervisionEvent, Supervisor},
};
use acktor_ipc_proto::control_message as proto;

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};
use crate::remote_message::RemoteSupervisionEvent;

impl Encode for Signal {
    const ID: u64 = u64::MAX;

    #[inline]
    fn encoded_len(&self) -> usize {
        proto::Signal::new(*self as i32).encoded_len()
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, _ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        proto::Signal::new(*self as i32)
            .encode(buf)
            .map_err(Into::into)
    }
}

impl Decode for Signal {
    const ID: u64 = u64::MAX;

    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let signal = proto::Signal::decode(buf)?;
        Signal::try_from(signal.signal as u8)
            .map_err(|_| "invalid signal value in the `Signal` message".into())
    }
}

impl<A> Encode for Supervisor<A>
where
    A: Actor,
{
    const ID: u64 = u64::MAX - 1;

    fn encoded_len(&self) -> usize {
        let supervisor = match self {
            Supervisor::Set(recipient) => proto::Supervisor::set(recipient.index()),
            Supervisor::Unset => proto::Supervisor::unset(),
        };
        supervisor.encoded_len()
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let supervisor = match self {
            Supervisor::Set(recipient) => {
                ctx.ok_or(EncodeError::MissingEncodeContext)?
                    .register(recipient)?;
                proto::Supervisor::set(recipient.index())
            }

            Supervisor::Unset => proto::Supervisor::unset(),
        };
        supervisor.encode(buf).map_err(Into::into)
    }
}

impl<A> Decode for Supervisor<A>
where
    A: Actor,
{
    const ID: u64 = u64::MAX - 1;

    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let supervisor = proto::Supervisor::decode(buf)?;
        match supervisor.supervisor {
            Some(proto::SupervisorType::Set(actor_id)) => {
                Ok(Supervisor::Set(ctx.create_remote_address(actor_id)?.into()))
            }
            Some(proto::SupervisorType::Unset(())) => Ok(Supervisor::Unset),
            None => Err("missing field `supervisor` in the `Supervisor` message".into()),
        }
    }
}

impl<M> Encode for Observer<M>
where
    M: Message + Encode,
    M::Result: Decode,
{
    const ID: u64 = u64::MAX - 2;

    fn encoded_len(&self) -> usize {
        let observer = match self {
            Observer::Register(recipient) => proto::Observer::register(recipient.index()),
            Observer::Unregister(recipient) => proto::Observer::unregister(recipient.index()),
        };
        observer.encoded_len()
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let observer = match self {
            Observer::Register(recipient) => {
                ctx.ok_or(EncodeError::MissingEncodeContext)?
                    .register(recipient)?;
                proto::Observer::register(recipient.index())
            }

            Observer::Unregister(recipient) => {
                ctx.ok_or(EncodeError::MissingEncodeContext)?
                    .register(recipient)?;
                proto::Observer::unregister(recipient.index())
            }
        };
        observer.encode(buf).map_err(Into::into)
    }
}

impl<M> Decode for Observer<M>
where
    M: Message + Encode,
    M::Result: Decode,
{
    const ID: u64 = u64::MAX - 2;

    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let observer = proto::Observer::decode(buf)?;
        match observer.observer {
            Some(proto::ObserverType::Register(actor_id)) => Ok(Observer::Register(
                ctx.create_remote_address(actor_id)?.into(),
            )),
            Some(proto::ObserverType::Unregister(actor_id)) => Ok(Observer::Unregister(
                ctx.create_remote_address(actor_id)?.into(),
            )),
            None => Err("missing field `observer` in the `Observer` message".into()),
        }
    }
}

fn build_supervision_event_message<A>(
    event: &SupervisionEvent<A>,
) -> (proto::SupervisionEvent, &Address<A>)
where
    A: Actor,
    A::Error: Display,
{
    match event {
        SupervisionEvent::Warn(address, error) => (
            proto::SupervisionEvent::warn(address.index(), error.to_string()),
            address,
        ),
        SupervisionEvent::Terminated(address, error) => (
            proto::SupervisionEvent::terminated(
                address.index(),
                error.as_ref().map(|e| e.to_string()),
            ),
            address,
        ),
        SupervisionEvent::Panicked(address, info) => (
            proto::SupervisionEvent::panicked(address.index(), info.to_string()),
            address,
        ),
        SupervisionEvent::State(address, state) => (
            proto::SupervisionEvent::state(address.index(), *state as i32),
            address,
        ),
    }
}

impl<A> Encode for SupervisionEvent<A>
where
    A: Actor,
    A::Error: Display,
{
    const ID: u64 = u64::MAX - 3;

    fn encoded_len(&self) -> usize {
        let (event, _) = build_supervision_event_message(self);
        event.encoded_len()
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let (event, address) = build_supervision_event_message(self);
        ctx.ok_or(EncodeError::MissingEncodeContext)?
            .register(address)?;
        event.encode(buf).map_err(Into::into)
    }

    fn encode_to_bytes(&self, ctx: Option<&EncodeContext>) -> Result<Bytes, EncodeError> {
        let (event, address) = build_supervision_event_message(self);
        ctx.ok_or(EncodeError::MissingEncodeContext)?
            .register(address)?;
        let mut buf = BytesMut::with_capacity(event.encoded_len());
        event.encode(&mut buf)?;
        Ok(buf.freeze())
    }
}

impl Decode for RemoteSupervisionEvent {
    const ID: u64 = u64::MAX - 3;

    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let event = proto::SupervisionEvent::decode(buf)?;
        match event.event {
            Some(proto::SupervisionEventType::Warn(warn)) => Ok(RemoteSupervisionEvent::Warn(
                ctx.create_remote_address(warn.actor_id)?,
                warn.err,
            )),

            Some(proto::SupervisionEventType::Terminated(terminated)) => {
                Ok(RemoteSupervisionEvent::Terminated(
                    ctx.create_remote_address(terminated.actor_id)?,
                    terminated.err,
                ))
            }

            Some(proto::SupervisionEventType::Panicked(panicked)) => {
                Ok(RemoteSupervisionEvent::Panicked(
                    ctx.create_remote_address(panicked.actor_id)?,
                    panicked.info,
                ))
            }

            Some(proto::SupervisionEventType::State(state)) => Ok(RemoteSupervisionEvent::State(
                ctx.create_remote_address(state.actor_id)?,
                ActorState::try_from(state.state as u8).map_err(|_| {
                    DecodeError::from("invalid actor state value in the `SupervisionEvent` message")
                })?,
            )),

            None => Err("missing field `event` in the `SupervisionEvent` message".into()),
        }
    }
}

impl Encode for CronSignal {
    const ID: u64 = u64::MAX - 4;

    #[inline]
    fn encoded_len(&self) -> usize {
        proto::CronSignal::new(*self as i32).encoded_len()
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, _ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        proto::CronSignal::new(*self as i32)
            .encode(buf)
            .map_err(Into::into)
    }
}

impl Decode for CronSignal {
    const ID: u64 = u64::MAX - 4;

    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let cron_signal = proto::CronSignal::decode(buf)?;
        CronSignal::try_from(cron_signal.signal as u8)
            .map_err(|_| "invalid signal value in the `CronSignal` message".into())
    }
}

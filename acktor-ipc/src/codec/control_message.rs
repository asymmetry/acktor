use std::fmt::Display;

use bytes::{Bytes, BytesMut};

use acktor::{
    Actor, ActorState, Address, Message, SenderId, Signal,
    cron::CronSignal,
    observer::Observer,
    supervisor::{SupervisionEvent, Supervisor},
};
use acktor_ipc_proto::control_message::{self as proto, ControlMessage};

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};
use crate::remote_message::RemoteSupervisionEvent;

impl Encode for Signal {
    #[inline]
    fn encoded_len(&self) -> usize {
        let signal = proto::Signal::new(*self as i32);
        let message = ControlMessage::signal(signal);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, _ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let signal = proto::Signal::new(*self as i32);
        let message = ControlMessage::signal(signal);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for Signal {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::Signal(signal)) => {
                Signal::try_from(signal.signal as u8)
                    .map_err(|_| "invalid signal value in the `Signal` message".into())
            }
            _ => Err("message is not a `Signal` message".into()),
        }
    }
}

impl<A> Encode for Supervisor<A>
where
    A: Actor,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        let supervisor = match self {
            Supervisor::Set(recipient) => proto::Supervisor::set(recipient.index()),
            Supervisor::Unset => proto::Supervisor::unset(),
        };
        let message = ControlMessage::supervisor(supervisor);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let supervisor = match self {
            Supervisor::Set(recipient) => {
                ctx.ok_or(EncodeError::MissingEncodeContext)?
                    .register(recipient)?;
                proto::Supervisor::set(recipient.index())
            }

            Supervisor::Unset => proto::Supervisor::unset(),
        };
        let message = ControlMessage::supervisor(supervisor);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl<A> Decode for Supervisor<A>
where
    A: Actor,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::Supervisor(supervisor)) => {
                match supervisor.supervisor {
                    Some(proto::SupervisorType::Set(actor_id)) => {
                        Ok(Supervisor::Set(ctx.create_remote_address(actor_id)?.into()))
                    }
                    Some(proto::SupervisorType::Unset(())) => Ok(Supervisor::Unset),
                    None => Err("missing field `supervisor` in the `Supervisor` message".into()),
                }
            }

            _ => Err("message is not a `Supervisor` message".into()),
        }
    }
}

impl<M> Encode for Observer<M>
where
    M: Message + Encode,
    M::Result: Decode,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        let observer = match self {
            Observer::Register(recipient) => proto::Observer::register(recipient.index()),
            Observer::Unregister(recipient) => proto::Observer::unregister(recipient.index()),
        };
        let message = ControlMessage::observer(observer);
        prost::Message::encoded_len(&message)
    }

    #[inline]
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
        let message = ControlMessage::observer(observer);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl<M> Decode for Observer<M>
where
    M: Message + Encode,
    M::Result: Decode,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::Observer(observer)) => match observer.observer {
                Some(proto::ObserverType::Register(actor_id)) => Ok(Observer::Register(
                    ctx.create_remote_address(actor_id)?.into(),
                )),
                Some(proto::ObserverType::Unregister(actor_id)) => Ok(Observer::Unregister(
                    ctx.create_remote_address(actor_id)?.into(),
                )),
                None => Err("missing field `observer` in the `Observer` message".into()),
            },

            _ => Err("message is not an `Observer` message".into()),
        }
    }
}

impl Encode for CronSignal {
    #[inline]
    fn encoded_len(&self) -> usize {
        let cron_signal = proto::CronSignal::new(*self as i32);
        let message = ControlMessage::cron_signal(cron_signal);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, _ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let cron_signal = proto::CronSignal::new(*self as i32);
        let message = ControlMessage::cron_signal(cron_signal);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for CronSignal {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::CronSignal(cron_signal)) => {
                CronSignal::try_from(cron_signal.signal as u8)
                    .map_err(|_| "invalid signal value in the `CronSignal` message".into())
            }
            _ => Err("message is not a `CronSignal` message".into()),
        }
    }
}

fn build_supervision_event_message<A>(event: &SupervisionEvent<A>) -> (ControlMessage, &Address<A>)
where
    A: Actor,
    A::Error: Display,
{
    let (proto_event, address) = match event {
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
    };
    (ControlMessage::supervision_event(proto_event), address)
}

impl<A> Encode for SupervisionEvent<A>
where
    A: Actor,
    A::Error: Display,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        let (message, _) = build_supervision_event_message(self);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        let (message, address) = build_supervision_event_message(self);
        ctx.ok_or(EncodeError::MissingEncodeContext)?
            .register(address)?;
        prost::Message::encode(&message, buf).map_err(Into::into)
    }

    #[inline]
    fn encode_to_bytes(&self, ctx: Option<&EncodeContext>) -> Result<Bytes, EncodeError> {
        let (message, address) = build_supervision_event_message(self);
        ctx.ok_or(EncodeError::MissingEncodeContext)?
            .register(address)?;
        let mut buf = BytesMut::with_capacity(prost::Message::encoded_len(&message));
        prost::Message::encode(&message, &mut buf)?;
        Ok(buf.freeze())
    }
}

impl Decode for RemoteSupervisionEvent {
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;
        let message: ControlMessage = prost::Message::decode(buf)?;

        match message.message {
            Some(proto::ControlMessageType::SupervisionEvent(event)) => match event.event {
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

                Some(proto::SupervisionEventType::State(state)) => {
                    Ok(RemoteSupervisionEvent::State(
                        ctx.create_remote_address(state.actor_id)?,
                        ActorState::try_from(state.state as u8).map_err(|_| {
                            DecodeError::from(
                                "invalid actor state value in the `SupervisionEvent` message",
                            )
                        })?,
                    ))
                }

                None => Err("missing field `event` in the `SupervisionEvent` message".into()),
            },

            _ => Err("message is not a `SupervisionEvent` message".into()),
        }
    }
}

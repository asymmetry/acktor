use std::fmt::Display;

use bytes::{Bytes, BytesMut};

use acktor::{
    Actor, ActorState, Message, SenderIndex, Signal,
    cron::CronSignal,
    observer::Observer,
    supervisor::{SupervisionEvent, Supervisor},
};
use acktor_ipc_proto::control_message::{self as proto, ControlMessage};

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode};
use crate::remote_address::RemoteAddress;
use crate::remote_message::{RemoteObserver, RemoteSupervisionEvent, RemoteSupervisor};

#[inline]
fn check_actor_id(actor_id: u64) -> Result<u64, DecodeError> {
    if actor_id.is_remote() {
        Err(DecodeError::DecodeRemoteAddress)
    } else {
        Ok(actor_id)
    }
}

impl Encode for Signal {
    #[inline]
    fn encoded_len(&self) -> usize {
        let signal = proto::Signal::new(*self as i32);
        let message = ControlMessage::signal(signal);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let signal = proto::Signal::new(*self as i32);
        let message = ControlMessage::signal(signal);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for Signal {
    #[inline]
    fn decode(buf: Bytes, _context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let supervisor = match self {
            Supervisor::Set(recipient) => {
                if recipient.is_remote() {
                    return Err(EncodeError::EncodeRemoteAddress);
                }
                proto::Supervisor::set(recipient.index())
            }
            Supervisor::Unset => proto::Supervisor::unset(),
        };
        let message = ControlMessage::supervisor(supervisor);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for RemoteSupervisor {
    #[inline]
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let session = context.ok_or::<DecodeError>("missing decode context".into())?;
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::Supervisor(supervisor)) => {
                match supervisor.supervisor {
                    Some(proto::SupervisorType::Set(actor_id)) => Ok(RemoteSupervisor::Set(
                        RemoteAddress::new(check_actor_id(actor_id)?, session.clone()),
                    )),
                    Some(proto::SupervisorType::Unset(())) => Ok(RemoteSupervisor::Unset),
                    None => Err("missing field `supervisor` in the `Supervisor` message".into()),
                }
            }
            _ => Err("message is not a `Supervisor` message".into()),
        }
    }
}

impl<M> Encode for Observer<M>
where
    M: Message,
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
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let observer = match self {
            Observer::Register(recipient) => {
                if recipient.is_remote() {
                    return Err(EncodeError::EncodeRemoteAddress);
                }
                proto::Observer::register(recipient.index())
            }
            Observer::Unregister(recipient) => {
                if recipient.is_remote() {
                    return Err(EncodeError::EncodeRemoteAddress);
                }
                proto::Observer::unregister(recipient.index())
            }
        };
        let message = ControlMessage::observer(observer);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for RemoteObserver {
    #[inline]
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let session = context.ok_or::<DecodeError>("missing decode context".into())?;
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::Observer(observer)) => match observer.observer {
                Some(proto::ObserverType::Register(actor_id)) => Ok(RemoteObserver::Register(
                    RemoteAddress::new(check_actor_id(actor_id)?, session.clone()),
                )),
                Some(proto::ObserverType::Unregister(actor_id)) => Ok(RemoteObserver::Unregister(
                    RemoteAddress::new(check_actor_id(actor_id)?, session.clone()),
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
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let cron_signal = proto::CronSignal::new(*self as i32);
        let message = ControlMessage::cron_signal(cron_signal);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }
}

impl Decode for CronSignal {
    #[inline]
    fn decode(buf: Bytes, _context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
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

impl<A> Encode for SupervisionEvent<A>
where
    A: Actor,
    A::Error: Display,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        let event = match self {
            SupervisionEvent::Warn(address, error) => {
                proto::SupervisionEvent::warn(address.index(), error.to_string())
            }
            SupervisionEvent::Terminated(address, error) => proto::SupervisionEvent::terminated(
                address.index(),
                error.as_ref().map(|e| e.to_string()),
            ),
            SupervisionEvent::State(address, state) => {
                proto::SupervisionEvent::state(address.index(), *state as i32)
            }
        };
        let message = ControlMessage::supervision_event(event);
        prost::Message::encoded_len(&message)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        let event = match self {
            SupervisionEvent::Warn(address, error) => {
                proto::SupervisionEvent::warn(address.index(), error.to_string())
            }
            SupervisionEvent::Terminated(address, error) => proto::SupervisionEvent::terminated(
                address.index(),
                error.as_ref().map(|e| e.to_string()),
            ),
            SupervisionEvent::State(address, state) => {
                proto::SupervisionEvent::state(address.index(), *state as i32)
            }
        };
        let message = ControlMessage::supervision_event(event);
        prost::Message::encode(&message, buf).map_err(Into::into)
    }

    #[inline]
    fn encode_to_bytes(&self) -> Result<Bytes, EncodeError> {
        let event = match self {
            SupervisionEvent::Warn(address, error) => {
                proto::SupervisionEvent::warn(address.index(), error.to_string())
            }
            SupervisionEvent::Terminated(address, error) => proto::SupervisionEvent::terminated(
                address.index(),
                error.as_ref().map(|e| e.to_string()),
            ),
            SupervisionEvent::State(address, state) => {
                proto::SupervisionEvent::state(address.index(), *state as i32)
            }
        };
        let message = ControlMessage::supervision_event(event);
        let mut buf = BytesMut::with_capacity(prost::Message::encoded_len(&message));
        prost::Message::encode(&message, &mut buf)?;

        Ok(buf.freeze())
    }
}

impl Decode for RemoteSupervisionEvent {
    #[inline]
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let session = context.ok_or::<DecodeError>("missing decode context".into())?;
        let message: ControlMessage = prost::Message::decode(buf)?;
        match message.message {
            Some(proto::ControlMessageType::SupervisionEvent(event)) => match event.event {
                Some(proto::SupervisionEventType::Warn(warn)) => Ok(RemoteSupervisionEvent::Warn(
                    RemoteAddress::new(check_actor_id(warn.actor_id)?, session.clone()),
                    warn.err,
                )),

                Some(proto::SupervisionEventType::Terminated(terminated)) => {
                    Ok(RemoteSupervisionEvent::Terminated(
                        RemoteAddress::new(check_actor_id(terminated.actor_id)?, session.clone()),
                        terminated.err,
                    ))
                }

                Some(proto::SupervisionEventType::State(state)) => {
                    Ok(RemoteSupervisionEvent::State(
                        RemoteAddress::new(check_actor_id(state.actor_id)?, session.clone()),
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

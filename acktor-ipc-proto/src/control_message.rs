mod signal {
    pub use crate::proto::control_message::Signal;
    pub use crate::proto::control_message::signal::Signal as SignalType;

    impl Signal {
        #[inline]
        pub fn new(signal: i32) -> Self {
            Self { signal }
        }

        #[inline]
        pub fn stop() -> Self {
            Self {
                signal: SignalType::Stop as i32,
            }
        }

        #[inline]
        pub fn terminate() -> Self {
            Self {
                signal: SignalType::Terminate as i32,
            }
        }
    }
}
pub use signal::*;

mod supervisor {
    pub use crate::proto::control_message::Supervisor;
    pub use crate::proto::control_message::supervisor::Supervisor as SupervisorType;

    impl Supervisor {
        #[inline]
        pub fn set(actor_id: u64) -> Self {
            Self {
                supervisor: Some(SupervisorType::Set(actor_id)),
            }
        }

        #[inline]
        pub fn unset() -> Self {
            Self {
                supervisor: Some(SupervisorType::Unset(())),
            }
        }
    }
}
pub use supervisor::*;

mod observer {
    pub use crate::proto::control_message::Observer;
    pub use crate::proto::control_message::observer::Observer as ObserverType;

    impl Observer {
        #[inline]
        pub fn register(actor_id: u64) -> Self {
            Self {
                observer: Some(ObserverType::Register(actor_id)),
            }
        }

        #[inline]
        pub fn unregister(actor_id: u64) -> Self {
            Self {
                observer: Some(ObserverType::Unregister(actor_id)),
            }
        }
    }
}
pub use observer::*;

mod cron_signal {
    pub use crate::proto::control_message::CronSignal;
    pub use crate::proto::control_message::cron_signal::CronSignal as CronSignalType;

    impl CronSignal {
        #[inline]
        pub fn new(signal: i32) -> Self {
            Self { signal }
        }

        #[inline]
        pub fn pause() -> Self {
            Self {
                signal: CronSignalType::Pause as i32,
            }
        }

        #[inline]
        pub fn resume() -> Self {
            Self {
                signal: CronSignalType::Resume as i32,
            }
        }
    }
}
pub use cron_signal::*;

mod supervision_event {
    pub use crate::proto::control_message::supervision_event::Event as SupervisionEventType;
    pub use crate::proto::control_message::supervision_event::state::ActorState;
    pub use crate::proto::control_message::{
        SupervisionEvent,
        supervision_event::{Panicked, State, Terminated, Warn},
    };

    impl SupervisionEvent {
        #[inline]
        pub fn warn(actor_id: u64, err: String) -> Self {
            Self {
                event: Some(SupervisionEventType::Warn(Warn { actor_id, err })),
            }
        }

        #[inline]
        pub fn terminated(actor_id: u64, err: Option<String>) -> Self {
            Self {
                event: Some(SupervisionEventType::Terminated(Terminated {
                    actor_id,
                    err,
                })),
            }
        }

        #[inline]
        pub fn panicked(actor_id: u64, info: String) -> Self {
            Self {
                event: Some(SupervisionEventType::Panicked(
                    crate::proto::control_message::supervision_event::Panicked { actor_id, info },
                )),
            }
        }

        #[inline]
        pub fn state(actor_id: u64, state: i32) -> Self {
            Self {
                event: Some(SupervisionEventType::State(State { actor_id, state })),
            }
        }
    }
}
pub use supervision_event::*;

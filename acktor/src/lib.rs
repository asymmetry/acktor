//! A pure-Rust actor framework built on top of the [Tokio](https://tokio.rs) async runtime,
//! inspired by Alice Ryhl's [Actors with Tokio](https://ryhl.io/blog/actors-with-tokio/).
//!
//! `acktor` builds on the patterns described in Alice Ryhl's blog post and extends them into a
//! structured library. Each actor runs as an independent `tokio` task with its own mailbox,
//! processing messages one at a time. Actors communicate exclusively through message passing —
//! there is no shared mutable state. The framework provides lifecycle hooks, supervision, an
//! observer pattern, and support for periodic tasks.
//!
//! # Quick Start
//!
//! An example `Counter` actor that handles arithmetic messages might be the following:
//!
//! ```rust
//! use acktor::{Actor, Context, Handler, Message, Signal};
//!
//! #[derive(Debug)]
//! struct Counter(i64);
//!
//! impl Actor for Counter {
//!     type Context = Context<Self>;
//!     type Error = String;
//! }
//!
//! #[derive(Debug)]
//! enum CounterMsg {
//!     Increment,
//!     Get,
//! }
//!
//! impl Message for CounterMsg {
//!     type Result = i64;
//! }
//!
//! impl Handler<CounterMsg> for Counter {
//!     type Result = i64;
//!
//!     async fn handle(&mut self, msg: CounterMsg, _ctx: &mut Self::Context) -> i64 {
//!         match msg {
//!             CounterMsg::Increment => self.0 += 1,
//!             CounterMsg::Get => {}
//!         }
//!         self.0
//!     }
//! }
//!
//! async fn start() {
//!     let (addr, handle) = Counter(0).start("counter").unwrap();
//!
//!     // fire-and-forget
//!     addr.do_send(CounterMsg::Increment).await.unwrap();
//!
//!     // request-reply
//!     let result = addr.send(CounterMsg::Get).await.unwrap().await.unwrap();
//!     println!("Counter: {}", result); // Counter: 1
//!
//!     addr.do_send(Signal::Stop).await.unwrap();
//!     handle.await.unwrap();
//! }
//! ```
//!
//! # Supervision
//!
//! Implement `Handler<SupervisionEvent<A>>` on the supervisor actor. Use the command
//! [`Supervisor::Set`][supervisor::Supervisor::Set] to attach the supervisor actor to the child
//! (or [`Supervisor::Unset`][supervisor::Supervisor::Unset] to detach). Since every actor handles
//! `Supervisor<A>` automatically, no extra wiring is needed on the child side.
//!
//! ```ignore
//! use acktor::{Actor, Context, Handler, supervisor::SupervisionEvent};
//!
//! struct Worker;
//!
//! impl Actor for Worker {
//!     type Context = Context<Self>;
//!     type Error = String;
//! }
//!
//! #[derive(Default)]
//! struct Watchdog;
//!
//! impl Actor for Watchdog {
//!     type Context = Context<Self>;
//!     type Error = String;
//! }
//!
//! impl Handler<SupervisionEvent<Worker>> for Watchdog {
//!     type Result = ();
//!
//!     async fn handle(&mut self, event: SupervisionEvent<Worker>, _ctx: &mut Self::Context) {
//!         println!("worker event: {:?}", event);
//!     }
//! }
//! ```
//!
//! # Observer
//!
//! For a subject actor, implement the [`SubjectActor<Event>`][observer::SubjectActor] trait so it
//! can emit `Event`s to registered observers. Every subject actor automatically gets a
//! `Handler<Observer<Event>>` implementation that manages the observers, so the observers can be
//! registered by sending [`Observer::Register`][observer::Observer::Register] commands to the
//! subject actor (or [`Observer::Unregister`][observer::Observer::Unregister] to stop receiving
//! events).
//!
//! ```ignore
//! use acktor::{Actor, Context, Message, observer::{ObserverSet, SubjectActor}};
//!
//! #[derive(Clone, Message)]
//! #[result_type(())]
//! struct Tick;
//!
//! #[derive(Default)]
//! struct Clock { observers: ObserverSet<Tick> }
//!
//! impl Actor for Clock {
//!     type Context = Context<Self>;
//!     type Error = String;
//! }
//!
//! impl SubjectActor<Tick> for Clock {
//!     fn observers_mut(&mut self) -> &mut ObserverSet<Tick> { &mut self.observers }
//! }
//! ```
//!
//! # Cron Tasks
//!
//! Use [`CronContext<Self>`][cron::CronContext] as the actor's context and implement
//! [`CronActor`][cron::CronActor] trait to opt in this feature. `CronActor` trait defines a
//! `task` method that is invoked repeatedly with a delay determined by its return value.
//!
//! ```ignore
//! use std::time::Duration;
//! use acktor::{Actor, cron::{CronActor, CronContext}};
//!
//! struct Heartbeat;
//!
//! impl Actor for Heartbeat {
//!     type Context = CronContext<Self>;
//!     type Error = String;
//! }
//!
//! impl CronActor for Heartbeat {
//!     async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
//!         println!("tick");
//!         Ok(Duration::from_secs(1))
//!     }
//! }
//! ```
//!
//! # Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | Yes | Re-exports the derive macros from [`acktor-derive`](https://docs.rs/acktor-derive). |
//! | `observer` | Yes | Enables the [`observer`] module. |
//! | `cron` | Yes | Enables the [`cron`] module. |
//! | `identifier` | No | Enables stable type identifiers ([`stable_type_id`], [`MessageId`]). |
//! | `ipc` | No | Enables IPC support: the [`codec`] module, [`BinaryMessage`][message::BinaryMessage], remote addressing ([`RemoteAddressable`], [`RemoteSpawnable`], [`RemoteProxy`]), and the `proto` re-export. Implies `identifier`. |
//! | `prost-codec` | No | Use an all-prost primitive codec instead of the default zerocopy + prost mix (useful for cross-language interop). |
//! | `bottleneck-warning` | No | Emits `tracing::debug!` logs when an observer's mailbox is full during notification, useful for spotting slow consumers. |
//! | `tokio-tracing` | No | Names spawned actor tasks for [`tokio-console`](https://docs.rs/console-subscriber). Requires building with `RUSTFLAGS="--cfg tokio_unstable"`. |
//!

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::uninlined_format_args)]

pub mod error;
pub use error::{ErrorReport, RecvError, SendError};

pub mod channel;
pub mod utils;

#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub mod codec;

#[cfg(feature = "identifier")]
#[cfg_attr(docsrs, doc(cfg(feature = "identifier")))]
pub mod stable_type_id;
#[cfg(feature = "identifier")]
#[cfg_attr(docsrs, doc(cfg(feature = "identifier")))]
pub use stable_type_id::{StableId, StableTypeId};

pub mod actor;
pub use actor::{Actor, ActorContext, ActorId, ActorState, JoinHandle, Stopping};
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use actor::{RemoteAddressable, RemoteSpawnable};

mod context;
pub use context::{Context, DEFAULT_MAILBOX_CAPACITY};

pub mod drain;
pub use drain::Drainable;

pub mod address;
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use address::RemoteProxy;
pub use address::{Address, Recipient, Sender, SenderInfo};

pub mod message;
#[cfg(feature = "identifier")]
#[cfg_attr(docsrs, doc(cfg(feature = "identifier")))]
pub use message::MessageId;
pub use message::{Handler, Message, MessageResponse};

pub mod envelope;

mod signal;
pub use signal::Signal;

pub mod supervisor;

#[cfg(feature = "observer")]
#[cfg_attr(docsrs, doc(cfg(feature = "observer")))]
pub mod observer;

#[cfg(feature = "cron")]
#[cfg_attr(docsrs, doc(cfg(feature = "cron")))]
pub mod cron;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Message, MessageResponse};
#[cfg(all(feature = "derive", feature = "identifier"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "derive", feature = "identifier"))))]
pub use acktor_derive::{MessageId, StableId};
#[cfg(all(feature = "derive", feature = "ipc"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "derive", feature = "ipc"))))]
pub use acktor_derive::{RemoteAddressable, remote};

/// Re-export the IPC protocol definitions for convenience.
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use acktor_ipc_proto as proto;

// re-export for use in derived code

#[doc(hidden)]
#[cfg(feature = "ipc")]
pub use bytes;
#[doc(hidden)]
#[cfg(feature = "identifier")]
pub use sha2_const;
#[doc(hidden)]
#[cfg(feature = "ipc")]
pub use tracing;

#[cfg(test)]
pub(crate) use utils::test_utils;

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
//! #[derive(Debug, Message)]
//! #[result_type(i64)]
//! enum CounterMsg {
//!     Increment,
//!     Get,
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
//! async fn run() {
//!     let (addr, handle) = Counter(0).run("counter").unwrap();
//!
//!     // fire-and-forget
//!     addr.do_send(CounterMsg::Increment).await.unwrap();
//!
//!     // request-reply
//!     let result = addr.send(CounterMsg::Get).await.unwrap().await.unwrap();
//!     println!("Counter: {result}"); // Counter: 1
//!
//!     addr.do_send(Signal::Stop).await.unwrap();
//!     handle.await.unwrap();
//! }
//! ```
//!
//! # Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `derive` | Yes | Enables `#[derive(Message)]` and `#[derive(MessageResponse)]` macros. |
//! | `tokio-tracing` | No | Names spawned actor tasks for [`tokio-console`](https://docs.rs/console-subscriber). Requires building with `RUSTFLAGS="--cfg tokio_unstable"`. |
//! | `bottleneck-warning` | No | Emits `tracing::debug!` logs when an observer's mailbox is full during notification, useful for spotting slow consumers. |

#![cfg_attr(docsrs, feature(doc_cfg))]

mod errors;
pub use errors::{ErrorReport, RecvError, SendError};

pub mod channel;

pub mod utils;

#[doc(hidden)]
pub mod actor;
pub use actor::{Actor, ActorContext, ActorId, ActorState, JoinHandle, Stopping};

mod context;
pub use context::{Context, DEFAULT_MAILBOX_CAPACITY};

pub mod address;
pub use address::{Address, Recipient, Sender, SenderId};

pub mod message;
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

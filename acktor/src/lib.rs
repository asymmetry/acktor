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
//! use acktor::{Actor, Context, Signal, message::{Handler, Message}};
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

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod errors {
    //! Re-exports some error types from tokio.

    pub use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};
}

mod utils;

mod actor;
pub use actor::{Actor, ActorContext, ActorState, Stopping};

mod context;
pub use context::{Context, DEFAULT_MAILBOX_CAPACITY};

pub mod address;
pub mod envelope;
pub mod message;

pub mod cron;

pub mod observer;
pub mod supervisor;

mod signal;
pub use signal::Signal;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub mod derive {
    //! Derive macros for defining messages and message responses.

    /// Implements the [`Message`][crate::message::Message] trait for a type.
    pub use acktor_derive::Message;

    /// Implements the [`MessageResponse`][crate::message::MessageResponse] trait for a type.
    pub use acktor_derive::MessageResponse;
}

pub mod report {
    //! Error reporting macro.
    //!
    //! This module provides a macro to report errors and their sources in a recursive way.

    pub use acktor_macros::report;
}

pub mod debug_trace {
    //! Debug trace macro.

    pub use acktor_macros::debug_trace;
}

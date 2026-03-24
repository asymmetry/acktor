//! Actor framework built on top of Tokio

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
    pub use actor_derive::Message;

    /// Implements the [`MessageResponse`][crate::message::MessageResponse] trait for a type.
    pub use actor_derive::MessageResponse;
}

pub mod report {
    //! Error reporting macro.
    //!
    //! This module provides a macro to report errors and their sources in a recursive way.

    pub use cactor_macros::report;
}

pub mod debug_trace {
    //! Debug trace macro.

    pub use cactor_macros::debug_trace;
}

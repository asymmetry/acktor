//! # acktor-ipc
//!
//! Interprocess communication for the [`acktor`](https://github.com/asymmetry/acktor) actor
//! framework.
//!
//! # About
//!
//! `acktor-ipc` lets actors in different processes talk to each other over a transport of your
//! choice. It introduces a [`Node`] actor that owns one or more listeners and per-connection
//! [`Session`] actors that mediate traffic.
//!
//! # Concepts
//!
//! - **Node** — a long-lived actor that holds the listener(s) for inbound connections, tracks the
//!   active sessions, and owns the registry of remote-addressable / remote-spawnable actor types.
//!   Send `Connect<C>` to it to dial out.
//! - **Session** — a per-connection actor that wraps a single [`IpcConnection`], routes inbound
//!   frames to the right local actor, forwards outbound messages, and correlates request/response
//!   tags.
//! - **RemoteAddressable** *(re-exported from `acktor`)* — derive on an actor (with
//!   `#[message(M1, M2, ...)]`) to declare the message types it can receive from other processes.
//!   Messages must implement `MessageId`, [`Encode`], and [`Decode`]; their `Result`s must
//!   implement [`Encode`] + [`Decode`].
//! - **RemoteSpawnable** *(re-exported from `acktor`)* — implement to allow other processes to
//!   spawn this actor on this node.
//! - **`#[remote]`** — attribute applied to the `impl Actor for ...` block of a remote-addressable
//!   actor; required so the actor exposes a remote mailbox.
//!
//! # Example
//!
//! A minimal message definition for IPC looks like:
//!
//! ```ignore
//! use acktor::{Message, MessageId};
//! use acktor_ipc::{Decode, Encode};
//! use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};
//!
//! #[derive(
//!     Debug, Clone, Copy,
//!     KnownLayout, Immutable, FromBytes, IntoBytes,
//!     Message, MessageId, Encode, Decode,
//! )]
//! #[codec(zerocopy)]
//! #[result_type(())]
//! #[repr(C)]
//! pub struct Ping {
//!     pub id: u64,
//!     pub timestamp: i64,
//! }
//! ```
//!
//! See the `pingpong` example in the repository for a complete client/server walkthrough using
//! the WebSocket transport.
//!
//! # Feature Flags
//!
//! Defaults: `derive`.
//!
//! | Feature     | Purpose                                        |
//! | ----------- | ---------------------------------------------- |
//! | `derive`    | Re-exports the `#[remote]` attribute macro.    |
//! | `pipe`      | Pipe transport (Unix sockets / Windows pipes). |
//! | `websocket` | WebSocket transport.                           |
//!
//! Neither transport is enabled by default — enable `pipe` and/or `websocket` to pick the
//! transports you want.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub use error::{NodeError, SessionError};

pub mod ipc_method;
pub use ipc_method::{IpcConnection, IpcListener};

pub(crate) mod remote;
pub use remote::{RemoteAddressable, RemoteSpawnable, StableId};

mod actor_ref;
pub use actor_ref::ActorRef;

pub mod node;
pub use node::{Node, NodeEvent};

pub mod session;
pub use session::Session;

pub use acktor::codec;
pub use acktor::codec::{Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError};

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::remote;

pub mod double_map;

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod errors;
pub use errors::{NodeError, SessionError};

pub mod ipc_method;
pub use ipc_method::{IpcConnection, IpcListener};

mod codec;
pub use codec::{Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError};

pub mod remote_actor;
pub use remote_actor::{RemoteActor, RemoteActorFactory};

mod actor_handle;
pub use actor_handle::ActorHandle;

pub mod node;
pub use node::{Node, NodeEvent};

pub mod session;
pub use session::{Session, SessionHandle};

mod remote_address;
pub use remote_address::RemoteAddress;

pub mod remote_message;
pub use remote_message::RemoteMessage;

/// Re-export of the generated IPC protocol crate.
pub use acktor_ipc_proto as proto;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Decode, Encode, RemoteActor, remote_actor};

// consider publish these two as separate crates

pub mod double_map;

// re-export some dependencies for use in derived code.

pub use bytes;

#[doc(hidden)]
pub use tracing;

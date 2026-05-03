#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub use error::{NodeError, SessionError};

pub mod ipc_method;
pub use ipc_method::{IpcConnection, IpcListener};

pub mod remote;
pub use remote::RemoteSpawnable;

mod actor_handle;
pub use actor_handle::ActorHandle;

pub mod node;
pub use node::{Node, NodeEvent};

pub mod session;
pub use session::{Session, SessionHandle};

/// Re-export of the generated IPC protocol crate.
pub use acktor_ipc_proto as proto;

pub use acktor::codec;
pub use acktor::codec::{Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError};

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Decode, Encode, RemoteActor, remote};

pub mod double_map;

// re-export some dependencies for use in derived code.

#[doc(hidden)]
pub use tracing;

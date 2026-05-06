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

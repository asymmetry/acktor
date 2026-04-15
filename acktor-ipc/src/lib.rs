#![cfg_attr(docsrs, feature(doc_cfg))]

// Re-export the bytes crate for use within derived code.
pub use bytes;

pub mod errors;

pub mod ipc_method;
pub use ipc_method::{IpcConnection, IpcListener};

mod codec;
pub use codec::{Decode, DecodeContext, Encode, EncodeContext};

mod remote_actor;
pub use remote_actor::{RemoteActor, RemoteActorFactory, RemoteActorRegistry};

mod actor_handle;
pub use actor_handle::ActorHandle;

pub mod node;
pub use node::Node;

pub mod session;
pub use session::Session;

mod remote_address;
pub use remote_address::RemoteAddress;

pub mod remote_message;
pub use remote_message::{RemoteMessage, RemoteMessageKind};

// Re-export the ipc protocol.
pub use acktor_ipc_proto as proto;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Decode, Encode, RemoteActor, remote};

pub mod double_map;

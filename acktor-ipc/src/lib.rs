#![cfg_attr(docsrs, feature(doc_cfg))]

// Re-export the bytes crate for use within derived code.
pub use bytes;

pub mod errors;

pub mod ipc_method;
pub use ipc_method::{IpcConnection, IpcListener};

mod codec;
pub use codec::{Decode, DecodeContext, Encode};

pub mod node;
pub use node::Node;

mod session;

mod remote_address;
pub use remote_address::{RemoteAddress, RemoteSender};

pub mod remote_message;
pub use remote_message::RemoteMessage;

// Re-export the ipc protocol.
pub use acktor_ipc_proto as proto;

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Decode, Encode};

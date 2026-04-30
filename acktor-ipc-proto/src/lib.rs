#![cfg_attr(docsrs, feature(doc_cfg))]

mod proto;

pub mod actor_message;
pub mod control_message;
pub mod ipc_message;
pub mod node_message;
pub mod utils;

#[cfg(feature = "adaptor")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptor")))]
pub mod adaptor;

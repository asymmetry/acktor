#![cfg_attr(docsrs, feature(doc_cfg))]

mod proto;

pub mod control_message;
pub mod message;
pub mod utils;

#[cfg(feature = "adaptor")]
#[cfg_attr(docsrs, doc(cfg(feature = "adaptor")))]
pub mod adaptor;

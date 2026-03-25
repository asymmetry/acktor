#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "observer")]
#[cfg_attr(docsrs, doc(cfg(feature = "observer")))]
pub mod observer;

pub mod report_impl;

#[cfg(feature = "debug-trace")]
#[cfg_attr(docsrs, doc(cfg(feature = "debug-trace")))]
pub mod trace;

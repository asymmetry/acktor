//! Channel primitives used by this crate, wrapping tokio's channels so receive operations yield
//! this crate's [`RecvError`][crate::errors::RecvError].

pub mod mpsc;
pub mod oneshot;

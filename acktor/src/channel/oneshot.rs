//! Oneshot channel with a receiver wraps [`tokio::sync::oneshot::Receiver`] and yields this
//! crate's [`RecvError`][crate::errors::RecvError] on failure.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use tokio::{
    sync::oneshot::Sender,
    time::{self, Duration},
};

use crate::errors::RecvError;

pub mod error {
    //! `Oneshot` error types.
    pub use crate::errors::RecvError;
}

/// A wrapper around [`tokio::sync::oneshot::Receiver`] whose receive methods yields this crate's
/// [`RecvError`][crate::errors::RecvError] on failure.
#[derive(Debug)]
#[repr(transparent)]
pub struct Receiver<T>(tokio::sync::oneshot::Receiver<T>);

impl<T> Receiver<T> {
    /// Prevents the associated [`Sender`] handle from sending a value.
    pub fn close(&mut self) {
        self.0.close();
    }

    /// Checks if this receiver is terminated.
    pub fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }

    /// Checks if a channel is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Attempts to receive a value.
    ///
    /// If a pending value exists in the channel, it is returned. If no value has been sent,
    /// the current task **will not** be registered for future notification.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        self.0.try_recv().map_err(Into::into)
    }

    /// Blocking receive to call outside of asynchronous contexts.
    pub fn blocking_recv(self) -> Result<T, RecvError> {
        self.0.blocking_recv().map_err(Into::into)
    }

    /// Awaits a value with a timeout, returning [`RecvError::Timeout`] if `timeout` elapses
    /// before the sender produces a value. The receiver is left intact on timeout so the
    /// caller can await it again.
    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        match time::timeout(timeout, self).await {
            Ok(res) => res,
            Err(_) => Err(RecvError::Timeout),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.map_err(Into::into))
    }
}

/// Creates a new oneshot channel, returning the sender/receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (tx, Receiver(rx))
}

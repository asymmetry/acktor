//! Mpsc channel with a receiver wrapping [`tokio::sync::mpsc::Receiver`] and yielding this
//! crate's [`RecvError`][crate::errors::RecvError] on failure.

pub use tokio::{
    sync::mpsc::{OwnedPermit, Permit, Sender, WeakSender, error},
    time::{self, Duration},
};

use crate::errors::RecvError;

/// A wrapper around [`tokio::sync::mpsc::Receiver`] whose receive methods return this crate's
/// [`RecvError`][crate::errors::RecvError] on failure.
#[derive(Debug)]
#[repr(transparent)]
pub struct Receiver<T>(tokio::sync::mpsc::Receiver<T>);

impl<T> Receiver<T> {
    /// Receives the next value for this receiver.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        self.0.recv().await.ok_or(RecvError::Closed)
    }

    /// Tries to receive the next value for this receiver.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        self.0.try_recv().map_err(Into::into)
    }

    /// Blocking receive to call outside of asynchronous contexts.
    pub fn blocking_recv(&mut self) -> Result<T, RecvError> {
        self.0.blocking_recv().ok_or(RecvError::Closed)
    }

    /// Closes the receiving half of the channel without dropping it.
    pub fn close(&mut self) {
        self.0.close();
    }

    /// Checks if a channel is closed.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Checks if a channel is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of messages in the channel.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the current capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns the maximum buffer capacity of the channel.
    pub fn max_capacity(&self) -> usize {
        self.0.max_capacity()
    }

    /// Returns the number of [`Sender`] handles.
    pub fn sender_strong_count(&self) -> usize {
        self.0.sender_strong_count()
    }

    /// Returns the number of [`WeakSender`] handles.
    pub fn sender_weak_count(&self) -> usize {
        self.0.sender_weak_count()
    }

    pub(crate) fn into_inner(self) -> tokio::sync::mpsc::Receiver<T> {
        self.0
    }

    /// Awaits a value with a timeout, returning [`RecvError::Timeout`] if `timeout` elapses
    /// before a value is produced. The receiver is left intact on timeout.
    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        match time::timeout(timeout, self.0.recv()).await {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(RecvError::Closed),
            Err(_) => Err(RecvError::Timeout),
        }
    }
}

/// Creates a new bounded mpsc channel, returning the sender/receiver halves.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (tx, Receiver(rx))
}

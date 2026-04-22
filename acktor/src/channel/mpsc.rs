//! A mpsc channel with a receiver wrapping [`tokio::sync::mpsc::Receiver`].
//!
//! This channel is used for sending messages to actors. The sender half is wrapped by the actor's
//! [`Address`][crate::address::Address] and the receiver half is wrapped by the actor's
//! [`Mailbox`][crate::address::Mailbox].
//!

use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::{
    sync::mpsc::Receiver as MpscReceiver,
    time::{self, Duration},
};

pub use tokio::sync::mpsc::{OwnedPermit, Permit, Sender, WeakSender, error};

use crate::errors::RecvError;

/// Receives values from the associated [`Sender`].
///
/// Instances are created by the [`channel`] function.
///
/// It is a wrapper around [`tokio::sync::mpsc::Receiver`].
#[derive(Debug)]
#[repr(transparent)]
pub struct Receiver<T>(MpscReceiver<T>);

impl<T> Receiver<T> {
    /// Receives the next value for this receiver.
    pub fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> {
        self.0.recv().map(|v| v.ok_or(RecvError::Closed))
    }

    /// Receives the next values for this receiver and extends `buffer`.
    pub fn recv_many(&mut self, buffer: &mut Vec<T>, limit: usize) -> impl Future<Output = usize> {
        self.0.recv_many(buffer, limit)
    }

    /// Tries to receive the next value for this receiver.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        self.0.try_recv().map_err(Into::into)
    }

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_recv(&mut self) -> Result<T, RecvError> {
        self.0.blocking_recv().ok_or(RecvError::Closed)
    }

    /// Variant of [`Self::recv_many`] for blocking contexts.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_recv_many(&mut self, buffer: &mut Vec<T>, limit: usize) -> usize {
        self.0.blocking_recv_many(buffer, limit)
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

    /// Polls to receive the next message on this channel.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        self.0.poll_recv(cx).map(|r| r.ok_or(RecvError::Closed))
    }

    /// Polls to receive multiple messages on this channel, extending the provided buffer.
    pub fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<T>,
        limit: usize,
    ) -> Poll<usize> {
        self.0.poll_recv_many(cx, buffer, limit)
    }

    /// Returns the number of [`Sender`] handles.
    pub fn sender_strong_count(&self) -> usize {
        self.0.sender_strong_count()
    }

    /// Returns the number of [`WeakSender`] handles.
    pub fn sender_weak_count(&self) -> usize {
        self.0.sender_weak_count()
    }

    // new methods

    /// Receives the next value for this receiver with a timeout.
    ///
    /// It returns [`RecvError::Timeout`] if `timeout` elapses before a value is received. The
    /// receiver is left intact so the caller can await it again.
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

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_recv() {
        // async `recv` delivers a value
        let (tx, mut rx) = channel::<u32>(4);
        tx.send(1).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), 1);

        // `try_recv` on an empty open channel is Empty
        assert!(matches!(rx.try_recv(), Err(RecvError::Empty)));

        // `try_recv` yields a value once present
        tx.send(2).await.unwrap();
        assert_eq!(rx.try_recv().unwrap(), 2);

        // dropping all senders closes the channel
        drop(tx);
        assert!(matches!(rx.recv().await, Err(RecvError::Closed)));
        assert!(matches!(rx.try_recv(), Err(RecvError::Closed)));

        // `blocking_recv` must run off the async runtime
        let (tx, mut rx) = channel::<u32>(4);
        let rx_handle = tokio::task::spawn_blocking(move || {
            let v = rx.blocking_recv().unwrap();
            let closed = rx.blocking_recv();
            (v, closed)
        });
        tx.send(42).await.unwrap();
        drop(tx);
        let (v, closed) = rx_handle.await.unwrap();
        assert_eq!(v, 42);
        assert!(matches!(closed, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn recv_timeout() {
        let (tx, mut rx) = channel::<u32>(1);

        // times out when nothing is sent
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(10)).await,
            Err(RecvError::Timeout),
        ));

        // the receiver is intact after a timeout — sending now succeeds
        tx.send(7).await.unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).await.unwrap(), 7);

        // closed channel returns Closed immediately, not Timeout
        drop(tx);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)).await,
            Err(RecvError::Closed),
        ));
    }

    #[tokio::test]
    async fn recv_many_behavior() {
        let (tx, mut rx) = channel::<u32>(8);
        for i in 0..5 {
            tx.send(i).await.unwrap();
        }

        // `recv_many` drains up to `limit` messages already buffered
        let mut buf = Vec::new();
        let n = rx.recv_many(&mut buf, 3).await;
        assert_eq!(n, 3);
        assert_eq!(buf, vec![0, 1, 2]);

        // `blocking_recv_many` pulls the rest off the async runtime
        let rx_handle = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            let n = rx.blocking_recv_many(&mut buf, 10);
            (n, buf)
        });
        drop(tx);
        let (n, buf) = rx_handle.await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf, vec![3, 4]);
    }

    #[tokio::test]
    async fn channel_state_and_close() {
        let (tx, mut rx) = channel::<u32>(4);

        // fresh channel: empty, open, full capacity available
        assert!(rx.is_empty());
        assert!(!rx.is_closed());
        assert_eq!(rx.len(), 0);
        assert_eq!(rx.capacity(), 4);
        assert_eq!(rx.max_capacity(), 4);

        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        assert_eq!(rx.len(), 2);
        assert!(!rx.is_empty());
        assert_eq!(rx.capacity(), 2);
        assert_eq!(rx.max_capacity(), 4);

        // sender counts reflect strong and weak handles
        let tx2 = tx.clone();
        let weak = tx.downgrade();
        assert_eq!(rx.sender_strong_count(), 2);
        assert_eq!(rx.sender_weak_count(), 1);
        drop(tx2);
        drop(weak);
        assert_eq!(rx.sender_strong_count(), 1);
        assert_eq!(rx.sender_weak_count(), 0);

        // `close()` closes the receiver without dropping it; pending messages remain drainable
        rx.close();
        assert!(rx.is_closed());
        assert!(tx.send(3).await.is_err());
        assert_eq!(rx.recv().await.unwrap(), 1);
        assert_eq!(rx.recv().await.unwrap(), 2);
        assert!(matches!(rx.recv().await, Err(RecvError::Closed)));
    }
}

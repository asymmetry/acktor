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

use crate::error::RecvError;

pub use tokio::sync::mpsc::{OwnedPermit, Permit, Sender, WeakSender, error};

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
    use std::future::poll_fn;

    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn test_send_recv() -> Result<()> {
        // send and recv
        let (tx, mut rx) = channel::<u32>(4);
        tx.send(1).await?;
        assert_eq!(rx.recv().await?, 1);

        // try_recv on an empty open channel is Empty
        assert!(matches!(rx.try_recv(), Err(RecvError::Empty)));

        // try_send and try_recv
        tx.try_send(2)?;
        assert_eq!(rx.try_recv()?, 2);

        tx.send(3).await?;
        assert_eq!(poll_fn(|cx| rx.poll_recv(cx)).await?, 3);

        // dropping all senders closes the channel
        drop(tx);
        assert!(matches!(rx.recv().await, Err(RecvError::Closed)));
        assert!(matches!(rx.try_recv(), Err(RecvError::Closed)));

        // blocking_recv works
        let (tx, mut rx) = channel::<u32>(4);
        let join_handle = tokio::task::spawn_blocking(move || {
            let result = rx.blocking_recv();
            let closed = rx.blocking_recv();
            (result, closed)
        });
        tx.send(4).await?;
        drop(tx);
        let (result, closed) = join_handle.await?;
        assert_eq!(result?, 4);
        assert!(matches!(closed, Err(RecvError::Closed)));

        Ok(())
    }

    #[tokio::test]
    async fn test_recv_timeout() -> Result<()> {
        let (tx, mut rx) = channel::<u32>(1);

        // times out when nothing is sent
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(100)).await,
            Err(RecvError::Timeout),
        ));

        // recv succeeds if a value is sent before the timeout
        tx.send(7).await?;
        assert_eq!(rx.recv_timeout(Duration::from_millis(100)).await?, 7);

        // closed channel returns Closed immediately, not Timeout
        drop(tx);
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(100)).await,
            Err(RecvError::Closed),
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_recv_many() -> Result<()> {
        let (tx, mut rx) = channel::<u32>(8);
        for i in 0..8 {
            tx.send(i).await?;
        }

        // recv_many pulls 3 messages
        let mut buf = Vec::new();
        let n = rx.recv_many(&mut buf, 3).await;
        assert_eq!(n, 3);
        assert_eq!(buf, vec![0, 1, 2]);

        // poll_recv_many also pulls 3
        let mut buf = Vec::new();
        let n = poll_fn(|cx| rx.poll_recv_many(cx, &mut buf, 3)).await;
        assert_eq!(n, 3);
        assert_eq!(buf, vec![3, 4, 5]);

        // blocking_recv_many pulls the rest
        let rx_handle = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            let n = rx.blocking_recv_many(&mut buf, 10);
            (n, buf)
        });
        drop(tx);
        let (n, buf) = rx_handle.await?;
        assert_eq!(n, 2);
        assert_eq!(buf, vec![6, 7]);

        Ok(())
    }

    #[tokio::test]
    async fn test_misc() -> Result<()> {
        let (tx, mut rx) = channel::<u32>(4);

        // fresh channel: empty, open, full capacity available
        assert!(rx.is_empty());
        assert!(!rx.is_closed());
        assert_eq!(rx.len(), 0);
        assert_eq!(rx.capacity(), 4);
        assert_eq!(rx.max_capacity(), 4);

        // send some messages
        tx.send(1).await?;
        tx.send(2).await?;
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

        // close closes the receiver without dropping it; pending messages remain drainable
        rx.close();
        assert!(rx.is_closed());
        assert!(tx.is_closed());
        assert!(matches!(tx.send(3).await, Err(error::SendError(3))));
        assert_eq!(rx.recv().await?, 1);
        assert_eq!(rx.recv().await?, 2);
        assert!(matches!(rx.recv().await, Err(RecvError::Closed)));

        Ok(())
    }
}

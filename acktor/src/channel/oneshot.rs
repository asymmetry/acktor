//! A one-shot channel wraps a [`tokio::sync::oneshot`] channel which uses
//! `Result<T, Box<dyn StdError + Send + Sync>>` as payload.
//!
//! This channel is used for sending message responses back to the message sender.
//!

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::{
    sync::oneshot::{
        Receiver as OneshotReceiver, Sender as OneshotSender, channel as oneshot_channel,
    },
    time::{self, Duration},
};

use crate::errors::{BoxError, RecvError, SendError};

/// Sends a value to the associated [`Receiver`].
///
/// A pair of both a [`Sender`] and a [`Receiver`] are created by the channel function.
///
/// It is a wrapper around [`tokio::sync::oneshot::Sender`] which uses
/// `Result<T, Box<dyn StdError + Send + Sync>>` as payload. The sender can send either a value of
/// type `T` or a custom error, which will be converted to [`RecvError::Other`] by the receiver.
#[derive(Debug)]
#[repr(transparent)]
pub struct Sender<T>(OneshotSender<Result<T, BoxError>>);

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could not be sent.
    pub fn send(self, t: T) -> Result<(), SendError<T>> {
        self.0.send(Ok(t)).map_err(|err| match err {
            Ok(t) => SendError::Closed(t),
            Err(_) => unreachable!(),
        })
    }

    /// Waits for the associated [`Receiver`] handle to close.
    pub async fn closed(&mut self) {
        self.0.closed().await
    }

    /// Returns `true` if the associated [`Receiver`] handle has been dropped.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Checks whether the `oneshot` channel has been closed, and if not, schedules the `Waker`
    /// in the provided `Context` to receive a notification when the channel is closed.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.0.poll_closed(cx)
    }

    // new methods

    /// Attempts to send an error on this channel, returning the boxed error back if it could not
    /// be sent.
    pub fn send_err<E>(self, err: E) -> Result<(), SendError<BoxError>>
    where
        E: Into<BoxError>,
    {
        self.0.send(Err(err.into())).map_err(|err| match err {
            Ok(_) => unreachable!(),
            Err(e) => SendError::Closed(e),
        })
    }
}

/// Receives a value from the associated [`Sender`].
///
/// A pair of both a [`Sender`] and a [`Receiver`] are created by the channel function.
///
/// It is a wrapper around [`tokio::sync::oneshot::Receiver`] which uses
/// `Result<T, Box<dyn StdError + Send + Sync>>` as payload. The sender can send either a value of
/// type `T` or a custom error, which will be converted to [`RecvError::Other`] by the receiver.
#[derive(Debug)]
#[repr(transparent)]
pub struct Receiver<T>(OneshotReceiver<Result<T, BoxError>>);

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
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        Ok(self.0.try_recv()??)
    }

    /// Blocking receive to call outside of asynchronous contexts.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_recv(self) -> Result<T, RecvError> {
        Ok(self.0.blocking_recv()??)
    }

    // new methods

    /// Awaits a value with a timeout.
    ///
    /// It returns [`RecvError::Timeout`] if `timeout` elapses before a value is received. The
    /// receiver is left intact so the caller can await it again.
    pub async fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        match time::timeout(timeout, self).await {
            Ok(r) => r,
            Err(_) => Err(RecvError::Timeout),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|r| Ok(r??))
    }
}

/// Creates a new oneshot channel, returning the sender/receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = oneshot_channel();
    (Sender(tx), Receiver(rx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::SendError;

    #[tokio::test]
    async fn test_send_recv() {
        // value path
        let (tx, rx) = channel::<u32>();
        tx.send(42).unwrap();
        assert_eq!(rx.await.unwrap(), 42);

        // custom error path
        let (tx, rx) = channel::<u32>();
        tx.send_err("boom").unwrap();
        match rx.await {
            Err(RecvError::Other(e)) => assert_eq!(e.to_string(), "boom"),
            other => panic!("expected RecvError::Other, got {:?}", other),
        }

        // sender dropped → Closed
        let (tx, rx) = channel::<u32>();
        drop(tx);
        assert!(matches!(rx.await, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn test_try_send_recv() {
        let (tx, mut rx) = channel::<u32>();
        assert!(matches!(rx.try_recv(), Err(RecvError::Empty)));
        tx.send(11).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 11);

        let (tx, mut rx) = channel::<u32>();
        drop(tx);
        assert!(matches!(rx.try_recv(), Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn test_send_closed() {
        // send returns the unsent value
        let (tx, rx) = channel::<u32>();
        drop(rx);
        match tx.send(7) {
            Err(SendError::Closed(v)) => assert_eq!(v, 7),
            other => panic!("expected SendError::Closed(7), got {:?}", other),
        }

        // send_err returns the boxed payload
        let (tx, rx) = channel::<u32>();
        drop(rx);
        match tx.send_err("late") {
            Err(SendError::Closed(e)) => assert_eq!(e.to_string(), "late"),
            other => panic!("expected SendError::Closed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_close_detection() {
        // dropping the receiver closes the sender
        let (mut tx, rx) = channel::<u32>();
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
        tx.closed().await;

        // explicit Receiver::close() also signals the sender
        let (mut tx, mut rx) = channel::<u32>();
        rx.close();
        tx.closed().await;
        assert!(tx.is_closed());
    }

    #[tokio::test]
    async fn test_recv_timeout() {
        let (tx, mut rx) = channel::<u32>();
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(10)).await,
            Err(RecvError::Timeout)
        ));
        // receiver remains usable after timeout
        tx.send(99).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).await.unwrap(), 99);
    }
}

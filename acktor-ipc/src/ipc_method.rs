//! Traits for Inter-Process Communication (IPC) and some pre-implemented IPC methods.

use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

#[cfg(feature = "pipe")]
#[cfg_attr(docsrs, doc(cfg(feature = "pipe")))]
pub mod pipe;

#[cfg(feature = "websocket")]
#[cfg_attr(docsrs, doc(cfg(feature = "websocket")))]
pub mod websocket;

/// A boxed `Send` future returning `Result<T, std::io::Error>`, used as the return type
/// of dyn-compatible trait methods in this module.
pub type IoFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Describes the behavior of an IPC listener which accepts incoming IPC connections.
///
/// A [`Node`][crate::node::Node] uses a listener to accept inbound connections. Outbound
/// connections are initiated by calling [`IpcConnection::connect`] on a concrete connection
/// type directly.
pub trait IpcListener: Send + Sync + 'static {
    /// Returns the local endpoint the listener is bound to.
    fn local_endpoint(&self) -> &str;

    /// Accepts an incoming IPC connection.
    ///
    /// # Cancel safety
    ///
    /// The implementation should be cancel safe. If the method is used as the event in a
    /// [`tokio::select!`](tokio::select) statement and some other branch completes first, then
    /// it is guaranteed that no new connections were accepted by this method.
    fn accept(&self) -> IoFuture<'_, Box<dyn IpcConnection>>;
}

/// Describes the behavior of an IPC connection.
///
/// An IPC connection should be a duplex communication channel between two end points. If the
/// underlying protocol is not duplex, the implementation should simulate the duplex behavior,
/// e.g., by using two separate connections.
///
/// Implementations are responsible for message framing: one call to [`send`](Self::send) must
/// correspond to exactly one call to [`recv`](Self::recv) on the remote end, regardless of how
/// the underlying transport delivers bytes.
pub trait IpcConnection: Send + Sync + 'static {
    /// Connects to an IPC listener at a specific endpoint.
    ///
    /// This is a constructor-style method that returns a concrete connection, so it requires
    /// `Self: Sized`.
    fn connect(endpoint: &str) -> impl Future<Output = Result<Self, Error>> + Send
    where
        Self: Sized;

    /// Returns the peer endpoint of the IPC connection.
    fn peer_endpoint(&self) -> &str;

    /// Closes the IPC connection.
    fn close(&mut self) -> IoFuture<'_, ()>;

    /// Sends a message to the other end of the connection.
    ///
    /// The entire `buf` is delivered as a single framed message.
    fn send(&mut self, buf: Bytes) -> IoFuture<'_, ()>;

    /// Receives the next message from the other end of the connection.
    ///
    /// Returns the message payload as a [`Bytes`] value; implementations should return a slice
    /// of their internal read buffer whenever possible to avoid copying.
    ///
    /// # Cancel safety
    ///
    /// The implementation should be cancel safe. If the method is used as the event in a
    /// [`tokio::select!`](tokio::select) statement and some other branch completes first, then
    /// it is guaranteed that no data was read from the underlying connection.
    fn recv(&mut self) -> IoFuture<'_, Bytes>;
}

impl<T> IpcListener for Box<T>
where
    T: IpcListener + ?Sized,
{
    fn local_endpoint(&self) -> &str {
        (**self).local_endpoint()
    }

    fn accept(&self) -> IoFuture<'_, Box<dyn IpcConnection>> {
        (**self).accept()
    }
}

impl<T> IpcListener for Arc<T>
where
    T: IpcListener + ?Sized,
{
    fn local_endpoint(&self) -> &str {
        (**self).local_endpoint()
    }

    fn accept(&self) -> IoFuture<'_, Box<dyn IpcConnection>> {
        (**self).accept()
    }
}

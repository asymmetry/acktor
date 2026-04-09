//! Traits for Inter-Process Communication (IPC) and some pre-implemented IPC methods.

use std::io::Error;

use bytes::Bytes;

#[cfg(feature = "pipe")]
#[cfg_attr(docsrs, doc(cfg(feature = "pipe")))]
pub mod pipe;

#[cfg(feature = "websocket")]
#[cfg_attr(docsrs, doc(cfg(feature = "websocket")))]
pub mod websocket;

/// Describes the behavior of a listener which could accept incoming IPC connections.
pub trait IpcListener: Send + Sync + 'static {
    type Connection: IpcConnection;

    /// Accepts an incoming IPC connection.
    fn accept(&self) -> impl Future<Output = Result<Self::Connection, Error>> + Send;
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
    fn connect(endpoint: &str) -> impl Future<Output = Result<Self, Error>> + Send
    where
        Self: Sized;

    /// Returns the endpoint of the IPC connection.
    fn endpoint(&self) -> &str;

    /// Closes the IPC connection.
    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    /// Sends a message to the other end of the connection.
    ///
    /// The entire `buf` is delivered as a single framed message.
    fn send(&mut self, buf: Bytes) -> impl Future<Output = Result<(), Error>> + Send;

    /// Receives the next message from the other end of the connection.
    ///
    /// Returns the message payload as a [`Bytes`] value; implementations should return a slice
    /// of their internal read buffer whenever possible to avoid copying.
    ///
    /// The implementation should be cancel safe, i.e., if the future is dropped, no data should
    /// be read from the connection.
    fn recv(&mut self) -> impl Future<Output = Result<Bytes, Error>> + Send;
}

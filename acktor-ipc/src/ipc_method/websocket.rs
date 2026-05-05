//! IPC method implementation using WebSocket.

use std::io::{Error, ErrorKind, Result};

use bytes::Bytes;
use futures_util::{
    FutureExt, SinkExt, StreamExt, TryFutureExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async,
    tungstenite::{Message as WebSocketMessage, error::Error as WebSocketError},
};
use tracing::info;

use super::{IoFuture, IpcConnection, IpcListener};

fn ws_error_to_io_error(e: WebSocketError) -> Error {
    match e {
        WebSocketError::Io(e) => e,
        e => Error::other(e),
    }
}

/// IPC listener implemented with WebSocket.
#[derive(Debug)]
pub struct WebSocketListener {
    listener: TcpListener,
    local_addr: String,
}

impl WebSocketListener {
    /// Constructs a new [`WebSocketListener`] with the given bind address.
    pub async fn bind(local_addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(local_addr).await?;

        Ok(Self {
            listener,
            local_addr: local_addr.to_string(),
        })
    }
}

impl IpcListener for WebSocketListener {
    fn local_endpoint(&self) -> &str {
        self.local_addr.as_str()
    }

    fn accept(&self) -> IoFuture<'_, Box<dyn IpcConnection>> {
        Box::pin(async move {
            let (socket, peer_addr) = self.listener.accept().await?;

            let ws_stream = accept_async(MaybeTlsStream::Plain(socket))
                .await
                .map_err(ws_error_to_io_error)?;

            info!("Accepted a new websocket connection from {}", peer_addr);

            Ok(
                Box::new(WebSocketConnection::new(ws_stream, peer_addr.to_string()))
                    as Box<dyn IpcConnection>,
            )
        })
    }
}

/// IPC connection implemented with WebSocket.
#[derive(Debug)]
pub struct WebSocketConnection {
    peer_addr: String,
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WebSocketMessage>,
    rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending_pong: Option<Bytes>,
}

impl WebSocketConnection {
    fn new(ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>, peer_addr: String) -> Self {
        let (tx, rx) = ws_stream.split();

        Self {
            peer_addr,
            tx,
            rx,
            pending_pong: None,
        }
    }

    async fn send(&mut self, msg: WebSocketMessage) -> Result<()> {
        self.tx.send(msg).await.map_err(ws_error_to_io_error)
    }

    async fn recv(&mut self) -> Result<WebSocketMessage> {
        self.rx
            .next()
            .await
            .ok_or(ErrorKind::ConnectionAborted)?
            .map_err(ws_error_to_io_error)
    }
}

impl IpcConnection for WebSocketConnection {
    async fn connect(peer_addr: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(peer_addr)
            .await
            .map_err(ws_error_to_io_error)?;

        info!("Connected to websocket server {}", peer_addr);

        Ok(Self::new(ws_stream, peer_addr.to_string()))
    }

    fn peer_endpoint(&self) -> &str {
        self.peer_addr.as_str()
    }

    fn close(&mut self) -> IoFuture<'_, ()> {
        self.tx.close().map_err(ws_error_to_io_error).boxed()
    }

    fn send(&mut self, buf: Bytes) -> IoFuture<'_, ()> {
        self.send(WebSocketMessage::Binary(buf)).boxed()
    }

    fn recv(&mut self) -> IoFuture<'_, Bytes> {
        async move {
            loop {
                // send any buffered Pong, the payload is cloned so `self.pending_pong` keeps
                // the value until the send completes
                // NOTE: clone a Bytes is cheap
                if let Some(payload) = self.pending_pong.clone() {
                    self.send(WebSocketMessage::Pong(payload)).await?; // #1
                    self.pending_pong = None;
                }

                let message = self.recv().await?;

                match message {
                    WebSocketMessage::Binary(payload) => return Ok(payload),
                    WebSocketMessage::Ping(payload) => {
                        // buffer the payload for sending in the next loop
                        // if the call is cancelled at #1, the payload is still buffered
                        // and next call retries
                        self.pending_pong = Some(payload);
                    }
                    WebSocketMessage::Pong(_) => {}
                    WebSocketMessage::Close(_) => {
                        return Err(Error::new(
                            ErrorKind::ConnectionAborted,
                            "received close message",
                        ));
                    }
                    _ => return Err(Error::other("received non-binary message")),
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq, assert_ne};

    use super::*;

    #[tokio::test]
    async fn test_listener() -> anyhow::Result<()> {
        // bind success + local_endpoint reflects the string passed to `bind`
        let listener = WebSocketListener::bind("127.0.0.1:0").await?;
        assert_eq!(listener.local_endpoint(), "127.0.0.1:0");
        drop(listener);

        // bind to an invalid address fails
        WebSocketListener::bind("not-a-valid-address")
            .await
            .expect_err("bind to an invalid address must fail");

        // connect with no server on the port fails
        let probe = TcpListener::bind("127.0.0.1:0").await?;
        let addr = probe.local_addr()?;
        drop(probe);
        WebSocketConnection::connect(&format!("ws://{addr}"))
            .await
            .expect_err("connect with no server must fail");

        Ok(())
    }

    #[tokio::test]
    async fn test_connection() -> anyhow::Result<()> {
        let listener = WebSocketListener::bind("127.0.0.1:0").await?;
        let addr = listener.listener.local_addr()?;
        let url = format!("ws://{addr}");

        let server = tokio::spawn(async move {
            // 1st client: echo roundtrip
            let mut conn = listener.accept().await?;
            assert!(!conn.peer_endpoint().is_empty());
            let msg = conn.recv().await?;
            conn.send(msg).await?;

            // 2nd client: server closes the connection
            let mut conn = listener.accept().await?;
            conn.close().await?;

            Ok::<_, anyhow::Error>(())
        });

        // client 1: full roundtrip, then IpcConnection::close
        let mut client = WebSocketConnection::connect(&url).await?;
        assert_eq!(client.peer_endpoint(), url);
        let payload = Bytes::from_static(b"hello");
        <WebSocketConnection as IpcConnection>::send(&mut client, payload.clone()).await?;
        let received = <WebSocketConnection as IpcConnection>::recv(&mut client).await?;
        assert_eq!(received, payload);
        <WebSocketConnection as IpcConnection>::close(&mut client).await?;

        // client 2: server closes, recv returns ConnectionAborted
        let mut client = WebSocketConnection::connect(&url).await?;
        let err = <WebSocketConnection as IpcConnection>::recv(&mut client)
            .await
            .expect_err("recv after server close must fail");
        assert_eq!(err.kind(), ErrorKind::ConnectionAborted);

        server.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_websocket_control_frames() -> anyhow::Result<()> {
        let tcp = TcpListener::bind("127.0.0.1:0").await?;
        let addr = tcp.local_addr()?;
        let url = format!("ws://{addr}");

        let server = tokio::spawn(async move {
            // session 1: Ping -> client should reply with Pong, then consume the Binary
            let (socket, peer) = tcp.accept().await?;
            let ws = accept_async(MaybeTlsStream::Plain(socket)).await?;
            let mut conn = WebSocketConnection::new(ws, peer.to_string());
            conn.send(WebSocketMessage::Ping(Bytes::from_static(b"p")))
                .await?;
            conn.send(WebSocketMessage::Binary(Bytes::from_static(b"hi")))
                .await?;
            let reply = conn.recv().await?;
            assert!(matches!(&reply, WebSocketMessage::Pong(p) if p.as_ref() == b"p"));

            // session 2: Pong is ignored, next Binary returned
            let (socket, peer) = tcp.accept().await?;
            let ws = accept_async(MaybeTlsStream::Plain(socket)).await?;
            let mut conn = WebSocketConnection::new(ws, peer.to_string());
            conn.send(WebSocketMessage::Pong(Bytes::from_static(b"p")))
                .await?;
            conn.send(WebSocketMessage::Binary(Bytes::from_static(b"ok")))
                .await?;

            // session 3: Text frame is rejected (not ConnectionAborted)
            let (socket, peer) = tcp.accept().await?;
            let ws = accept_async(MaybeTlsStream::Plain(socket)).await?;
            let mut conn = WebSocketConnection::new(ws, peer.to_string());
            conn.send(WebSocketMessage::Text("unexpected".into()))
                .await?;

            Ok::<_, anyhow::Error>(())
        });

        // session 1
        let mut client = WebSocketConnection::connect(&url).await?;
        // pong is automatically handled
        let bytes = <WebSocketConnection as IpcConnection>::recv(&mut client).await?;
        assert_eq!(bytes.as_ref(), b"hi");

        // session 2
        let mut client = WebSocketConnection::connect(&url).await?;
        let bytes = <WebSocketConnection as IpcConnection>::recv(&mut client).await?;
        assert_eq!(bytes.as_ref(), b"ok");

        // session 3
        let mut client = WebSocketConnection::connect(&url).await?;
        let err = <WebSocketConnection as IpcConnection>::recv(&mut client)
            .await
            .expect_err("text message should be rejected");
        assert_ne!(err.kind(), ErrorKind::ConnectionAborted);

        server.await??;

        Ok(())
    }
}

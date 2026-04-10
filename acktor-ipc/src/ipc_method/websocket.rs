//! IPC method implementation using WebSocket.

use std::io::{Error, ErrorKind, Result};

use bytes::Bytes;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, connect_async,
    tungstenite::{Message as WebSocketMessage, error::Error as WebSocketError},
};
use tracing::info;

use super::{IoFuture, IpcConnection, IpcListener};

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

    fn accept<'a>(&'a self) -> IoFuture<'a, Box<dyn IpcConnection>> {
        Box::pin(async move {
            let (socket, peer_addr) = self.listener.accept().await?;

            let ws_stream =
                accept_async(MaybeTlsStream::Plain(socket))
                    .await
                    .map_err(|e| match e {
                        WebSocketError::Io(e) => e,
                        e => Error::other(e),
                    })?;

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
        self.tx.send(msg).await.map_err(|e| match e {
            WebSocketError::Io(e) => e,
            e => Error::other(e),
        })
    }

    async fn recv(&mut self) -> Result<WebSocketMessage> {
        self.rx
            .next()
            .await
            .ok_or(ErrorKind::ConnectionAborted)?
            .map_err(|e| match e {
                WebSocketError::Io(e) => e,
                e => Error::other(e),
            })
    }
}

impl IpcConnection for WebSocketConnection {
    async fn connect(peer_addr: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(peer_addr).await.map_err(|e| match e {
            WebSocketError::Io(e) => e,
            e => Error::other(e),
        })?;

        info!("Connected to websocket server {}", peer_addr);

        Ok(Self::new(ws_stream, peer_addr.to_string()))
    }

    fn peer_endpoint(&self) -> &str {
        self.peer_addr.as_str()
    }

    fn close<'a>(&'a mut self) -> IoFuture<'a, ()> {
        Box::pin(async move {
            self.tx.close().await.map_err(|e| match e {
                WebSocketError::Io(e) => e,
                e => Error::other(e),
            })
        })
    }

    fn send<'a>(&'a mut self, buf: Bytes) -> IoFuture<'a, ()> {
        Box::pin(self.send(WebSocketMessage::Binary(buf)))
    }

    fn recv<'a>(&'a mut self) -> IoFuture<'a, Bytes> {
        Box::pin(async move {
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
        })
    }
}

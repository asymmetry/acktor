//! IPC method implementation using WebSocket.

use std::io::{Error, ErrorKind};

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

use super::{IpcConnection, IpcListener};

/// IPC listener implemented with WebSocket.
#[derive(Debug)]
#[repr(transparent)]
pub struct WebSocketListener(pub TcpListener);

impl WebSocketListener {
    pub async fn new(addr: &str) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self(listener))
    }
}

impl IpcListener for WebSocketListener {
    type Connection = WebSocketConnection;

    async fn accept(&self) -> Result<Self::Connection, Error> {
        let (socket, addr) = self.0.accept().await?;

        let ws_stream = accept_async(MaybeTlsStream::Plain(socket))
            .await
            .map_err(|e| match e {
                WebSocketError::Io(e) => e,
                e => Error::other(e),
            })?;

        info!("Accepted a new websocket connection from {}", addr);

        Ok(WebSocketConnection::new(ws_stream, addr.to_string()))
    }
}

/// IPC connection implemented with WebSocket.
#[derive(Debug)]
pub struct WebSocketConnection {
    endpoint: String,
    tx: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WebSocketMessage>,
    rx: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl WebSocketConnection {
    pub fn new(ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>, endpoint: String) -> Self {
        let (tx, rx) = ws_stream.split();

        Self { endpoint, tx, rx }
    }
}

impl IpcConnection for WebSocketConnection {
    async fn connect(endpoint: &str) -> Result<Self, Error> {
        let (ws_stream, _) = connect_async(endpoint).await.map_err(|e| match e {
            WebSocketError::Io(e) => e,
            e => Error::other(e),
        })?;

        info!("Connected to websocket server {}", endpoint);

        Ok(Self::new(
            ws_stream,
            endpoint
                .trim_start_matches("ws://")
                .trim_start_matches("wss://")
                .to_string(),
        ))
    }

    fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.tx.close().await.map_err(|e| match e {
            WebSocketError::Io(e) => e,
            e => Error::other(e),
        })?;

        Ok(())
    }

    async fn send(&mut self, buf: Bytes) -> Result<(), Error> {
        self.tx
            .send(WebSocketMessage::Binary(buf))
            .await
            .map_err(|e| match e {
                WebSocketError::Io(e) => e,
                e => Error::other(e),
            })?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes, Error> {
        loop {
            let message = self
                .rx
                .next()
                .await
                .ok_or_else(|| Error::from(ErrorKind::ConnectionAborted))?
                .map_err(|e| match e {
                    WebSocketError::Io(e) => e,
                    e => Error::other(e),
                })?;

            match message {
                WebSocketMessage::Binary(payload) => return Ok(payload),
                WebSocketMessage::Ping(payload) => {
                    self.tx
                        .send(WebSocketMessage::Pong(payload))
                        .await
                        .map_err(|e| match e {
                            WebSocketError::Io(e) => e,
                            e => Error::other(e),
                        })?;
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
}

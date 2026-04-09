//! IPC method implementation using Unix domain sockets and Windows named pipes.

use std::io::{Error, ErrorKind};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use interprocess::local_socket::{
    GenericNamespaced, ListenerOptions,
    tokio::{RecvHalf, SendHalf, prelude::*},
};
use tokio_util::codec::{FramedRead, FramedWrite, length_delimited::LengthDelimitedCodec};
use tracing::info;

use super::{IpcConnection, IpcListener};

/// IPC listener implemented with Unix domain sockets and Windows named pipes.
#[derive(Debug)]
pub struct PipeListener {
    pub name: String,
    pub listener: LocalSocketListener,
}

impl PipeListener {
    pub fn new(name: &str) -> Result<Self, Error> {
        let name_string = name.to_string();
        let name = name.to_ns_name::<GenericNamespaced>()?;

        let opts = ListenerOptions::new().name(name);
        let listener = opts.create_tokio()?;

        Ok(Self {
            name: name_string,
            listener,
        })
    }
}

impl IpcListener for PipeListener {
    type Connection = PipeConnection;

    async fn accept(&self) -> Result<Self::Connection, Error> {
        let stream = self.listener.accept().await?;

        info!("Accepted a new pipe connection");

        Ok(PipeConnection::new(stream, self.name.clone()))
    }
}

/// IPC connection implemented with Unix domain sockets and Windows named pipes.
#[derive(Debug)]
pub struct PipeConnection {
    endpoint: String,
    tx: FramedWrite<SendHalf, LengthDelimitedCodec>,
    rx: FramedRead<RecvHalf, LengthDelimitedCodec>,
}

impl PipeConnection {
    pub fn new(stream: LocalSocketStream, endpoint: String) -> Self {
        let (rx, tx) = stream.split();
        let codec = LengthDelimitedCodec::new();

        Self {
            endpoint,
            tx: FramedWrite::new(tx, codec.clone()),
            rx: FramedRead::new(rx, codec),
        }
    }
}

impl IpcConnection for PipeConnection {
    async fn connect(endpoint: &str) -> Result<Self, Error> {
        let stream =
            LocalSocketStream::connect(endpoint.to_ns_name::<GenericNamespaced>()?).await?;

        info!("Connected to pipe {}", endpoint);

        Ok(Self::new(stream, endpoint.to_string()))
    }

    fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn send(&mut self, buf: Bytes) -> Result<(), Error> {
        self.tx.send(buf).await?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<Bytes, Error> {
        let frame = self
            .rx
            .next()
            .await
            .ok_or_else(|| Error::from(ErrorKind::ConnectionAborted))??;

        Ok(frame.freeze())
    }
}

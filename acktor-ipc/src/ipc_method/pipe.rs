//! IPC method implementation using Unix domain sockets and Windows named pipes.

use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use interprocess::local_socket::{
    GenericNamespaced, ListenerOptions,
    tokio::{RecvHalf, SendHalf, prelude::*},
};
use tokio_util::codec::{FramedRead, FramedWrite, length_delimited::LengthDelimitedCodec};
use tracing::info;

use super::{IoFuture, IpcConnection, IpcListener};

/// IPC listener implemented with Unix domain sockets and Windows named pipes.
#[derive(Debug)]
pub struct PipeListener {
    name: String,
    listener: LocalSocketListener,
    accept_counter: AtomicU64,
}

impl PipeListener {
    /// Constructs a new [`PipeListener`] with the given pipe name.
    pub fn new(name: &str) -> Result<Self, Error> {
        let name_string = name.to_string();
        let name = name.to_ns_name::<GenericNamespaced>()?;

        let opts = ListenerOptions::new().name(name);
        let listener = opts.create_tokio()?;

        Ok(Self {
            name: name_string,
            listener,
            accept_counter: AtomicU64::new(0),
        })
    }
}

impl IpcListener for PipeListener {
    fn local_endpoint(&self) -> &str {
        self.name.as_str()
    }

    fn accept<'a>(&'a self) -> IoFuture<'a, Box<dyn IpcConnection>> {
        Box::pin(async move {
            let stream = self.listener.accept().await?;

            let id = self.accept_counter.fetch_add(1, Ordering::Relaxed);
            let name = format!("{}#{}", self.name, id);

            info!("Accepted a new pipe connection: {}", name);

            Ok(Box::new(PipeConnection::new(stream, name)) as Box<dyn IpcConnection>)
        })
    }
}

/// IPC connection implemented with Unix domain sockets and Windows named pipes.
#[derive(Debug)]
pub struct PipeConnection {
    name: String,
    tx: FramedWrite<SendHalf, LengthDelimitedCodec>,
    rx: FramedRead<RecvHalf, LengthDelimitedCodec>,
}

impl PipeConnection {
    fn new(stream: LocalSocketStream, name: String) -> Self {
        let (rx, tx) = stream.split();
        let codec = LengthDelimitedCodec::new();

        Self {
            name,
            tx: FramedWrite::new(tx, codec.clone()),
            rx: FramedRead::new(rx, codec),
        }
    }
}

impl IpcConnection for PipeConnection {
    async fn connect(name: &str) -> Result<Self, Error> {
        let stream = LocalSocketStream::connect(name.to_ns_name::<GenericNamespaced>()?).await?;

        info!("Connected to pipe {}", name);

        Ok(Self::new(stream, name.to_string()))
    }

    fn peer_endpoint(&self) -> &str {
        self.name.as_str()
    }

    fn close<'a>(&'a mut self) -> IoFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn send<'a>(&'a mut self, buf: Bytes) -> IoFuture<'a, ()> {
        Box::pin(self.tx.send(buf))
    }

    fn recv<'a>(&'a mut self) -> IoFuture<'a, Bytes> {
        Box::pin(async move {
            let frame = self
                .rx
                .next()
                .await
                .ok_or_else(|| Error::from(ErrorKind::ConnectionAborted))??;

            Ok(frame.freeze())
        })
    }
}

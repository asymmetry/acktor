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

    fn accept(&self) -> IoFuture<'_, Box<dyn IpcConnection>> {
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

    fn close(&mut self) -> IoFuture<'_, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn send(&mut self, buf: Bytes) -> IoFuture<'_, ()> {
        Box::pin(self.tx.send(buf))
    }

    fn recv(&mut self) -> IoFuture<'_, Bytes> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_pipe_name() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!(
            "acktor-ipc-pipe-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[tokio::test]
    async fn test_listener() {
        // new + local_endpoint
        let name = unique_pipe_name();
        let listener = PipeListener::new(&name).unwrap();
        assert_eq!(listener.local_endpoint(), name);

        // duplicate bind to the same name fails
        PipeListener::new(&name).expect_err("second bind to the same name must fail");

        // connect to an unbound name fails
        let missing = unique_pipe_name();
        PipeConnection::connect(&missing)
            .await
            .expect_err("connect to non-existent pipe must fail");
    }

    #[tokio::test]
    async fn test_connection() {
        let name = unique_pipe_name();
        let listener = PipeListener::new(&name).unwrap();
        let server_name = name.clone();

        let server = tokio::spawn(async move {
            // 1st client: echo roundtrip; peer_endpoint uses the `#0` suffix
            let mut c1 = listener.accept().await.expect("accept c1");
            assert_eq!(c1.peer_endpoint(), format!("{server_name}#0"));
            let msg = c1.recv().await.expect("recv c1");
            c1.send(msg).await.expect("send c1");
            c1.close().await.expect("close c1");

            // 2nd client: accept counter increments to `#1`; drop without writing to exercise
            // the client-side ConnectionAborted path
            let c2 = listener.accept().await.expect("accept c2");
            assert_eq!(c2.peer_endpoint(), format!("{server_name}#1"));
            drop(c2);
        });

        // client 1: full roundtrip
        let mut client1 = PipeConnection::connect(&name).await.expect("connect c1");
        assert_eq!(client1.peer_endpoint(), name);
        let payload = Bytes::from_static(b"hello");
        client1.send(payload.clone()).await.expect("send");
        assert_eq!(client1.recv().await.expect("recv"), payload);
        client1.close().await.expect("close");

        // client 2: server drops its connection, recv returns ConnectionAborted
        let mut client2 = PipeConnection::connect(&name).await.expect("connect c2");
        server.await.unwrap();
        let err = client2
            .recv()
            .await
            .expect_err("recv should fail after peer drop");
        assert_eq!(err.kind(), ErrorKind::ConnectionAborted);
    }
}

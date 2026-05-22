//! Test-only facilities shared across the crate's unit tests.
//!
//! This module is compiled only under `cfg(test)` and is excluded from the coverage report (see
//! the `**/test_utils.*` ignore rule in `codecov.yml`), so nothing here should count against
//! coverage targets.
//!
//! The remote actor below ([`Echo`]) is implemented entirely by hand instead of via the derive
//! macros, so these utilities compile even with `--no-default-features` (i.e. with the crate's
//! `derive` feature turned off).

use std::any::{Any, TypeId};
use std::io;
use std::sync::OnceLock;

use bytes::{Bytes, BytesMut};

use acktor::{
    Actor, Address, Context, Handler, JoinHandle,
    actor::RemoteAddressable,
    address::RemoteMailbox,
    codec::{
        Codec, CodecTable, Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError,
        MessageCodec,
    },
    message::{BinaryMessage, Message, MessageId},
    stable_type_id::{StableId, StableTypeId},
    utils::TypeMap,
};

use crate::ipc_method::{IoFuture, IpcConnection};
use crate::node::actor_mgr::ActorMgr;
use crate::remote::{RemoteMailboxRegistry, RemoteSpawnable};
use crate::session::Session;

/// A mock [`IpcConnection`] whose [`send`](IpcConnection::send) always fails.
///
/// `recv` parks forever, so a [`Session`]'s `run_loop` only ever fires on its mailbox branch.
/// This lets tests drive the outbound-send failure branches in the session handlers in isolation,
/// without a real transport.
pub(crate) struct FailingConnection {
    peer: String,
}

impl FailingConnection {
    pub(crate) fn new() -> Self {
        Self {
            peer: "failing-peer".to_string(),
        }
    }
}

impl IpcConnection for FailingConnection {
    async fn connect(_endpoint: &str) -> io::Result<Self>
    where
        Self: Sized,
    {
        unreachable!("FailingConnection is constructed directly in tests")
    }

    fn peer_endpoint(&self) -> &str {
        &self.peer
    }

    fn close(&mut self) -> IoFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn send(&mut self, _buf: Bytes) -> IoFuture<'_, ()> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "mock send failure",
            ))
        })
    }

    fn recv(&mut self) -> IoFuture<'_, Bytes> {
        // Never resolves: the run_loop's `connection.recv()` select branch parks here while the
        // test drives the session through its mailbox.
        Box::pin(std::future::pending())
    }
}

/// A message handled by [`Echo`], used to construct remote commands in tests.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ping {
    pub by: i64,
}

impl Message for Ping {
    type Result = i64;
}

impl MessageId for Ping {
    const ID: u64 = 1;
}

impl Encode for Ping {
    #[inline]
    fn encoded_len(&self) -> usize {
        8
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(&self.by.to_le_bytes());
        Ok(())
    }
}

impl Decode for Ping {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        // used in tests only, so it is implemented to be infallible
        let mut arr = [0u8; 8];
        let len = buf.len().min(8);
        arr[..len].copy_from_slice(&buf[..len]);
        Ok(Ping {
            by: i64::from_le_bytes(arr),
        })
    }
}

/// A minimal remote-addressable / remote-spawnable actor for exercising the session's
/// `RemoteCreateActor` / `RemoteGetActor` handlers.
///
/// All the trait impls below (`Actor::remote_mailbox`, [`Codec`], [`RemoteAddressable`],
/// [`StableId`], [`Handler<BinaryMessage>`]) are written by hand — they mirror what the
/// `#[derive(RemoteAddressable)]` / `#[derive(StableId)]` / `#[remote]` macros would generate —
/// so this module does not depend on the `derive` feature.
#[derive(Debug, Default)]
pub(crate) struct Echo {
    total: i64,
}

impl Actor for Echo {
    type Context = Context<Self>;
    type Error = io::Error;

    // The `#[remote]` attribute macro generates exactly this override.
    fn remote_mailbox(address: Address<Self>) -> Option<RemoteMailbox> {
        Some(address.into())
    }
}

impl StableId for Echo {
    const TYPE_ID: StableTypeId = StableTypeId::from_stable_type_name("Echo");
}

impl Codec for Echo {
    fn codec_table() -> &'static CodecTable {
        static TABLE: OnceLock<CodecTable> = OnceLock::new();
        TABLE.get_or_init(|| {
            let mut map = TypeMap::<MessageCodec>::default();
            map.insert(
                TypeId::of::<Ping>(),
                MessageCodec {
                    message_id: Ping::ID,
                    encode_msg: |any, ctx| {
                        let m = any.downcast_ref::<Ping>().expect("TypeId invariant");
                        m.encode_to_bytes(ctx)
                    },
                    decode_res: |bytes, ctx| Ok(Box::new(i64::decode(bytes, ctx)?) as Box<dyn Any>),
                },
            );
            CodecTable::new(map)
        })
    }
}

impl RemoteAddressable for Echo {}

impl RemoteSpawnable for Echo {
    fn create_remote(
        label: String,
        _config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), Self::Error> {
        Echo::default().start(label)
    }
}

impl Handler<Ping> for Echo {
    type Result = i64;

    async fn handle(&mut self, msg: Ping, _ctx: &mut Self::Context) -> i64 {
        self.total += msg.by;
        self.total
    }
}

impl Handler<BinaryMessage> for Echo {
    type Result = ();

    async fn handle(&mut self, msg: BinaryMessage, ctx: &mut Self::Context) {
        let BinaryMessage {
            message_id,
            bytes,
            result_tx,
            decode_msg_ctx,
            encode_res_ctx,
            ..
        } = msg;

        let decode_msg_ctx = decode_msg_ctx
            .as_deref()
            .map(|ctx| ctx as &dyn DecodeContext);

        if message_id == Ping::ID {
            match Ping::decode(bytes, decode_msg_ctx) {
                Ok(ping) => {
                    let result = self.handle(ping, ctx).await;
                    if let Some(tx) = result_tx {
                        let encode_res_ctx = encode_res_ctx
                            .as_deref()
                            .map(|ctx| ctx as &dyn EncodeContext);
                        match result.encode_to_bytes(encode_res_ctx) {
                            Ok(bytes) => {
                                let _ = tx.send(bytes);
                            }
                            Err(e) => {
                                let _ = tx.send_err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    if let Some(tx) = result_tx {
                        let _ = tx.send_err(e);
                    }
                }
            }
        }
    }
}

/// Boots a running [`Session`] actor backed by a [`FailingConnection`]. The returned address
/// keeps the actor (and, transitively, its `ActorMgr`) alive for the duration of the test; the
/// join handles are detached.
pub(crate) fn start_failing_session() -> Address<Session> {
    let registry = RemoteMailboxRegistry::default();

    let (actor_mgr, _mgr_handle) =
        ActorMgr::new(registry.clone(), Default::default(), Default::default())
            .start("test-mgr")
            .expect("actor mgr starts");

    let session = Session::new(Box::new(FailingConnection::new()), registry, actor_mgr);

    let (address, _session_handle) =
        Session::create("failing-session", |_ctx| Ok(session)).expect("session starts");

    address
}

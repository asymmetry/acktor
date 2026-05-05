use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::hash::{Hash, Hasher};
#[cfg(feature = "ipc")]
use std::{
    any::{Any, TypeId},
    future,
    num::NonZeroU64,
    sync::{Arc, Weak},
};

#[cfg(feature = "ipc")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "ipc")]
use futures_util::FutureExt;
#[cfg(feature = "ipc")]
use tokio::runtime;

#[cfg(feature = "ipc")]
use super::TypeMap;
use crate::actor::Actor;
use crate::address::{Address, Mailbox};
use crate::channel::mpsc;
use crate::context::Context;
use crate::envelope::Envelope;
use crate::message::{Handler, Message};
#[cfg(feature = "ipc")]
use crate::{
    actor::RemoteAddressable,
    address::{DoSendResult, DoSendResultFuture, EmptyFuture, RemoteMailbox, RemoteProxy},
    codec::{
        Codec, CodecTable, Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError,
        MessageCodec,
    },
    message::BinaryMessage,
};
#[cfg(feature = "identifier")]
use crate::{
    message::MessageId,
    stable_type_id::{StableId, StableTypeId},
};

#[derive(Debug)]
pub struct TestError(pub String);

impl Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for TestError {}

impl From<String> for TestError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TestError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

pub fn hash_of<T>(value: &T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug)]
pub struct Dummy;

impl Actor for Dummy {
    type Context = Context<Self>;
    type Error = TestError;

    #[cfg(feature = "ipc")]
    fn remote_mailbox(address: Address<Self>) -> Option<RemoteMailbox> {
        Some(address.into())
    }
}

#[cfg(feature = "ipc")]
impl Codec for Dummy {
    fn codec_table() -> &'static CodecTable {
        static TABLE: std::sync::OnceLock<CodecTable> = std::sync::OnceLock::new();
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
                    decode_res: |_, _| Ok(Box::new(()) as Box<dyn Any>),
                },
            );
            CodecTable::new(map)
        })
    }
}

#[cfg(feature = "ipc")]
impl RemoteAddressable for Dummy {}

#[cfg(feature = "identifier")]
impl StableId for Dummy {
    const TYPE_ID: StableTypeId = StableTypeId::from_stable_type_name("Dummy");
}

#[derive(Debug)]
pub struct Ping(pub u32);

impl Message for Ping {
    type Result = ();
}

#[cfg(feature = "identifier")]
impl MessageId for Ping {
    const ID: u64 = 1;
}

#[cfg(feature = "ipc")]
impl Encode for Ping {
    #[inline]
    fn encoded_len(&self) -> usize {
        4
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(&self.0.to_le_bytes());
        Ok(())
    }
}

#[cfg(feature = "ipc")]
impl Decode for Ping {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        // used in test only so it is implemented to be infallible
        let mut arr = [0u8; 4];
        let len = buf.len().min(4);
        arr[..len].copy_from_slice(&buf[..len]);
        Ok(Ping(u32::from_le_bytes(arr)))
    }
}

impl Handler<Ping> for Dummy {
    type Result = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) {}
}

#[cfg(feature = "ipc")]
impl Handler<BinaryMessage> for Dummy {
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

pub fn make_address(capacity: usize) -> (Address<Dummy>, Mailbox<Dummy>) {
    let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
    (Address::new(tx), Mailbox::new(rx))
}

#[cfg(feature = "ipc")]
pub struct DummyProxy {
    runtime: runtime::Handle,
    me: Weak<Self>,
}

#[cfg(feature = "ipc")]
impl DummyProxy {
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            runtime: runtime::Handle::current(),
            me: me.clone(),
        })
    }
}

#[cfg(feature = "ipc")]
impl RemoteProxy for DummyProxy {
    fn runtime(&self) -> runtime::Handle {
        self.runtime.clone()
    }

    fn encode_context(&self) -> Option<&dyn EncodeContext> {
        Some(self)
    }

    fn decode_context(&self) -> Option<&dyn DecodeContext> {
        Some(self)
    }

    fn index(&self) -> NonZeroU64 {
        NonZeroU64::new(42).unwrap()
    }

    fn closed(&self) -> EmptyFuture<'_> {
        future::ready(()).boxed()
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn capacity(&self) -> usize {
        usize::MAX
    }

    fn do_send(&self, msg: BinaryMessage) -> DoSendResultFuture<'_, ()> {
        async move {
            let BinaryMessage { result_tx, .. } = msg;
            if let Some(tx) = result_tx {
                let _ = tx.send(Vec::new().into());
            }
            Ok(())
        }
        .boxed()
    }

    fn try_do_send(&self, msg: BinaryMessage) -> DoSendResult<()> {
        let BinaryMessage { result_tx, .. } = msg;
        if let Some(tx) = result_tx {
            let _ = tx.send(Vec::new().into());
        }
        Ok(())
    }

    fn do_send_timeout(
        &self,
        msg: BinaryMessage,
        _timeout: std::time::Duration,
    ) -> DoSendResultFuture<'_, ()> {
        async move {
            let BinaryMessage { result_tx, .. } = msg;
            if let Some(tx) = result_tx {
                let _ = tx.send(Vec::new().into());
            }
            Ok(())
        }
        .boxed()
    }

    fn blocking_do_send(&self, msg: BinaryMessage) -> DoSendResult<()> {
        let BinaryMessage { result_tx, .. } = msg;
        if let Some(tx) = result_tx {
            let _ = tx.send(Vec::new().into());
        }
        Ok(())
    }
}

#[cfg(feature = "ipc")]
impl EncodeContext for DummyProxy {
    fn register(&self, _actor: RemoteMailbox) -> Result<(), EncodeError> {
        Ok(())
    }
}

#[cfg(feature = "ipc")]
impl DecodeContext for DummyProxy {
    fn remote_proxy(&self) -> Option<Arc<dyn RemoteProxy + Send + Sync>> {
        self.me
            .upgrade()
            .map(|proxy| proxy as Arc<dyn RemoteProxy + Send + Sync>)
    }
}

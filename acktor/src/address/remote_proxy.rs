use bytes::Bytes;
use tokio::runtime;

use super::sender::{DoSendResult, DoSendResultFuture, EmptyFuture};
use crate::channel::oneshot;
use crate::codec::{DecodeContext, EncodeContext};

pub trait RemoteProxy {
    fn runtime(&self) -> runtime::Handle;

    fn encode_context(&self) -> Option<&dyn EncodeContext>;

    fn decode_context(&self) -> Option<&dyn DecodeContext>;

    fn closed(&self) -> EmptyFuture<'_>;

    fn is_closed(&self) -> bool;

    fn capacity(&self) -> usize;

    fn do_send(
        &self,
        actor_id: u64,
        message_id: u64,
        bytes: Bytes,
        raw_tx: Option<oneshot::Sender<Bytes>>,
    ) -> DoSendResultFuture<'_, ()>;

    fn try_do_send(
        &self,
        actor_id: u64,
        message_id: u64,
        bytes: Bytes,
        raw_tx: Option<oneshot::Sender<Bytes>>,
    ) -> DoSendResult<()>;

    fn do_send_timeout(
        &self,
        actor_id: u64,
        message_id: u64,
        bytes: Bytes,
        timeout: std::time::Duration,
        raw_tx: Option<oneshot::Sender<Bytes>>,
    ) -> DoSendResultFuture<'_, ()>;

    fn blocking_do_send(
        &self,
        actor_id: u64,
        message_id: u64,
        bytes: Bytes,
        raw_tx: Option<oneshot::Sender<Bytes>>,
    ) -> DoSendResult<()>;
}

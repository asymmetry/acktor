use std::num::NonZeroU64;
use std::sync::{Arc, Weak};

use futures_util::{FutureExt, TryFutureExt};
use tokio::{runtime, time::Duration};

use acktor::{
    Address,
    address::{DoSendResult, DoSendResultFuture, EmptyFuture},
};

use super::Session;
use crate::codec::{DecodeContext, EncodeContext, EncodeError};
use crate::remote::{BinaryMessage, RemoteMailbox, RemoteMailboxRegistry, RemoteProxy};

pub struct Proxy {
    session: Address<Session>,
    registry: RemoteMailboxRegistry,
    runtime: runtime::Handle,
    me: Weak<Self>,
}

impl Proxy {
    #[inline]
    pub fn new(session: Address<Session>, registry: RemoteMailboxRegistry) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            session,
            registry,
            runtime: runtime::Handle::current(),
            me: me.clone(),
        })
    }
}

impl RemoteProxy for Proxy {
    #[inline]
    fn runtime(&self) -> runtime::Handle {
        self.runtime.clone()
    }

    #[inline]
    fn encode_context(&self) -> Option<&dyn EncodeContext> {
        Some(self)
    }

    #[inline]
    fn decode_context(&self) -> Option<&dyn DecodeContext> {
        Some(self)
    }

    #[inline]
    fn index(&self) -> NonZeroU64 {
        NonZeroU64::new(self.session.index().as_local())
            .expect("session index is always non-zero since it can only be created by a node")
    }

    #[inline]
    fn closed(&self) -> EmptyFuture<'_> {
        self.session.closed().boxed()
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.session.capacity()
    }

    #[inline]
    fn do_send(&self, msg: BinaryMessage) -> DoSendResultFuture<'_, ()> {
        self.session
            .do_send(msg)
            .map_err(|e| e.without_msg())
            .boxed()
    }

    #[inline]
    fn try_do_send(&self, msg: BinaryMessage) -> DoSendResult<()> {
        self.session.try_do_send(msg).map_err(|e| e.without_msg())
    }

    #[inline]
    fn do_send_timeout(&self, msg: BinaryMessage, timeout: Duration) -> DoSendResultFuture<'_, ()> {
        self.session
            .do_send_timeout(msg, timeout)
            .map_err(|e| e.without_msg())
            .boxed()
    }

    #[inline]
    fn blocking_do_send(&self, msg: BinaryMessage) -> DoSendResult<()> {
        self.session
            .blocking_do_send(msg)
            .map_err(|e| e.without_msg())
    }
}

impl EncodeContext for Proxy {
    #[inline]
    fn register(&self, actor: RemoteMailbox) -> Result<(), EncodeError> {
        self.registry.insert(actor);

        Ok(())
    }
}

impl DecodeContext for Proxy {
    #[inline]
    fn remote_proxy(&self) -> Option<Arc<dyn RemoteProxy + Send + Sync>> {
        self.me
            .upgrade()
            .map(|proxy| proxy as Arc<dyn RemoteProxy + Send + Sync>)
    }
}

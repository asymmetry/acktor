use std::fmt::{self, Debug};
use std::future;
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{FutureExt, TryFutureExt};
use tracing::Instrument;

use acktor::{
    Address, Message, Recipient, SendError, Sender, SenderId,
    address::{DoSendResult, DoSendResultFuture, SendResult, SendResultFuture},
    channel::oneshot,
};

use crate::codec::{Decode, DecodeContext, Encode, EncodeContext};
use crate::remote_actor::RemoteActorRegistry;
use crate::remote_message::RemoteMessage;
use crate::session::Session;

// TODO: better way to handle the error received from the remote peer

/// A type which is used to send messages to a remote actor.
///
/// [`Sender`] trait is implemented for this type, so it can be converted into a [`Recipient`]
/// type easily. This will allow us to store a remote address in the same place as a locals
/// address, and send messages with it without caring about the underlying transport details.
///
/// **NOTE**: user should not send a received [`RemoteAddress`] to another remote actor,
/// it is considered as a meaningless operation.
pub struct RemoteAddress {
    /// Computed once at construction, see [`RemoteAddress::new`].
    index: u64,
    /// Index of the corresponding actor in the remote node.
    remote_actor_id: u64,
    /// Address of the IPC connection session actor, used to send encoded messages to the remote
    /// actor.
    session: Address<Session>,
    /// Pre-built encode context reused for every outbound message through this address.
    encode_context: EncodeContext,
}

impl Debug for RemoteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RemoteAddress")
            .field(&self.session.index())
            .field(&self.remote_actor_id)
            .finish()
    }
}

impl Clone for RemoteAddress {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            remote_actor_id: self.remote_actor_id,
            session: self.session.clone(),
            encode_context: self.encode_context.clone(),
        }
    }
}

impl PartialEq for RemoteAddress {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.remote_actor_id == other.remote_actor_id
            && self.session.index() == other.session.index()
    }
}

impl Eq for RemoteAddress {}

impl Hash for RemoteAddress {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state)
    }
}

impl RemoteAddress {
    /// High bit of the 64-bit id space, reserved to tag a [`SenderIndex::index`] value as
    /// remote address.
    const REMOTE_FLAG: u64 = 1 << 63;

    /// Constructs a new [`RemoteAddress`] with the specified remote actor index and the address
    /// of the IPC connection session actor. The `registry` is used to construct the encode context for this address.
    ///
    /// The `index` is computed by reversing the bits of `session.index()` into bits
    /// 0..62 (small session ids occupy the high bits, growing downward) and XORing with
    /// `remote_actor_id` (small ids occupy the low bits, growing upward). Bit 63 is reserved
    /// for [`REMOTE_FLAG`][Self::REMOTE_FLAG].
    pub fn new(
        remote_actor_id: u64,
        session: Address<Session>,
        registry: RemoteActorRegistry,
    ) -> Self {
        let index = Self::REMOTE_FLAG | ((session.index().reverse_bits() >> 1) ^ remote_actor_id);
        Self {
            index,
            remote_actor_id,
            session,
            encode_context: EncodeContext::new(registry),
        }
    }

    fn decode_context(&self) -> DecodeContext {
        self.encode_context
            .create_decode_context(self.session.clone())
    }
}

async fn decode_and_forward<M>(
    raw_rx: oneshot::Receiver<Bytes>,
    tx: oneshot::Sender<M::Result>,
    decode_context: DecodeContext,
) where
    M: Message + Encode,
    M::Result: Decode,
{
    let decode_result = match raw_rx.await {
        Ok(bytes) => M::Result::decode(bytes, Some(&decode_context)),
        Err(e) => {
            let _ = tx.send_err(e);
            return;
        }
    };

    match decode_result {
        Ok(result) => {
            let _ = tx.send(result);
        }
        Err(e) => {
            let _ = tx.send_err(e);
        }
    }
}

impl SenderId for RemoteAddress {
    #[inline]
    fn index(&self) -> u64 {
        self.index
    }
}

impl<M> Sender<M> for RemoteAddress
where
    M: Message + Encode,
    M::Result: Decode,
{
    fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    fn capacity(&self) -> usize {
        self.session.capacity()
    }

    fn send(&self, msg: M) -> SendResultFuture<'_, M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        let (raw_tx, raw_rx) = oneshot::channel::<Bytes>();
        let (tx, rx) = oneshot::channel::<M::Result>();

        let decode_context = self.decode_context();

        tokio::spawn(
            async move {
                decode_and_forward::<M>(raw_rx, tx, decode_context).await;
            }
            .in_current_span(),
        );

        // this future will be resolved to Ok once the message is in the session actor's mailbox,
        // it does not guarantee the message arrives at the remote peer actor, the IPC
        // communication may fail
        self.session
            .do_send(RemoteMessage::send(
                self.remote_actor_id,
                message_bytes,
                raw_tx,
            ))
            // error return by the sender, which means the session actor is stopped before
            // receiving the message
            .map(|result| match result {
                Ok(_) => Ok(rx),
                Err(_) => Err(SendError::Closed(msg)),
            })
            .boxed()
    }

    fn do_send(&self, msg: M) -> DoSendResultFuture<'_, M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return future::ready(Err(SendError::other(e, msg))).boxed(),
        };

        // this future will be resolved to Ok once the message is in the session actor's mailbox,
        // it does not guarantee the message arrives at the remote peer actor, the IPC
        // communication may fail
        self.session
            .do_send(RemoteMessage::do_send(self.remote_actor_id, message_bytes))
            // error return by the sender, which means the session actor is stopped before
            // receiving the message
            .map_err(|_| SendError::Closed(msg))
            .boxed()
    }

    fn try_send(&self, msg: M) -> SendResult<M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        let (raw_tx, raw_rx) = oneshot::channel::<Bytes>();
        let (tx, rx) = oneshot::channel::<M::Result>();

        let decode_context = self.decode_context();

        tokio::spawn(
            async move {
                decode_and_forward::<M>(raw_rx, tx, decode_context).await;
            }
            .in_current_span(),
        );

        match self.session.try_do_send(RemoteMessage::send(
            self.remote_actor_id,
            message_bytes,
            raw_tx,
        )) {
            Ok(_) => Ok(rx),
            Err(e) => match e {
                SendError::Full(_) => Err(SendError::Full(msg)),
                _ => Err(SendError::Closed(msg)),
            },
        }
    }

    fn try_do_send(&self, msg: M) -> DoSendResult<M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        self.session
            .try_do_send(RemoteMessage::do_send(self.remote_actor_id, message_bytes))
            .map_err(|e| match e {
                SendError::Full(_) => SendError::Full(msg),
                _ => SendError::Closed(msg),
            })
    }

    fn blocking_send(&self, msg: M) -> SendResult<M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        let (raw_tx, raw_rx) = oneshot::channel::<Bytes>();
        let (tx, rx) = oneshot::channel::<M::Result>();

        let decode_context = self.decode_context();

        tokio::spawn(
            async move {
                decode_and_forward::<M>(raw_rx, tx, decode_context).await;
            }
            .in_current_span(),
        );

        match self.session.blocking_do_send(RemoteMessage::send(
            self.remote_actor_id,
            message_bytes,
            raw_tx,
        )) {
            Ok(_) => Ok(rx),
            Err(_) => Err(SendError::Closed(msg)),
        }
    }

    fn blocking_do_send(&self, msg: M) -> DoSendResult<M> {
        let message_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(e) => return Err(SendError::other(e, msg)),
        };

        self.session
            .blocking_do_send(RemoteMessage::do_send(self.remote_actor_id, message_bytes))
            .map_err(|_| SendError::Closed(msg))
    }
}

impl<M> From<RemoteAddress> for Recipient<M>
where
    M: Message + Encode,
    M::Result: Decode,
{
    fn from(address: RemoteAddress) -> Self {
        Recipient(Arc::new(address))
    }
}

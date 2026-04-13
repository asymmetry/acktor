use std::fmt::{self, Debug};
use std::future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use tokio::sync::{
    mpsc::error::{SendError, TrySendError},
    oneshot,
};
use tracing::{Instrument, warn};

use acktor::{Address, Message, Recipient, Sender, SenderId};

use crate::codec::{Decode, Encode, EncodeContext};
use crate::remote_actor::RemoteActorRegistry;
use crate::remote_message::RemoteMessage;
use crate::session::Session;

type SendResult<M, R> = Result<oneshot::Receiver<R>, SendError<M>>;
type TrySendResult<M, R> = Result<oneshot::Receiver<R>, TrySendError<M>>;

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
}

async fn wait_for_result<M>(
    result_bytes_rx: oneshot::Receiver<Bytes>,
    result_tx: oneshot::Sender<M::Result>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    M: Message + Encode,
    M::Result: Decode,
{
    let result_bytes = result_bytes_rx.await?;
    let result = M::Result::decode(result_bytes, None)?;
    result_tx.send(result).map_err(|_| "channel closed")?;
    Ok(())
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

    fn send(&self, msg: M) -> Pin<Box<dyn Future<Output = SendResult<M, M::Result>> + Send + '_>> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Box::pin(future::ready(Err(SendError(msg)))),
        };

        let (result_tx, result_rx) = oneshot::channel::<M::Result>();
        let (result_bytes_tx, result_bytes_rx) = oneshot::channel::<Bytes>();

        tokio::spawn(
            async move {
                if let Err(e) = wait_for_result::<M>(result_bytes_rx, result_tx).await {
                    warn!("Could not receive result from remote actor: {}", e)
                }
            }
            .in_current_span(),
        );

        self.session
            .do_send(RemoteMessage::send(
                self.remote_actor_id,
                msg_bytes,
                result_bytes_tx,
            ))
            .map(|result| match result {
                Ok(_) => Ok(result_rx),
                Err(_) => Err(SendError(msg)),
            })
            .boxed()
    }

    fn do_send(
        &self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), SendError<M>>> + Send + '_>> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Box::pin(future::ready(Err(SendError(msg)))),
        };

        self.session
            .do_send(RemoteMessage::do_send(self.remote_actor_id, msg_bytes))
            .map(|result| result.map_err(|_| SendError(msg)))
            .boxed()
    }

    fn try_send(&self, msg: M) -> TrySendResult<M, M::Result> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Err(TrySendError::Closed(msg)),
        };

        let (result_tx, result_rx) = oneshot::channel::<M::Result>();
        let (result_bytes_tx, result_bytes_rx) = oneshot::channel::<Bytes>();

        tokio::spawn(
            async move {
                if let Err(e) = wait_for_result::<M>(result_bytes_rx, result_tx).await {
                    warn!("Could not receive result from remote actor: {}", e)
                }
            }
            .in_current_span(),
        );

        match self.session.try_do_send(RemoteMessage::send(
            self.remote_actor_id,
            msg_bytes,
            result_bytes_tx,
        )) {
            Ok(_) => Ok(result_rx),
            Err(e) => match e {
                TrySendError::Closed(_) => Err(TrySendError::Closed(msg)),
                TrySendError::Full(_) => Err(TrySendError::Full(msg)),
            },
        }
    }

    fn try_do_send(&self, msg: M) -> Result<(), TrySendError<M>> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Err(TrySendError::Closed(msg)),
        };

        self.session
            .try_do_send(RemoteMessage::do_send(self.remote_actor_id, msg_bytes))
            .map_err(|e| match e {
                TrySendError::Closed(_) => TrySendError::Closed(msg),
                TrySendError::Full(_) => TrySendError::Full(msg),
            })
    }

    fn blocking_send(&self, msg: M) -> SendResult<M, M::Result> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Err(SendError(msg)),
        };

        let (result_tx, result_rx) = oneshot::channel::<M::Result>();
        let (result_bytes_tx, result_bytes_rx) = oneshot::channel::<Bytes>();

        tokio::spawn(
            async move {
                if let Err(e) = wait_for_result::<M>(result_bytes_rx, result_tx).await {
                    warn!("Could not receive result from remote actor: {}", e)
                }
            }
            .in_current_span(),
        );

        match self.session.blocking_do_send(RemoteMessage::send(
            self.remote_actor_id,
            msg_bytes,
            result_bytes_tx,
        )) {
            Ok(_) => Ok(result_rx),
            Err(_) => Err(SendError(msg)),
        }
    }

    fn blocking_do_send(&self, msg: M) -> Result<(), SendError<M>> {
        let msg_bytes = match msg.encode_to_bytes(Some(&self.encode_context)) {
            Ok(bytes) => bytes,
            Err(_) => return Err(SendError(msg)),
        };

        self.session
            .blocking_do_send(RemoteMessage::do_send(self.remote_actor_id, msg_bytes))
            .map_err(|_| SendError(msg))
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

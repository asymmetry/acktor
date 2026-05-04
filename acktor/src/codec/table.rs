use std::any::Any;
use std::ops::Deref;

use bytes::Bytes;

use super::{DecodeContext, DecodeError, EncodeContext, EncodeError};
use crate::utils::TypeMap;

/// Function for encoding a message to bytes.
pub type EncodeMsgFn = fn(&dyn Any, Option<&dyn EncodeContext>) -> Result<Bytes, EncodeError>;

/// Function for decoding bytes to a message response.
pub type DecodeResFn = fn(Bytes, Option<&dyn DecodeContext>) -> Result<Box<dyn Any>, DecodeError>;

/// A codec for a specific message type, containing the message id
/// ([`MessageId::ID`][crate::message::MessageId::ID]), an [`EncodeMsgFn`] for encoding the
/// message to bytes, and a [`DecodeResFn`] for decoding the message response bytes back to the
/// concrete message response type.
#[derive(Clone, Copy)]
pub struct MessageCodec {
    pub message_id: u64,
    pub encode_msg: EncodeMsgFn,
    pub decode_res: DecodeResFn,
}

/// A table mapping the [`TypeId`][std::any::TypeId] of messages to their corresponding codecs.
pub struct CodecTable(TypeMap<MessageCodec>);

impl CodecTable {
    pub fn new(map: TypeMap<MessageCodec>) -> Self {
        Self(map)
    }
}

impl Deref for CodecTable {
    type Target = TypeMap<MessageCodec>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A codec table for encoding messages and decoding message responses.
///
/// Each entry in the codec table corresponds to a message type the actor can receive from
/// another actor remotely. It records the message id, an [`EncodeMsgFn`] for encoding the message
/// to bytes before sending, and a [`DecodeResFn`] for decoding the message response bytes back
/// to the concrete message response type.
///
/// For example, if actor `B` needs to send a message to actor `A` with `A`'s `Address<A>`, `B`
/// needs to use `A`'s codec table. Since a copy of the codec table is stored in `Address<A>`,
/// `B` has access to it automatically if it has `A`'s address.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(RemoteAddressable)]`][acktor_derive::RemoteAddressable], which will emit an
/// implementation of this trait for a remote addressable actor.
pub trait Codec {
    fn codec_table() -> &'static CodecTable;
}

use std::any::Any;
use std::ops::Deref;

use bytes::Bytes;

use super::{DecodeContext, DecodeError, EncodeContext, EncodeError};
use crate::actor::Actor;
use crate::utils::TypeMap;

/// Function for encoding a message to bytes.
pub type EncodeMsgFn = fn(&dyn Any, Option<&dyn EncodeContext>) -> Result<Bytes, EncodeError>;
/// Function for decoding bytes to a message response.
pub type DecodeResFn = fn(Bytes, Option<&dyn DecodeContext>) -> Result<Box<dyn Any>, DecodeError>;

/// A codec for a specific message type, containing the message id and the encode/decode
/// functions.
#[derive(Clone, Copy)]
pub struct CodecItem {
    pub message_id: u64,
    pub encode_msg: EncodeMsgFn,
    pub decode_res: DecodeResFn,
}

/// A table mapping the [`TypeId`][std::any::TypeId] of messages to their corresponding codecs.
pub struct CodecTable(TypeMap<CodecItem>);

impl Deref for CodecTable {
    type Target = TypeMap<CodecItem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A trait for actors which has a static codec table.
pub trait HasCodecTable: Actor {
    fn codec_table() -> &'static CodecTable;
}

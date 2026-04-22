use bytes::{Bytes, BytesMut};

use acktor_ipc_proto::ipc_message::IpcMessage;

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};

impl Encode for IpcMessage {
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(self)
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, _ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        prost::Message::encode(self, buf).map_err(Into::into)
    }
}

impl Decode for IpcMessage {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        prost::Message::decode(buf).map_err(Into::into)
    }
}

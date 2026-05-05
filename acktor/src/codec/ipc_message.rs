use bytes::{Bytes, BytesMut};

use acktor_ipc_proto::message::IpcMessage;

use super::error::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};

impl Encode for IpcMessage {
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(self)
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        prost::Message::encode(self, buf).map_err(Into::into)
    }
}

impl Decode for IpcMessage {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        prost::Message::decode(buf).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use acktor_ipc_proto::message::ActorMessage;

    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_ipc_message() -> anyhow::Result<()> {
        let value =
            IpcMessage::actor_message(ActorMessage::send(1, 2, Bytes::from_static(b"hello"), 3));

        let buf = value.encode_to_bytes(None)?;
        let decoded = IpcMessage::decode(buf, None)?;
        assert_eq!(value, decoded);

        Ok(())
    }
}

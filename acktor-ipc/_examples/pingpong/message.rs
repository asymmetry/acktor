use acktor::Message;
use acktor_ipc::bytes::Bytes;
use acktor_ipc::{Decode, DecodeContext, DecodeError, Encode, EncodeError};

#[derive(Debug, Clone, Message)]
#[result_type(())]
pub struct Ping;

#[derive(Debug, Clone, Message)]
#[result_type(())]
pub struct Pong;

impl Encode for Ping {
    fn buffer_size(&self) -> usize {
        1
    }

    fn encode<B>(&self, mut buf: B) -> Result<usize, EncodeError>
    where
        B: AsMut<[u8]>,
    {
        let buf = buf.as_mut();
        if buf.is_empty() {
            return Err(EncodeError::custom("buffer too small"));
        }
        buf[0] = 0;

        Ok(1)
    }
}

impl Decode for Ping {
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Err("buffer too small".into());
        }
        if buf[0] != 0 {
            return Err("unknown message type".into());
        }

        Ok(Self)
    }
}

impl Encode for Pong {
    fn buffer_size(&self) -> usize {
        1
    }

    fn encode<B>(&self, mut buf: B) -> Result<usize, EncodeError>
    where
        B: AsMut<[u8]>,
    {
        let buf = buf.as_mut();
        if buf.is_empty() {
            return Err(EncodeError::custom("buffer too small"));
        }
        buf[0] = 1;

        Ok(1)
    }
}

impl Decode for Pong {
    fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Err("buffer too small".into());
        }
        if buf[0] != 1 {
            return Err("unknown message type".into());
        }
        Ok(Self)
    }
}

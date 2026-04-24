use std::fmt::Display;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message as _;

use acktor_ipc_proto::utils::{ProtoOption, ProtoResult, ProtoResultType};

use super::errors::{DecodeError, EncodeError};
use super::protobuf_helper::LENGTH_DELIMITED_TAGS;
use super::{Decode, DecodeContext, Encode, EncodeContext};

impl<T> Encode for Box<T>
where
    T: Encode,
{
    const ID: u64 = T::ID;

    fn encoded_len(&self) -> usize {
        self.as_ref().encoded_len()
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        self.as_ref().encode(buf, ctx)
    }
}

impl<T> Decode for Box<T>
where
    T: Decode,
{
    const ID: u64 = T::ID;

    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        T::decode(buf, ctx).map(Box::new)
    }
}

impl<T> Encode for Arc<T>
where
    T: Encode,
{
    const ID: u64 = T::ID;

    fn encoded_len(&self) -> usize {
        self.as_ref().encoded_len()
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        self.as_ref().encode(buf, ctx)
    }
}

impl<T> Decode for Arc<T>
where
    T: Decode,
{
    const ID: u64 = T::ID;

    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        T::decode(buf, ctx).map(Arc::new)
    }
}

// Encode a `Result<T, E>`.
//
// The `Err` variant is lossy: the error is serialized as its `Display` string and decoded back
// via `E::from(String)` (see the `Decode` impl below). Any structured data on the error type —
// enum discriminants, numeric codes, nested fields — is collapsed to the rendered message.
impl<T, E> Encode for Result<T, E>
where
    T: Encode,
    E: Display,
{
    fn encoded_len(&self) -> usize {
        let inner_len = match self {
            Ok(ok) => ok.encoded_len(),
            Err(err) => err.to_string().len(),
        };
        // oneof field: 1 byte tag + varint length + data
        1 + prost::length_delimiter_len(inner_len) + inner_len
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        match self {
            Ok(ok) => {
                // field 1, wire type LengthDelimited (bytes)
                buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[1]]);
                prost::encoding::encode_varint(ok.encoded_len() as u64, buf);
                ok.encode(buf, ctx)?;
            }
            Err(err) => {
                // field 2, wire type LengthDelimited (string)
                let err_str = err.to_string();
                buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[2]]);
                prost::encoding::encode_varint(err_str.len() as u64, buf);
                buf.extend_from_slice(err_str.as_bytes());
            }
        }

        Ok(())
    }

    fn encode_to_bytes(&self, ctx: Option<&EncodeContext>) -> Result<Bytes, EncodeError> {
        match self {
            Ok(ok) => {
                let inner_len = ok.encoded_len();
                let total = 1 + prost::length_delimiter_len(inner_len) + inner_len;
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(inner_len as u64, &mut buf);
                ok.encode(&mut buf, ctx)?;

                Ok(buf.freeze())
            }
            Err(err) => {
                let err_string = err.to_string();
                let total = 1 + prost::length_delimiter_len(err_string.len()) + err_string.len();
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[2]]);
                prost::encoding::encode_varint(err_string.len() as u64, &mut buf);
                buf.extend_from_slice(err_string.as_bytes());

                Ok(buf.freeze())
            }
        }
    }
}

impl<T, E> Decode for Result<T, E>
where
    T: Decode,
    E: From<String>,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let result = ProtoResult::decode(buf)?;
        match result.result {
            Some(ProtoResultType::Ok(ok)) => Ok(Ok(T::decode(ok, ctx)?)),
            Some(ProtoResultType::Err(err)) => Ok(Err(E::from(err))),
            _ => Err("missing field `result` in the `Result` message".into()),
        }
    }
}

impl<T> Encode for Option<T>
where
    T: Encode,
{
    fn encoded_len(&self) -> usize {
        match self {
            // bytes field: 1 byte tag + varint length + data
            Some(some) => {
                let inner_len = some.encoded_len();
                1 + prost::length_delimiter_len(inner_len) + inner_len
            }
            // empty message
            None => 0,
        }
    }

    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        if let Some(some) = self {
            // field 1, wire type LengthDelimited (bytes)
            buf.extend_from_slice(&[0x0A]);
            prost::encoding::encode_varint(some.encoded_len() as u64, buf);
            some.encode(buf, ctx)?;
        }

        Ok(())
    }

    fn encode_to_bytes(&self, ctx: Option<&EncodeContext>) -> Result<Bytes, EncodeError> {
        match self {
            Some(some) => {
                let inner_len = some.encoded_len();
                let total = 1 + prost::length_delimiter_len(inner_len) + inner_len;
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(inner_len as u64, &mut buf);
                some.encode(&mut buf, ctx)?;

                Ok(buf.freeze())
            }
            None => Ok(Bytes::new()),
        }
    }
}

impl<T> Decode for Option<T>
where
    T: Decode,
{
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let option = ProtoOption::decode(buf)?;
        match option.option {
            Some(bytes) => Ok(Some(T::decode(bytes, ctx)?)),
            None => Ok(None),
        }
    }
}

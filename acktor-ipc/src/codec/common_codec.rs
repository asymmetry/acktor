use std::fmt::Display;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use acktor_ipc_proto::utils::{OptionMessage, ResultMessage, ResultType};

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode};

impl<T> Encode for Box<T>
where
    T: Encode,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        self.as_ref().encoded_len()
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.as_ref().encode(buf)
    }
}

impl<T> Decode for Box<T>
where
    T: Decode,
{
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        T::decode(buf, context).map(Box::new)
    }
}

impl<T> Encode for Arc<T>
where
    T: Encode,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        self.as_ref().encoded_len()
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.as_ref().encode(buf)
    }
}

impl<T> Decode for Arc<T>
where
    T: Decode,
{
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        T::decode(buf, context).map(Arc::new)
    }
}

impl<T, E> Encode for Result<T, E>
where
    T: Encode,
    E: Display,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        let inner_len = match self {
            Ok(ok) => ok.encoded_len(),
            Err(err) => err.to_string().len(),
        };
        // oneof field: 1 byte tag + varint length + data
        1 + prost::length_delimiter_len(inner_len) + inner_len
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        match self {
            Ok(ok) => {
                // field 1, wire type LengthDelimited (bytes)
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(ok.encoded_len() as u64, buf);
                ok.encode(buf)?;
            }
            Err(err) => {
                // field 2, wire type LengthDelimited (string)
                let err_str = err.to_string();
                buf.extend_from_slice(&[0x12]);
                prost::encoding::encode_varint(err_str.len() as u64, buf);
                buf.extend_from_slice(err_str.as_bytes());
            }
        }

        Ok(())
    }

    fn encode_to_bytes(&self) -> Result<Bytes, EncodeError> {
        match self {
            Ok(ok) => {
                let inner_len = ok.encoded_len();
                let total = 1 + prost::length_delimiter_len(inner_len) + inner_len;
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(inner_len as u64, &mut buf);
                ok.encode(&mut buf)?;

                Ok(buf.freeze())
            }
            Err(err) => {
                let err_string = err.to_string();
                let total = 1 + prost::length_delimiter_len(err_string.len()) + err_string.len();
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[0x12]);
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
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let result = <ResultMessage as prost::Message>::decode(buf)?;
        match result.result {
            Some(ResultType::Ok(ok)) => Ok(Ok(T::decode(ok, context)?)),
            Some(ResultType::Err(err)) => Ok(Err(E::from(err))),
            _ => Err("missing field `result` in the `Result` message".into()),
        }
    }
}

impl<T> Encode for Option<T>
where
    T: Encode,
{
    #[inline]
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

    #[inline]
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        if let Some(some) = self {
            // field 1, wire type LengthDelimited (bytes)
            buf.extend_from_slice(&[0x0A]);
            prost::encoding::encode_varint(some.encoded_len() as u64, buf);
            some.encode(buf)?;
        }

        Ok(())
    }

    fn encode_to_bytes(&self) -> Result<Bytes, EncodeError> {
        match self {
            Some(some) => {
                let inner_len = some.encoded_len();
                let total = 1 + prost::length_delimiter_len(inner_len) + inner_len;
                let mut buf = BytesMut::with_capacity(total);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(inner_len as u64, &mut buf);
                some.encode(&mut buf)?;

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
    #[inline]
    fn decode(buf: Bytes, context: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let option = <OptionMessage as prost::Message>::decode(buf)?;
        match option.option {
            Some(bytes) => Ok(Some(T::decode(bytes, context)?)),
            None => Ok(None),
        }
    }
}

use bytes::{Bytes, BytesMut};

use acktor_ipc_proto::utils::{
    Tuple, VecBool, VecDouble, VecFloat, VecInt32, VecInt64, VecUint32, VecUint64,
};

use super::errors::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};

macro_rules! impl_encode_decode_for {
    ($type:ty) => {
        impl Encode for $type {
            #[inline]
            fn encoded_len(&self) -> usize {
                prost::Message::encoded_len(self)
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                prost::Message::encode(self, buf).map_err(Into::into)
            }
        }

        impl Decode for $type {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                prost::Message::decode(buf).map_err(Into::into)
            }
        }
    };
    ($type:ty, $wire_type:ty) => {
        impl Encode for $type {
            #[inline]
            fn encoded_len(&self) -> usize {
                prost::Message::encoded_len(&(*self as $wire_type))
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                prost::Message::encode(&(*self as $wire_type), buf).map_err(Into::into)
            }
        }

        impl Decode for $type {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                let value = <$wire_type as prost::Message>::decode(buf)?;
                <$type>::try_from(value).map_err(|e| e.to_string().into())
            }
        }
    };
}

impl_encode_decode_for!(());
impl_encode_decode_for!(bool);
impl_encode_decode_for!(i8, i32);
impl_encode_decode_for!(i16, i32);
impl_encode_decode_for!(i32);
impl_encode_decode_for!(i64);
impl_encode_decode_for!(u8, u32);
impl_encode_decode_for!(u16, u32);
impl_encode_decode_for!(u32);
impl_encode_decode_for!(u64);
impl_encode_decode_for!(f32);
impl_encode_decode_for!(f64);
impl_encode_decode_for!(isize, i64);
impl_encode_decode_for!(usize, u64);
impl_encode_decode_for!(String);
impl_encode_decode_for!(Vec<u8>);

macro_rules! impl_encode_decode_for_vec {
    (varint, $type:ty, $msg:ident) => {
        impl Encode for Vec<$type> {
            #[inline]
            fn encoded_len(&self) -> usize {
                if self.is_empty() {
                    return 0;
                }
                let data_len: usize = self
                    .iter()
                    .map(|&v| prost::encoding::encoded_len_varint(v as u64))
                    .sum();
                1 + prost::length_delimiter_len(data_len) + data_len
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                if self.is_empty() {
                    return Ok(());
                }

                let data_len: usize = self
                    .iter()
                    .map(|&v| prost::encoding::encoded_len_varint(v as u64))
                    .sum();
                buf.reserve(1 + prost::length_delimiter_len(data_len) + data_len);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(data_len as u64, buf);
                for value in self.iter() {
                    prost::encoding::encode_varint(*value as u64, buf);
                }

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                let message = <$msg as prost::Message>::decode(buf)?;

                Ok(message.values)
            }
        }
    };
    (varint, $type:ty, $wire_type:ty, $msg:ident) => {
        impl Encode for Vec<$type> {
            #[inline]
            fn encoded_len(&self) -> usize {
                if self.is_empty() {
                    return 0;
                }
                let data_len: usize = self
                    .iter()
                    .map(|&v| prost::encoding::encoded_len_varint(v as $wire_type as u64))
                    .sum();
                1 + prost::length_delimiter_len(data_len) + data_len
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                if self.is_empty() {
                    return Ok(());
                }

                let data_len: usize = self
                    .iter()
                    .map(|&v| prost::encoding::encoded_len_varint(v as $wire_type as u64))
                    .sum();
                buf.reserve(1 + prost::length_delimiter_len(data_len) + data_len);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(data_len as u64, buf);
                for value in self.iter() {
                    prost::encoding::encode_varint(*value as $wire_type as u64, buf);
                }

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                let message = <$msg as prost::Message>::decode(buf)?;
                message
                    .values
                    .into_iter()
                    .map(|v| <$type>::try_from(v).map_err(|e| e.to_string().into()))
                    .collect()
            }
        }
    };
    (fixed, $type:ty, $msg:ident) => {
        impl Encode for Vec<$type> {
            #[inline]
            fn encoded_len(&self) -> usize {
                if self.is_empty() {
                    return 0;
                }
                let data_len = self.len() * std::mem::size_of::<$type>();
                1 + prost::length_delimiter_len(data_len) + data_len
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                if self.is_empty() {
                    return Ok(());
                }

                let data_len = self.len() * std::mem::size_of::<$type>();
                buf.reserve(1 + prost::length_delimiter_len(data_len) + data_len);
                buf.extend_from_slice(&[0x0A]);
                prost::encoding::encode_varint(data_len as u64, buf);
                for value in self.iter() {
                    buf.extend_from_slice(&value.to_le_bytes());
                }

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                let message = <$msg as prost::Message>::decode(buf)?;

                Ok(message.values)
            }
        }
    };
}

impl_encode_decode_for_vec!(varint, bool, VecBool);
impl_encode_decode_for_vec!(varint, i8, i32, VecInt32);
impl_encode_decode_for_vec!(varint, i16, i32, VecInt32);
impl_encode_decode_for_vec!(varint, i32, VecInt32);
impl_encode_decode_for_vec!(varint, i64, VecInt64);
// `Vec<u8>` is handled above by `impl_encode_decode_for!(Vec<u8>)` via prost's bytes field.
impl_encode_decode_for_vec!(varint, u16, u32, VecUint32);
impl_encode_decode_for_vec!(varint, u32, VecUint32);
impl_encode_decode_for_vec!(varint, u64, VecUint64);
impl_encode_decode_for_vec!(fixed, f32, VecFloat);
impl_encode_decode_for_vec!(fixed, f64, VecDouble);
impl_encode_decode_for_vec!(varint, isize, i64, VecInt64);
impl_encode_decode_for_vec!(varint, usize, u64, VecUint64);

macro_rules! impl_encode_decode_for_tuple {
    // $tag = precomputed tag byte: (field_number << 3) | 2
    ([$($type:ident $field:ident $index:tt $tag:expr),+]) => {
        impl<$($type,)+> Encode for ($($type,)+)
        where
            $($type: Encode,)+
        {
            #[inline]
            fn encoded_len(&self) -> usize {
                let mut len = 0;
                // 1 byte tag + varint length + data
                $({
                    let element_len = self.$index.encoded_len();
                    len += 1 + prost::length_delimiter_len(element_len) + element_len;
                })+
                len
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                ctx: Option<&EncodeContext>,
            ) -> Result<(), EncodeError> {
                buf.reserve(self.encoded_len());
                $({
                    buf.extend_from_slice(&[$tag]);
                    prost::encoding::encode_varint(self.$index.encoded_len() as u64, buf);
                    self.$index.encode(buf, ctx)?;
                })+

                Ok(())
            }
        }

        impl<$($type,)+> Decode for ($($type,)+)
        where
            $($type: Decode,)+
        {
            #[inline]
            fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
                let message = <Tuple as prost::Message>::decode(buf)?;

                Ok((
                    $($type::decode(
                        message.$field.ok_or(DecodeError::from(
                            concat!("missing field ", stringify!($field), " in tuple message")
                        ))?,
                        ctx,
                    )?,)+
                ))
            }
        }
    };
}

impl_encode_decode_for_tuple!([T0 t0 0 0x0A, T1 t1 1 0x12]);
impl_encode_decode_for_tuple!([T0 t0 0 0x0A, T1 t1 1 0x12, T2 t2 2 0x1A]);
impl_encode_decode_for_tuple!([T0 t0 0 0x0A, T1 t1 1 0x12, T2 t2 2 0x1A, T3 t3 3 0x22]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A, T1 t1 1 0x12, T2 t2 2 0x1A, T3 t3 3 0x22, T4 t4 4 0x2A
]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A, T1 t1 1 0x12, T2 t2 2 0x1A, T3 t3 3 0x22, T4 t4 4 0x2A, T5 t5 5 0x32
]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A,
    T1 t1 1 0x12,
    T2 t2 2 0x1A,
    T3 t3 3 0x22,
    T4 t4 4 0x2A,
    T5 t5 5 0x32,
    T6 t6 6 0x3A
]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A,
    T1 t1 1 0x12,
    T2 t2 2 0x1A,
    T3 t3 3 0x22,
    T4 t4 4 0x2A,
    T5 t5 5 0x32,
    T6 t6 6 0x3A,
    T7 t7 7 0x42
]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A,
    T1 t1 1 0x12,
    T2 t2 2 0x1A,
    T3 t3 3 0x22,
    T4 t4 4 0x2A,
    T5 t5 5 0x32,
    T6 t6 6 0x3A,
    T7 t7 7 0x42,
    T8 t8 8 0x4A
]);
impl_encode_decode_for_tuple!([
    T0 t0 0 0x0A,
    T1 t1 1 0x12,
    T2 t2 2 0x1A,
    T3 t3 3 0x22,
    T4 t4 4 0x2A,
    T5 t5 5 0x32,
    T6 t6 6 0x3A,
    T7 t7 7 0x42,
    T8 t8 8 0x4A,
    T9 t9 9 0x52
]);

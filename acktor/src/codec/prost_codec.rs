use bytes::{Bytes, BytesMut};

use acktor_ipc_proto::{
    optional_length_delimited_field_encoded_len,
    utils::{Tuple, VecBool, VecDouble, VecFloat, VecInt32, VecInt64, VecUint32, VecUint64},
};

use super::error::{DecodeError, EncodeError};
use super::protobuf_helper::LENGTH_DELIMITED_TAGS;
use super::{Decode, DecodeContext, Encode, EncodeContext};

#[inline]
fn zigzag32(n: i32) -> u64 {
    (((n << 1) ^ (n >> 31)) as u32) as u64
}

#[inline]
fn zigzag64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

#[inline]
fn encoded_len_packed_varint<T: Copy>(values: &[T], to_varint: impl Fn(T) -> u64) -> usize {
    if values.is_empty() {
        return 0;
    }
    let data_len: usize = values
        .iter()
        .map(|&v| prost::encoding::encoded_len_varint(to_varint(v)))
        .sum();
    optional_length_delimited_field_encoded_len(1, data_len)
}

#[inline]
fn encode_packed_varint<T: Copy>(values: &[T], buf: &mut BytesMut, to_varint: impl Fn(T) -> u64) {
    if values.is_empty() {
        return;
    }
    let data_len: usize = values
        .iter()
        .map(|&v| prost::encoding::encoded_len_varint(to_varint(v)))
        .sum();
    buf.reserve(optional_length_delimited_field_encoded_len(1, data_len));
    buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[1]]);
    prost::encoding::encode_varint(data_len as u64, buf);
    for &value in values.iter() {
        prost::encoding::encode_varint(to_varint(value), buf);
    }
}

#[inline]
fn encoded_len_packed_fixed<T>(values: &[T]) -> usize {
    if values.is_empty() {
        return 0;
    }
    let data_len = std::mem::size_of_val(values);
    optional_length_delimited_field_encoded_len(1, data_len)
}

#[inline]
fn encode_packed_fixed<T: Copy>(
    values: &[T],
    buf: &mut BytesMut,
    write: impl Fn(T, &mut BytesMut),
) {
    if values.is_empty() {
        return;
    }
    let data_len = std::mem::size_of_val(values);
    buf.reserve(optional_length_delimited_field_encoded_len(1, data_len));
    buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[1]]);
    prost::encoding::encode_varint(data_len as u64, buf);
    for &value in values.iter() {
        write(value, buf);
    }
}

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
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                prost::Message::encode(self, buf).map_err(Into::into)
            }
        }

        impl Decode for $type {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
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
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                prost::Message::encode(&(*self as $wire_type), buf).map_err(Into::into)
            }
        }

        impl Decode for $type {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
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
    (varint, $type:ty, $msg:ident, |$v:ident| $to_varint:expr) => {
        impl Encode for Vec<$type> {
            #[inline]
            fn encoded_len(&self) -> usize {
                encoded_len_packed_varint::<$type>(self, |$v| $to_varint)
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                encode_packed_varint::<$type>(self, buf, |$v| $to_varint);

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
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
                encoded_len_packed_fixed::<$type>(self)
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                encode_packed_fixed::<$type>(self, buf, |v, b| {
                    b.extend_from_slice(&v.to_le_bytes())
                });

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
                let message = <$msg as prost::Message>::decode(buf)?;

                Ok(message.values)
            }
        }
    };
}

impl_encode_decode_for_vec!(varint, bool, VecBool, |v| v as u64);
impl_encode_decode_for_vec!(varint, i8, VecInt32, |v| zigzag32(v as i32));
impl_encode_decode_for_vec!(varint, i16, VecInt32, |v| zigzag32(v as i32));
impl_encode_decode_for_vec!(varint, i32, VecInt32, |v| zigzag32(v));
impl_encode_decode_for_vec!(varint, i64, VecInt64, |v| zigzag64(v));
// `Vec<u8>` is handled above by `impl_encode_decode_for!(Vec<u8>)` via prost's bytes field.
impl_encode_decode_for_vec!(varint, u16, VecUint32, |v| v as u64);
impl_encode_decode_for_vec!(varint, u32, VecUint32, |v| v as u64);
impl_encode_decode_for_vec!(varint, u64, VecUint64, |v| v);
impl_encode_decode_for_vec!(varint, isize, VecInt64, |v| zigzag64(v as i64));
impl_encode_decode_for_vec!(varint, usize, VecUint64, |v| v as u64);
impl_encode_decode_for_vec!(fixed, f32, VecFloat);
impl_encode_decode_for_vec!(fixed, f64, VecDouble);

macro_rules! impl_encode_decode_for_tuple {
    ($($type:ident<$index:tt>($field:ident)),+) => {
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
                ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                buf.reserve(self.encoded_len());
                $({
                    buf.extend_from_slice(&[LENGTH_DELIMITED_TAGS[$index + 1]]);
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
            fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
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

impl_encode_decode_for_tuple!(T0<0>(t0), T1<1>(t1));
impl_encode_decode_for_tuple!(T0<0>(t0), T1<1>(t1), T2<2>(t2));
impl_encode_decode_for_tuple!(T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3));
impl_encode_decode_for_tuple!(T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4));
impl_encode_decode_for_tuple!(
    T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4), T5<5>(t5)
);
impl_encode_decode_for_tuple!(
    T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4), T5<5>(t5), T6<6>(t6)
);
impl_encode_decode_for_tuple!(
    T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4), T5<5>(t5), T6<6>(t6), T7<7>(t7)
);
impl_encode_decode_for_tuple!(
    T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4), T5<5>(t5), T6<6>(t6), T7<7>(t7),
    T8<8>(t8)
);
impl_encode_decode_for_tuple!(
    T0<0>(t0), T1<1>(t1), T2<2>(t2), T3<3>(t3), T4<4>(t4), T5<5>(t5), T6<6>(t6), T7<7>(t7),
    T8<8>(t8), T9<9>(t9)
);

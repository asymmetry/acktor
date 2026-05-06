use std::mem;

use bytes::{Bytes, BytesMut};
use zerocopy::{FromBytes, IntoBytes, Ref};

use super::error::{DecodeError, EncodeError};
use super::{Decode, DecodeContext, Encode, EncodeContext};

#[cfg(target_endian = "big")]
compile_error!(
    "default codec does not support big-endian targets, enable prost-codec feature instead"
);

macro_rules! impl_encode_decode_for {
    ($type:ty) => {
        impl Encode for $type {
            #[inline]
            fn encoded_len(&self) -> usize {
                mem::size_of::<$type>()
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                buf.extend_from_slice(self.as_bytes());

                Ok(())
            }
        }

        impl Decode for $type {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
                Self::read_from_bytes(&buf).map_err(|e| e.to_string().into())
            }
        }
    };
}

impl Encode for () {
    #[inline]
    fn encoded_len(&self) -> usize {
        0
    }

    #[inline]
    fn encode(
        &self,
        _buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl Decode for () {
    #[inline]
    fn decode(_buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        Ok(())
    }
}

impl Encode for bool {
    #[inline]
    fn encoded_len(&self) -> usize {
        1
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }
}

impl Decode for bool {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        Ok(u8::read_from_bytes(&buf).map_err(|e| e.to_string())? != 0)
    }
}

impl_encode_decode_for!(i8);
impl_encode_decode_for!(i16);
impl_encode_decode_for!(i32);
impl_encode_decode_for!(i64);
impl_encode_decode_for!(u8);
impl_encode_decode_for!(u16);
impl_encode_decode_for!(u32);
impl_encode_decode_for!(u64);
impl_encode_decode_for!(f32);
impl_encode_decode_for!(f64);

// `isize` / `usize` are encoded as fixed-width i64 / u64 on the wire so peers with
// different `target_pointer_width` can interoperate.

impl Encode for isize {
    #[inline]
    fn encoded_len(&self) -> usize {
        mem::size_of::<i64>()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        (*self as i64).encode(buf, ctx)
    }
}

impl Decode for isize {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let value = i64::decode(buf, None)?;
        isize::try_from(value).map_err(|e| e.to_string().into())
    }
}

impl Encode for usize {
    #[inline]
    fn encoded_len(&self) -> usize {
        mem::size_of::<u64>()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        (*self as u64).encode(buf, ctx)
    }
}

impl Decode for usize {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let value = u64::decode(buf, None)?;
        usize::try_from(value).map_err(|e| e.to_string().into())
    }
}

impl Encode for String {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }
}

impl Decode for String {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        String::from_utf8(buf.to_vec()).map_err(|e| e.to_string().into())
    }
}

macro_rules! impl_encode_decode_for_vec {
    ($type:ty) => {
        impl Encode for Vec<$type> {
            #[inline]
            fn encoded_len(&self) -> usize {
                self.len() * mem::size_of::<$type>()
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                _ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                buf.extend_from_slice(self.as_slice().as_bytes());

                Ok(())
            }
        }

        impl Decode for Vec<$type> {
            #[inline]
            fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
                if buf.is_empty() {
                    return Ok(Vec::new());
                }

                Ok(Ref::<_, [$type]>::from_bytes(buf.as_ref())
                    .map_err(|e| e.to_string())?
                    .to_vec())
            }
        }
    };
}

impl Encode for Vec<bool> {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(self.as_slice().as_bytes());

        Ok(())
    }
}

impl Decode for Vec<bool> {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Ok(Vec::new());
        }

        Ok(buf.iter().map(|&b| b != 0).collect())
    }
}

impl_encode_decode_for_vec!(i8);
impl_encode_decode_for_vec!(i16);
impl_encode_decode_for_vec!(i32);
impl_encode_decode_for_vec!(i64);
impl_encode_decode_for_vec!(u8);
impl_encode_decode_for_vec!(u16);
impl_encode_decode_for_vec!(u32);
impl_encode_decode_for_vec!(u64);
impl_encode_decode_for_vec!(f32);
impl_encode_decode_for_vec!(f64);

impl Encode for Vec<isize> {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len() * mem::size_of::<i64>()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        let vec: Vec<i64> = self.iter().map(|&v| v as i64).collect();
        buf.extend_from_slice(vec.as_slice().as_bytes());

        Ok(())
    }
}

impl Decode for Vec<isize> {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Ok(Vec::new());
        }

        let vec: Vec<i64> = Ref::<_, [i64]>::from_bytes(buf.as_ref())
            .map_err(|e| e.to_string())?
            .to_vec();
        vec.into_iter()
            .map(|v| isize::try_from(v).map_err(DecodeError::other))
            .collect()
    }
}

impl Encode for Vec<usize> {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len() * mem::size_of::<u64>()
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        let vec: Vec<u64> = self.iter().map(|&v| v as u64).collect();
        buf.extend_from_slice(vec.as_slice().as_bytes());

        Ok(())
    }
}

impl Decode for Vec<usize> {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        if buf.is_empty() {
            return Ok(Vec::new());
        }

        let vec: Vec<u64> = Ref::<_, [u64]>::from_bytes(buf.as_ref())
            .map_err(|e| e.to_string())?
            .to_vec();
        vec.into_iter()
            .map(|v| usize::try_from(v).map_err(DecodeError::other))
            .collect()
    }
}

fn encode_element<T>(
    value: &T,
    buf: &mut BytesMut,
    ctx: Option<&dyn EncodeContext>,
) -> Result<(), EncodeError>
where
    T: Encode,
{
    let len = value.encoded_len() as u32;
    buf.extend_from_slice(len.as_bytes());
    value.encode(buf, ctx)?;

    Ok(())
}

#[inline]
fn decode_element(buf: &mut Bytes) -> Result<Bytes, DecodeError> {
    if buf.len() < mem::size_of::<u32>() {
        return Err("missing the tuple element length".into());
    }
    let len = u32::read_from_bytes(&buf.split_to(mem::size_of::<u32>()))
        .map_err(|e| e.to_string())? as usize;
    if buf.len() < len {
        return Err("missing the tuple element data".into());
    }

    Ok(buf.split_to(len))
}

macro_rules! impl_encode_decode_for_tuple {
    ($($type:ident<$index:tt>),+) => {
        impl<$($type),+> Encode for ($($type,)+)
        where
            $($type: Encode,)+
        {
            #[inline]
            fn encoded_len(&self) -> usize {
                0 $(+ mem::size_of::<u32>() + self.$index.encoded_len())+
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut BytesMut,
                ctx: Option<&dyn EncodeContext>,
            ) -> Result<(), EncodeError> {
                $(
                    encode_element(&self.$index, buf, ctx)?;
                )+

                Ok(())
            }
        }

        impl<$($type),+> Decode for ($($type,)+)
        where
            $($type: Decode,)+
        {
            #[inline]
            fn decode(mut buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
                Ok((
                    $(
                        <$type as Decode>::decode(decode_element(&mut buf)?, ctx)?,
                    )+
                ))
            }
        }
    };
}

impl_encode_decode_for_tuple!(T0<0>, T1<1>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>, T3<3>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>, T3<3>, T4<4>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>, T3<3>, T4<4>, T5<5>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>, T3<3>, T4<4>, T5<5>, T6<6>);
impl_encode_decode_for_tuple!(T0<0>, T1<1>, T2<2>, T3<3>, T4<4>, T5<5>, T6<6>, T7<7>);
impl_encode_decode_for_tuple!(
    T0<0>,
    T1<1>,
    T2<2>,
    T3<3>,
    T4<4>,
    T5<5>,
    T6<6>,
    T7<7>,
    T8<8>
);
impl_encode_decode_for_tuple!(
    T0<0>,
    T1<1>,
    T2<2>,
    T3<3>,
    T4<4>,
    T5<5>,
    T6<6>,
    T7<7>,
    T8<8>,
    T9<9>
);

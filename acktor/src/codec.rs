//! Codec traits for encoding and decoding remote messages.
//!
//! This module provides the [`Encode`] and [`Decode`] traits along with implementations for
//! primitive types, standard library containers, and acktor types.
//!

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::actor::{Actor, RemoteAddressable};
use crate::address::{Address, Recipient, RemoteMailbox, RemoteProxy, SenderInfo};
use crate::message::{Message, MessageId};

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use acktor_derive::{Decode, Encode};

mod error;
pub use error::{DecodeError, EncodeError};

mod table;
pub use table::{Codec, CodecTable, MessageCodec};

mod control_message;
mod ipc_message;

mod protobuf_helper;

mod common_codec;
#[cfg(not(feature = "prost-codec"))]
mod default_codec;
#[cfg(feature = "prost-codec")]
mod prost_codec;

/// Context for encoding messages.
pub trait EncodeContext {
    /// Registers an actor with its [`RemoteMailbox`].
    ///
    /// The actor becomes reachable from other processes after registration.
    fn register(&self, actor: RemoteMailbox) -> Result<(), EncodeError>;
}

/// Context for decoding messages.
pub trait DecodeContext {
    /// Returns the [`RemoteProxy`] associated with this context, if any.
    fn remote_proxy(&self) -> Option<Arc<dyn RemoteProxy + Send + Sync>>;
}

/// Describes how to encode a message.
pub trait Encode {
    /// Returns the number of bytes this message will encode to.
    fn encoded_len(&self) -> usize;

    /// Encodes the message into the provided buffer.
    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError>;

    /// Encodes the message into a freshly allocated [`Bytes`].
    fn encode_to_bytes(&self, ctx: Option<&dyn EncodeContext>) -> Result<Bytes, EncodeError> {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf, ctx)?;

        Ok(buf.freeze())
    }
}

/// Describes how to decode a message.
pub trait Decode {
    /// Decodes the message from the provided buffer.
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError>
    where
        Self: Sized;
}

impl<A> Address<A>
where
    A: Actor + RemoteAddressable,
{
    pub fn register(&self, ctx: &dyn EncodeContext) -> Result<(), EncodeError> {
        let actor_id = self.index();

        if actor_id.is_remote() {
            Err(EncodeError::EncodeRemoteAddress)
        } else {
            ctx.register(
                self.remote_mailbox()
                    .ok_or(EncodeError::NotRemoteAccessible)?,
            )
        }
    }

    pub fn new_with_decode_context(
        index: u64,
        ctx: &dyn DecodeContext,
    ) -> Result<Self, DecodeError> {
        let proxy = ctx.remote_proxy().ok_or(DecodeError::MissingRemoteProxy)?;
        Ok(Address::new_remote(index, proxy))
    }
}

impl<A> Encode for Address<A>
where
    A: Actor + RemoteAddressable,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(&self.index().as_local())
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        // auto-register the address if it is an local address
        self.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
        prost::Message::encode(&self.index().as_local(), buf).map_err(Into::into)
    }
}

impl<A> Decode for Address<A>
where
    A: Actor + RemoteAddressable,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let actor_id = <u64 as prost::Message>::decode(buf)?;
        Self::new_with_decode_context(actor_id, ctx.ok_or(DecodeError::MissingDecodeContext)?)
    }
}

impl<M> Recipient<M>
where
    M: Message,
{
    pub fn register(&self, ctx: &dyn EncodeContext) -> Result<(), EncodeError> {
        let actor_id = self.index();

        if actor_id.is_remote() {
            Err(EncodeError::EncodeRemoteAddress)
        } else {
            ctx.register(
                self.remote_mailbox()
                    .ok_or(EncodeError::NotRemoteAccessible)?,
            )
        }
    }

    pub fn new_with_decode_context(index: u64, ctx: &dyn DecodeContext) -> Result<Self, DecodeError>
    where
        M: MessageId + Encode,
        M::Result: Decode,
    {
        let proxy = ctx.remote_proxy().ok_or(DecodeError::MissingRemoteProxy)?;
        Ok(Recipient::new_remote(index, proxy))
    }
}

impl<M> Encode for Recipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(&self.index().as_local())
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        // auto-register the recipient if it is an local address
        self.register(ctx.ok_or(EncodeError::MissingEncodeContext)?)?;
        prost::Message::encode(&self.index().as_local(), buf).map_err(Into::into)
    }
}

impl<M> Decode for Recipient<M>
where
    M: Message + MessageId + Encode,
    M::Result: Decode,
{
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        let actor_id = <u64 as prost::Message>::decode(buf)?;
        Self::new_with_decode_context(actor_id, ctx.ok_or(DecodeError::MissingDecodeContext)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! create_test {
        ($name:ident, $type:ty, $value:expr) => {
            #[test]
            fn $name() -> anyhow::Result<()> {
                let value = $value;
                let buf = Encode::encode_to_bytes(&value, None)?;
                let decoded = <$type as Decode>::decode(buf, None)?;
                assert_eq!(value, decoded);

                Ok(())
            }
        };
    }

    create_test!(test_unit, (), ());
    create_test!(test_bool, bool, true);
    create_test!(test_u8, u8, 42_u8);
    create_test!(test_u16, u16, 4242_u16);
    create_test!(test_u32, u32, 424242_u32);
    create_test!(test_u64, u64, 42424242_u64);
    create_test!(test_usize, usize, 4242424242_usize);
    create_test!(test_i8, i8, -42_i8);
    create_test!(test_i16, i16, -4242_i16);
    create_test!(test_i32, i32, -424242_i32);
    create_test!(test_i64, i64, -42424242_i64);
    create_test!(test_isize, isize, -4242424242_isize);
    create_test!(test_f32, f32, 42.42_f32);
    create_test!(test_f64, f64, 42.42_f64);
    create_test!(test_string, String, "hello".to_string());

    create_test!(test_vec_bool, Vec<bool>, vec![true, false, true]);
    create_test!(test_vec_u8, Vec<u8>, vec![42_u8, 42_u8, 42_u8]);
    create_test!(test_vec_u16, Vec<u16>, vec![4242_u16, 4242_u16, 4242_u16]);
    create_test!(
        test_vec_u32,
        Vec<u32>,
        vec![424242_u32, 424242_u32, 424242_u32]
    );
    create_test!(
        test_vec_u64,
        Vec<u64>,
        vec![42424242_u64, 42424242_u64, 42424242_u64]
    );
    create_test!(
        test_vec_usize,
        Vec<usize>,
        vec![4242424242_usize, 4242424242_usize, 4242424242_usize]
    );
    create_test!(test_vec_i8, Vec<i8>, vec![-42_i8, -42_i8, -42_i8]);
    create_test!(
        test_vec_i16,
        Vec<i16>,
        vec![-4242_i16, -4242_i16, -4242_i16]
    );
    create_test!(
        test_vec_i32,
        Vec<i32>,
        vec![-424242_i32, -424242_i32, -424242_i32]
    );
    create_test!(
        test_vec_i64,
        Vec<i64>,
        vec![-42424242_i64, -42424242_i64, -42424242_i64]
    );
    create_test!(
        test_vec_isize,
        Vec<isize>,
        vec![-4242424242_isize, -4242424242_isize, -4242424242_isize]
    );
    create_test!(
        test_vec_f32,
        Vec<f32>,
        vec![42.42_f32, 42.42_f32, 42.42_f32]
    );
    create_test!(
        test_vec_f64,
        Vec<f64>,
        vec![42.42_f64, 42.42_f64, 42.42_f64]
    );

    create_test!(test_option_none, Option<u16>, None::<u16>);
    create_test!(test_option_some, Option<u16>, Some(4242_u16));

    create_test!(test_result_ok, Result<u32, String>, Ok::<u32, String>(424242_u32));
    create_test!(test_result_err, Result<u32, String>, Err::<u32, String>("hello".into()));
    create_test!(
        test_result_nested_option,
        Result<Option<u32>, String>,
        Ok::<Option<u32>, String>(Some(424242_u32))
    );

    create_test!(
        test_box_vec,
        Box<Vec<u16>>,
        Box::new(vec![4242_u16, 4242_u16, 4242_u16])
    );
    create_test!(
        test_arc_string,
        std::sync::Arc<String>,
        std::sync::Arc::new("hello".to_string())
    );

    create_test!(test_tuple2, (u32, String), (42_u32, "hello".to_string()));
    create_test!(
        test_tuple4,
        (i64, bool, String, Option<u16>),
        (-42424242_i64, true, "hello".to_string(), Some(4242_u16))
    );
    create_test!(
        test_nested_tuple,
        (u8, (i32, String)),
        (42_u8, (-424242_i32, "hello".to_string()))
    );

    #[test]
    fn test_result_string() -> anyhow::Result<()> {
        for value in [
            Ok::<String, String>("hello".to_string()),
            Err::<String, String>("boom".to_string()),
        ] {
            let expected_len = value.encoded_len();
            let mut buf = BytesMut::with_capacity(expected_len);
            value.encode(&mut buf, None)?;
            assert_eq!(buf.len(), expected_len);

            let decoded = <Result<String, String> as Decode>::decode(buf.freeze(), None)?;
            assert_eq!(value, decoded);
        }

        Ok(())
    }
}

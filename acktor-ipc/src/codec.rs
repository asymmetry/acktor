//! Codec traits for encoding and decoding remote messages over IPC channels.
//!
//! This module provides the [`Encode`] and [`Decode`] traits along with implementations for
//! primitive types, standard library containers, and acktor types.

use std::fmt::{self, Debug};

use bytes::{Bytes, BytesMut};

use acktor::{Actor, Address, Message, Recipient, SenderId};

use crate::remote_actor::RemoteActorRegistry;
use crate::remote_address::RemoteAddress;
use crate::remote_message::RecipientExt;
use crate::session::Session;

pub mod errors;
use errors::{DecodeError, EncodeError};

mod control_message;
mod ipc_message;

#[cfg(any(feature = "default-codec", feature = "prost-codec"))]
mod common_codec;
#[cfg(all(feature = "default-codec", not(feature = "prost-codec")))]
mod default_codec;
#[cfg(feature = "prost-codec")]
mod prost_codec;

/// Context passed through every [`Encode::encode`] call.
///
/// Carries the remote actor registry so that when the address of an actor which can be reached
/// over IPC is serialized for the first time, it can self-register in the registry.
#[derive(Debug, Clone, Default)]
pub struct EncodeContext {
    /// The actor registry associated with this context.
    pub registry: RemoteActorRegistry,
}

impl EncodeContext {
    /// Constructs a new [`EncodeContext`] from the given actor registry.
    pub fn new(registry: RemoteActorRegistry) -> Self {
        Self { registry }
    }
}

/// Context passed through every [`Decode::decode`] call.
///
/// Carries the IPC session actor address and the remote actor registry which are needed to
/// reconstruct [`RemoteAddress`]s during decoding.
#[derive(Clone)]
pub struct DecodeContext {
    /// The remote session associated with this context.
    pub session: Address<Session>,
    /// The actor registry associated with this context.
    pub registry: RemoteActorRegistry,
}

impl Debug for DecodeContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DecodeContext")
            .field(
                "session",
                &format_args!("Session({})", self.session.index()),
            )
            .field("registry", &self.registry)
            .finish()
    }
}

impl DecodeContext {
    /// Constructs a new [`DecodeContext`] from the given remote session and the actor registry.
    pub fn new(session: Address<Session>, registry: RemoteActorRegistry) -> Self {
        Self { session, registry }
    }
}

/// Describes how to encode a remote message.
pub trait Encode {
    /// Returns the number of bytes this value will encode to.
    fn encoded_len(&self) -> usize;

    /// Encodes the value into the provided buffer.
    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError>;

    /// Encodes the value into a freshly allocated [`Bytes`].
    fn encode_to_bytes(&self, ctx: Option<&EncodeContext>) -> Result<Bytes, EncodeError> {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf, ctx)?;

        Ok(buf.freeze())
    }
}

/// Describes how to decode a remote message.
pub trait Decode {
    /// Decodes the remote message from the provided buffer.
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError>
    where
        Self: Sized;
}

impl<A> Encode for Address<A>
where
    A: Actor,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(&self.index())
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        // auto-register the address if it is an local address
        if !self.index().is_remote() {
            let ctx = ctx.ok_or(EncodeError::MissingEncodeContext)?;
            if let Ok(recipient) = self.to_recipient_remote_message() {
                ctx.registry.insert(recipient);
            }
        }
        prost::Message::encode(&self.index(), buf).map_err(Into::into)
    }
}

impl<M> Encode for Recipient<M>
where
    M: Message,
{
    #[inline]
    fn encoded_len(&self) -> usize {
        prost::Message::encoded_len(&self.index())
    }

    #[inline]
    fn encode(&self, buf: &mut BytesMut, ctx: Option<&EncodeContext>) -> Result<(), EncodeError> {
        // auto-register the address if it is an local address
        if !self.index().is_remote() {
            let ctx = ctx.ok_or(EncodeError::MissingEncodeContext)?;
            if let Ok(recipient) = self.to_recipient_remote_message() {
                ctx.registry.insert(recipient);
            }
        }
        prost::Message::encode(&self.index(), buf).map_err(Into::into)
    }
}

impl Decode for RemoteAddress {
    #[inline]
    fn decode(buf: Bytes, ctx: Option<&DecodeContext>) -> Result<Self, DecodeError> {
        let actor_id = <u64 as prost::Message>::decode(buf)?;
        if actor_id.is_remote() {
            return Err(DecodeError::DecodeRemoteAddress);
        }
        let ctx = ctx.ok_or(DecodeError::MissingDecodeContext)?;

        Ok(RemoteAddress::new(
            actor_id,
            ctx.session.clone(),
            ctx.registry.clone(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! create_test {
        ($name:ident, $type:ty, $value:expr) => {
            #[test]
            fn $name() {
                let value = $value;
                let buf = Encode::encode_to_bytes(&value, None).unwrap();
                let decoded = <$type as Decode>::decode(buf, None).unwrap();
                assert_eq!(value, decoded);
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
}

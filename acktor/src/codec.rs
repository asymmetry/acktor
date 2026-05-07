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
    ///
    /// The buffer must have at least `self.encoded_len()` bytes of capacity. If not, the encoding
    /// may or may not fail with an error, depending on the implementation.
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
                    .ok_or(EncodeError::NotRemoteAddressable)?,
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
                    .ok_or(EncodeError::NotRemoteAddressable)?,
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
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::utils::test_utils::{Dummy, DummyProxy, Ping, make_address};

    fn roundtrip<T>(value: T) -> anyhow::Result<()>
    where
        T: Encode + Decode + PartialEq + Debug,
    {
        let expected_len = value.encoded_len();
        let mut buf = BytesMut::with_capacity(expected_len);
        value.encode(&mut buf, None)?;
        let buf = buf.freeze();
        assert_eq!(buf.len(), expected_len);

        let direct = value.encode_to_bytes(None)?;
        assert_eq!(direct.len(), expected_len);
        assert_eq!(buf, direct);

        let decoded = T::decode(buf, None)?;
        assert_eq!(value, decoded);

        Ok(())
    }

    #[test]
    fn test_primitive() -> anyhow::Result<()> {
        roundtrip(())?;
        roundtrip(true)?;
        roundtrip(42_u8)?;
        roundtrip(4242_u16)?;
        roundtrip(424242_u32)?;
        roundtrip(42424242_u64)?;
        roundtrip(4242424242_usize)?;
        roundtrip(-42_i8)?;
        roundtrip(-4242_i16)?;
        roundtrip(-424242_i32)?;
        roundtrip(-42424242_i64)?;
        roundtrip(-4242424242_isize)?;
        roundtrip(42.42_f32)?;
        roundtrip(42.42_f64)?;
        roundtrip("hello".to_string())?;

        Ok(())
    }

    #[test]
    fn test_vector() -> anyhow::Result<()> {
        roundtrip(vec![true, false, true])?;
        roundtrip(vec![42_u8, 42_u8, 42_u8])?;
        roundtrip(vec![4242_u16, 4242_u16, 4242_u16])?;
        roundtrip(vec![424242_u32, 424242_u32, 424242_u32])?;
        roundtrip(vec![42424242_u64, 42424242_u64, 42424242_u64])?;
        roundtrip(vec![42424242_usize, 42424242_usize, 42424242_usize])?;
        roundtrip(vec![-42_i8, -42_i8, -42_i8])?;
        roundtrip(vec![-4242_i16, -4242_i16, -4242_i16])?;
        roundtrip(vec![-424242_i32, -424242_i32, -424242_i32])?;
        roundtrip(vec![-42424242_i64, -42424242_i64, -42424242_i64])?;
        roundtrip(vec![-42424242_isize, -42424242_isize, -42424242_isize])?;
        roundtrip(vec![42.42_f32, 42.42_f32, 42.42_f32])?;
        roundtrip(vec![42.42_f64, 42.42_f64, 42.42_f64])?;
        // empty vector
        roundtrip(Vec::<bool>::new())?;
        roundtrip(Vec::<u16>::new())?;
        roundtrip(Vec::<f32>::new())?;
        roundtrip(Vec::<usize>::new())?;
        roundtrip(Vec::<isize>::new())?;

        Ok(())
    }

    #[test]
    fn test_option() -> anyhow::Result<()> {
        roundtrip(None::<u16>)?;
        roundtrip(Some(4242_u16))?;

        Ok(())
    }

    #[test]
    fn test_result() -> anyhow::Result<()> {
        roundtrip(Ok::<String, String>("hello".to_string()))?;
        roundtrip(Err::<String, String>("boom".to_string()))?;

        Ok(())
    }

    #[test]
    fn test_smart_pointer() -> anyhow::Result<()> {
        roundtrip(Box::new(vec![4242_u16, 4242_u16, 4242_u16]))?;
        roundtrip(Arc::new(vec![4242_u16, 4242_u16, 4242_u16]))?;

        Ok(())
    }

    #[test]
    fn test_tuple() -> anyhow::Result<()> {
        roundtrip((42_u32, "hello".to_string()))?;
        roundtrip((-42424242_i64, true, "hello".to_string(), Some(4242_u16)))?;
        // tuple in tuple
        roundtrip((42_u8, (-424242_i32, "hello".to_string())))?;

        #[cfg(not(feature = "prost-codec"))]
        {
            use crate::error::ErrorReport;

            let bad: Bytes = vec![0_u8, 1_u8, 2_u8].into();
            let result = <(u32, u32)>::decode(bad, None);
            assert_eq!(
                result.unwrap_err().report(),
                "could not decode the message: missing the tuple element length"
            );

            let bad: Bytes = vec![0xff_u8, 0xff_u8, 0xff_u8, 0xff_u8, 42_u8].into();
            let result = <(u32, u32)>::decode(bad, None);
            assert_eq!(
                result.unwrap_err().report(),
                "could not decode the message: missing the tuple element data"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_address() -> anyhow::Result<()> {
        use crate::error::ErrorReport;

        let proxy = DummyProxy::new();

        let (address, _) = make_address(1);

        let expected_len = address.encoded_len();
        let mut buf = BytesMut::with_capacity(expected_len);
        address.encode(&mut buf, proxy.encode_context())?;
        let buf = buf.freeze();
        assert_eq!(buf.len(), expected_len);

        let direct = address.encode_to_bytes(proxy.encode_context())?;
        assert_eq!(direct.len(), expected_len);
        assert_eq!(buf, direct);

        let decoded = Address::<Dummy>::decode(buf, proxy.decode_context())?;
        assert_eq!(address.index().as_local(), decoded.index().as_local());

        let address = Address::<Dummy>::new_remote(42, proxy.clone());
        let result = address.encode_to_bytes(proxy.encode_context());
        assert_eq!(
            result.unwrap_err().report(),
            "remote address should not be encoded into a message"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_recipient() -> anyhow::Result<()> {
        use crate::error::ErrorReport;

        let proxy = DummyProxy::new();

        let (address, _) = make_address(1);
        let recipient: Recipient<Ping> = address.into();

        let expected_len = recipient.encoded_len();
        let mut buf = BytesMut::with_capacity(expected_len);
        recipient.encode(&mut buf, proxy.encode_context())?;
        let buf = buf.freeze();
        assert_eq!(buf.len(), expected_len);

        let direct = recipient.encode_to_bytes(proxy.encode_context())?;
        assert_eq!(direct.len(), expected_len);
        assert_eq!(buf, direct);

        let decoded = Recipient::<Ping>::decode(buf, proxy.decode_context())?;
        assert_eq!(recipient.index().as_local(), decoded.index().as_local());

        let recipient = Recipient::<Ping>::new_remote(42, proxy.clone());
        let result = recipient.encode_to_bytes(proxy.encode_context());
        assert_eq!(
            result.unwrap_err().report(),
            "remote address should not be encoded into a message"
        );

        Ok(())
    }
}

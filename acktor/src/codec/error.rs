use std::fmt::{Debug, Display};
use std::ops::Deref;

use thiserror::Error;
use zerocopy::{KnownLayout, TryFromBytes};

use crate::error::BoxError;

/// Error type used by [`Encode`][crate::codec::Encode].
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("missing encode context")]
    MissingEncodeContext,

    #[error("remote address should not be encoded into a message")]
    EncodeRemoteAddress,

    #[error("the actor is not remote accessible")]
    NotRemoteAccessible,

    #[error(transparent)]
    ProstEncodeError(#[from] prost::EncodeError),

    #[error("could not encode the message")]
    Other(BoxError),
}

impl EncodeError {
    /// Constructs a new [`EncodeError`] from an arbitrary error.
    pub fn other<E>(err: E) -> Self
    where
        E: Into<BoxError>,
    {
        EncodeError::Other(err.into())
    }
}

impl From<BoxError> for EncodeError {
    fn from(e: BoxError) -> Self {
        Self::Other(e)
    }
}

impl From<String> for EncodeError {
    fn from(s: String) -> Self {
        Self::other(s)
    }
}

impl From<&str> for EncodeError {
    fn from(s: &str) -> Self {
        Self::other(s)
    }
}

/// Error type used by [`Decode`][crate::codec::Decode].
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("missing decode context")]
    MissingDecodeContext,

    #[error("message should not contain a remote address")]
    DecodeRemoteAddress,

    #[error("unknown message id: {0}")]
    UnknownMessageId(u64),

    #[error("decode context does not contain a remote proxy")]
    MissingRemoteProxy,

    #[error(transparent)]
    ProstDecodeError(#[from] prost::DecodeError),

    #[error("could not decode the message: {0}")]
    ZerocopyError(String),

    #[error("could not decode the message")]
    Other(BoxError),
}

impl DecodeError {
    /// Constructs a new [`DecodeError`] from an arbitrary error.
    pub fn other<E>(err: E) -> Self
    where
        E: Into<BoxError>,
    {
        DecodeError::Other(err.into())
    }
}

impl From<BoxError> for DecodeError {
    fn from(e: BoxError) -> Self {
        Self::Other(e)
    }
}

impl From<String> for DecodeError {
    fn from(s: String) -> Self {
        Self::other(s)
    }
}

impl From<&str> for DecodeError {
    fn from(s: &str) -> Self {
        Self::other(s)
    }
}

impl<A, S, V> From<zerocopy::ConvertError<A, S, V>> for DecodeError
where
    A: Debug + Display,
    S: Debug + Display,
    V: Debug + Display,
{
    fn from(err: zerocopy::ConvertError<A, S, V>) -> Self {
        DecodeError::ZerocopyError(err.to_string())
    }
}

impl<Src, Dst> From<zerocopy::AlignmentError<Src, Dst>> for DecodeError
where
    Src: Deref,
    Dst: KnownLayout + ?Sized,
{
    fn from(err: zerocopy::AlignmentError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError(err.to_string())
    }
}

impl<Src, Dst> From<zerocopy::SizeError<Src, Dst>> for DecodeError
where
    Src: Deref,
    Dst: KnownLayout + ?Sized,
{
    fn from(err: zerocopy::SizeError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError(err.to_string())
    }
}

impl<Src, Dst> From<zerocopy::ValidityError<Src, Dst>> for DecodeError
where
    Dst: KnownLayout + TryFromBytes + ?Sized,
{
    fn from(err: zerocopy::ValidityError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError(err.to_string())
    }
}

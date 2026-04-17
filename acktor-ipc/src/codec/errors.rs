use std::borrow::Cow;

use thiserror::Error;

use crate::remote_message::ToRemoteMessageRecipientError;

/// Error type used by [`Encode`][crate::codec::Encode].
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error(transparent)]
    ProstEncodeError(#[from] prost::EncodeError),

    #[error("remote address cannot be encoded in a remote message")]
    EncodeRemoteAddress,

    #[error(transparent)]
    ToRemoteMessageRecipientError(#[from] ToRemoteMessageRecipientError),

    #[error("missing encode context")]
    MissingEncodeContext,

    #[error("could not encode the message: {description}")]
    Other { description: Cow<'static, str> },
}

impl From<&'static str> for EncodeError {
    fn from(description: &'static str) -> Self {
        EncodeError::Other {
            description: description.into(),
        }
    }
}

impl From<String> for EncodeError {
    fn from(description: String) -> Self {
        EncodeError::Other {
            description: description.into(),
        }
    }
}

/// Error type used by [`Decode`][crate::codec::Decode].
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error(transparent)]
    ProstDecodeError(#[from] prost::DecodeError),

    #[error("could not decode the message: {description}")]
    ZerocopyError { description: Cow<'static, str> },

    #[error("remote message should not contain a remote address")]
    DecodeRemoteAddress,

    #[error("missing decode context")]
    MissingDecodeContext,

    #[error("could not decode the message: {description}")]
    Other { description: Cow<'static, str> },
}

impl From<&'static str> for DecodeError {
    fn from(description: &'static str) -> Self {
        DecodeError::Other {
            description: description.into(),
        }
    }
}

impl From<String> for DecodeError {
    fn from(description: String) -> Self {
        DecodeError::Other {
            description: description.into(),
        }
    }
}

impl<A, S, V> From<zerocopy::ConvertError<A, S, V>> for DecodeError
where
    A: std::fmt::Display + std::fmt::Debug,
    S: std::fmt::Display + std::fmt::Debug,
    V: std::fmt::Display + std::fmt::Debug,
{
    fn from(err: zerocopy::ConvertError<A, S, V>) -> Self {
        DecodeError::ZerocopyError {
            description: err.to_string().into(),
        }
    }
}

impl<Src, Dst> From<zerocopy::AlignmentError<Src, Dst>> for DecodeError
where
    Src: std::ops::Deref,
    Dst: zerocopy::KnownLayout + ?Sized,
{
    fn from(err: zerocopy::AlignmentError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError {
            description: err.to_string().into(),
        }
    }
}

impl<Src, Dst> From<zerocopy::SizeError<Src, Dst>> for DecodeError
where
    Src: std::ops::Deref,
    Dst: zerocopy::KnownLayout + ?Sized,
{
    fn from(err: zerocopy::SizeError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError {
            description: err.to_string().into(),
        }
    }
}

impl<Src, Dst> From<zerocopy::ValidityError<Src, Dst>> for DecodeError
where
    Dst: zerocopy::KnownLayout + zerocopy::TryFromBytes + ?Sized,
{
    fn from(err: zerocopy::ValidityError<Src, Dst>) -> Self {
        DecodeError::ZerocopyError {
            description: err.to_string().into(),
        }
    }
}

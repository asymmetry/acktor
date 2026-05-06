use thiserror::Error;

use crate::error::BoxError;

/// Error type used by [`Encode`][crate::codec::Encode].
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("missing encode context")]
    MissingEncodeContext,

    #[error("remote address should not be encoded into a message")]
    EncodeRemoteAddress,

    #[error("the actor is not remote addressable")]
    NotRemoteAddressable,

    #[error("could not encode the message")]
    Other(#[source] BoxError),
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
    #[inline]
    fn from(e: BoxError) -> Self {
        Self::other(e)
    }
}

impl From<String> for EncodeError {
    #[inline]
    fn from(s: String) -> Self {
        Self::other(s)
    }
}

impl From<&str> for EncodeError {
    #[inline]
    fn from(s: &str) -> Self {
        Self::other(s)
    }
}

impl From<prost::EncodeError> for EncodeError {
    #[inline]
    fn from(e: prost::EncodeError) -> Self {
        Self::other(e)
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

    #[error("could not decode the message")]
    Other(#[source] BoxError),
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
    #[inline]
    fn from(e: BoxError) -> Self {
        Self::other(e)
    }
}

impl From<String> for DecodeError {
    #[inline]
    fn from(s: String) -> Self {
        Self::other(s)
    }
}

impl From<&str> for DecodeError {
    #[inline]
    fn from(s: &str) -> Self {
        Self::other(s)
    }
}

impl From<prost::DecodeError> for DecodeError {
    #[inline]
    fn from(e: prost::DecodeError) -> Self {
        Self::other(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_errors() {
        let from_str: EncodeError = "boom".into();
        assert!(matches!(from_str, EncodeError::Other(_)));
        let from_string: EncodeError = String::from("boom").into();
        assert!(matches!(from_string, EncodeError::Other(_)));
        let boxed: BoxError = "boom".into();
        let from_box: EncodeError = boxed.into();
        assert!(matches!(from_box, EncodeError::Other(_)));

        let from_str: DecodeError = "boom".into();
        assert!(matches!(from_str, DecodeError::Other(_)));
        let from_string: DecodeError = String::from("boom").into();
        assert!(matches!(from_string, DecodeError::Other(_)));
        let boxed: BoxError = "boom".into();
        let from_box: DecodeError = boxed.into();
        assert!(matches!(from_box, DecodeError::Other(_)));
    }
}

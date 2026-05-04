//! Error types used by this crate.
//!

use std::error::Error as StdError;
use std::fmt::{self, Debug, Display};

use thiserror::Error;
use tokio::sync::{
    mpsc::error::{
        SendError as MpscSendError, SendTimeoutError as MpscSendTimeoutError,
        TryRecvError as MpscTryRecvError, TrySendError as MpscTrySendError,
    },
    oneshot::error::{RecvError as OneshotRecvError, TryRecvError as OneshotTryRecvError},
};

#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use crate::codec::{DecodeError, EncodeError};

mod report;
pub use report::ErrorReport;

/// Alias for a type-erased error type.
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// Error returned when sending a message.
pub enum SendError<M> {
    Closed(M),
    Full(M),
    Timeout(M),
    #[cfg(feature = "ipc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
    NoEncodeFn(M),
    // the behavior is similar to the #[transparent] attribute of this error
    Other(BoxError, M),
}

impl<M> SendError<M> {
    /// Constructs a new [`SendError`] from an arbitrary error and the message that failed to be
    /// sent.
    pub fn other<E>(err: E, msg: M) -> Self
    where
        E: Into<BoxError>,
    {
        Self::Other(err.into(), msg)
    }
}

#[cfg(feature = "ipc")]
impl SendError<crate::message::BinaryMessage> {
    pub(crate) fn with_msg<M>(self, msg: M) -> SendError<M> {
        match self {
            SendError::Closed(_) => SendError::Closed(msg),
            SendError::Full(_) => SendError::Full(msg),
            SendError::Timeout(_) => SendError::Timeout(msg),
            SendError::NoEncodeFn(_) => SendError::NoEncodeFn(msg),
            SendError::Other(e, _) => SendError::Other(e, msg),
        }
    }
}

impl<M> Debug for SendError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => fmt.write_str("Closed(..)"),
            SendError::Full(_) => fmt.write_str("Full(..)"),
            SendError::Timeout(_) => fmt.write_str("Timeout(..)"),
            #[cfg(feature = "ipc")]
            SendError::NoEncodeFn(_) => fmt.write_str("NoEncodeFn(..)"),
            SendError::Other(trans, _) => Debug::fmt(trans, fmt),
        }
    }
}

impl<M> Display for SendError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SendError::Closed(_) => fmt.write_str("sending on a closed channel"),
            SendError::Full(_) => fmt.write_str("sending on a full channel"),
            SendError::Timeout(_) => fmt.write_str("timed out waiting on sending"),
            #[cfg(feature = "ipc")]
            SendError::NoEncodeFn(_) => fmt.write_str(&format!(
                "no encode function for message type {}",
                crate::utils::ShortName::of::<M>()
            )),
            SendError::Other(trans, _) => Display::fmt(trans, fmt),
        }
    }
}

impl<M> StdError for SendError<M> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SendError::Closed(_) => Option::None,
            SendError::Full(_) => Option::None,
            SendError::Timeout(_) => Option::None,
            #[cfg(feature = "ipc")]
            SendError::NoEncodeFn(_) => Option::None,
            SendError::Other(trans, _) => trans.source(),
        }
    }
}

impl<M> From<MpscSendError<M>> for SendError<M> {
    fn from(e: MpscSendError<M>) -> Self {
        Self::Closed(e.0)
    }
}

impl<M> From<MpscTrySendError<M>> for SendError<M> {
    fn from(e: MpscTrySendError<M>) -> Self {
        match e {
            MpscTrySendError::Closed(m) => Self::Closed(m),
            MpscTrySendError::Full(m) => Self::Full(m),
        }
    }
}

impl<M> From<MpscSendTimeoutError<M>> for SendError<M> {
    fn from(e: MpscSendTimeoutError<M>) -> Self {
        match e {
            MpscSendTimeoutError::Closed(m) => Self::Closed(m),
            MpscSendTimeoutError::Timeout(m) => Self::Timeout(m),
        }
    }
}

/// Error returned when receiving a message.
#[derive(Debug, Error)]
pub enum RecvError {
    #[error("receiving on a closed channel")]
    Closed,

    #[error("receiving on an empty channel")]
    Empty,

    #[error("timed out waiting on receiving")]
    Timeout,

    #[error(transparent)]
    Other(BoxError),
}

impl RecvError {
    /// Constructs a new [`RecvError`] from an arbitrary error.
    pub fn other<E>(err: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::Other(err.into())
    }
}

impl From<MpscTryRecvError> for RecvError {
    fn from(e: MpscTryRecvError) -> Self {
        match e {
            MpscTryRecvError::Disconnected => Self::Closed,
            MpscTryRecvError::Empty => Self::Empty,
        }
    }
}

impl From<OneshotRecvError> for RecvError {
    fn from(_: OneshotRecvError) -> Self {
        Self::Closed
    }
}

impl From<OneshotTryRecvError> for RecvError {
    fn from(e: OneshotTryRecvError) -> Self {
        match e {
            OneshotTryRecvError::Closed => Self::Closed,
            OneshotTryRecvError::Empty => Self::Empty,
        }
    }
}

impl From<BoxError> for RecvError {
    fn from(e: BoxError) -> Self {
        Self::Other(e)
    }
}

impl From<String> for RecvError {
    fn from(s: String) -> Self {
        Self::other(s)
    }
}

impl From<&str> for RecvError {
    fn from(s: &str) -> Self {
        Self::other(s)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use anyhow::Context;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_error_report() {
        let err: SendError<()> = SendError::Closed(());
        assert_eq!(err.report(), "sending on a closed channel");

        let io_err = io::Error::other("io-level");
        let err: SendError<()> = SendError::other(io_err, ());
        assert_eq!(err.report(), "io-level");

        let box_err: BoxError = Box::new(io::Error::other("boxed"));
        assert_eq!(box_err.report(), "boxed");

        let e = Err::<(), _>(io::Error::other("root cause"))
            .context("middle context")
            .context("top context")
            .unwrap_err();

        let err_ref: &(dyn std::error::Error + Send + Sync) = e.as_ref();
        assert_eq!(err_ref.report(), "top context: middle context: root cause",);
    }

    #[test]
    fn test_error() {
        // SendError Display
        let err: SendError<()> = SendError::Closed(());
        assert_eq!(format!("{err}"), "sending on a closed channel");

        let err: SendError<()> = SendError::Full(());
        assert_eq!(format!("{err}"), "sending on a full channel");

        let err: SendError<()> = SendError::Timeout(());
        assert_eq!(format!("{err}"), "timed out waiting on sending");

        let err: SendError<()> = SendError::other(io::Error::other("io-level"), ());
        assert_eq!(format!("{err}"), "io-level");

        // SendError Debug
        let err: SendError<()> = SendError::Closed(());
        assert_eq!(format!("{err:?}"), "Closed(..)");

        let err: SendError<()> = SendError::Full(());
        assert_eq!(format!("{err:?}"), "Full(..)");

        let err: SendError<()> = SendError::Timeout(());
        assert_eq!(format!("{err:?}"), "Timeout(..)");

        // RecvError Display
        assert_eq!(
            format!("{}", RecvError::Closed),
            "receiving on a closed channel"
        );
        assert_eq!(
            format!("{}", RecvError::Empty),
            "receiving on an empty channel"
        );
        assert_eq!(
            format!("{}", RecvError::Timeout),
            "timed out waiting on receiving"
        );

        // RecvError Debug
        assert_eq!(format!("{:?}", RecvError::Closed), "Closed");
        assert_eq!(format!("{:?}", RecvError::Empty), "Empty");
        assert_eq!(format!("{:?}", RecvError::Timeout), "Timeout");

        // From<String> for RecvError
        let err: RecvError = String::from("string-error").into();
        assert_eq!(format!("{err}"), "string-error");

        // From<&str> for RecvError
        let err: RecvError = "str-error".into();
        assert_eq!(format!("{err}"), "str-error");
    }
}

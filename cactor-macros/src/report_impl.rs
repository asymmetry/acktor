use std::error::Error;
use std::fmt::Write;
use std::iter;

/// An error reporter that prints an error and its sources.
///
/// There is a nightly-only API [`Report`][std::error::Report] which could be used to do the same
/// thing. However, there are some limitations:
/// - It requires unstable feature.
/// - It does not support `Box<dyn Error>` as input.
/// - It consumes the error.
///
/// Since `Box<dyn Error>` does not implement [`Error`][std::error::Error] trait, we could not
/// use `E: Error` bound if we need to handle error types and `Box<dyn Error>` with a generic API, this
/// is a common problem. For example, we could not use [`Report::from`][std::error::Report::from] with
/// `Box<dyn Error>` since it requires `E: Error` bound. We could use specialization to work around this.
/// Unfortunately, specialization is another unstable feature. In stable Rust, we have to use a trick
/// called [dtolnay's specialization](https://github.com/dtolnay/case-studies/blob/master/autoref-specialization/README.md).
/// This trick requires us to define the API as a macro.
#[macro_export]
macro_rules! report {
    ($err:expr) => {{
        #[allow(unused_imports)]
        use $crate::report_impl::{BoxedErrorKind, StdErrorKind};
        match $err {
            ref error => (&error).report_kind().report(error),
        }
    }};
}

pub struct BoxedErrorTag;

impl BoxedErrorTag {
    #[allow(clippy::borrowed_box)]
    pub fn report<E>(self, error: E) -> String
    where
        E: AsRef<dyn Error + Send + Sync>,
    {
        let mut result = String::new();
        write!(result, "{}", error.as_ref()).unwrap();
        for source in iter::successors(error.as_ref().source(), |e| (*e).source()) {
            write!(result, ": {source}").unwrap();
        }
        result
    }
}

pub trait BoxedErrorKind {
    #[inline]
    fn report_kind(&self) -> BoxedErrorTag {
        BoxedErrorTag
    }
}

impl<E> BoxedErrorKind for &E where E: AsRef<dyn Error + Send + Sync> {}

pub struct StdErrorTag;

impl StdErrorTag {
    pub fn report<E>(self, error: E) -> String
    where
        E: Error,
    {
        let mut result = String::new();
        write!(result, "{error}").unwrap();
        for source in iter::successors(error.source(), |e| (*e).source()) {
            write!(result, ": {source}").unwrap();
        }
        result
    }
}

pub trait StdErrorKind {
    #[inline]
    fn report_kind(&self) -> StdErrorTag {
        StdErrorTag
    }
}

impl<E> StdErrorKind for E where E: Error {}

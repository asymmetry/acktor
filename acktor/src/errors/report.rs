use std::error::Error;
use std::fmt::Write;
use std::iter;

/// A trait which allows an error to print itself and its sources (if any) recursively.
pub trait ErrorReport {
    fn report(&self) -> String;
}

macro_rules! impl_report {
    ($this:ident) => {{
        let mut result = String::new();
        write!(result, "{}", $this).unwrap();
        for source in iter::successors($this.source(), |e| (*e).source()) {
            write!(result, ": {}", source).unwrap();
        }
        result
    }};
}

impl<E> ErrorReport for E
where
    E: Error,
{
    fn report(&self) -> String {
        impl_report!(self)
    }
}

impl ErrorReport for dyn Error {
    fn report(&self) -> String {
        impl_report!(self)
    }
}

impl ErrorReport for dyn Error + Send {
    fn report(&self) -> String {
        impl_report!(self)
    }
}

impl ErrorReport for dyn Error + Send + Sync {
    fn report(&self) -> String {
        impl_report!(self)
    }
}

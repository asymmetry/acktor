/// Traces only in debug mode.
#[macro_export]
macro_rules! debug_trace {
    ($($arg:tt)+) => {
        #[cfg(debug_assertions)]
        tracing::trace!($($arg)+);
    };
}

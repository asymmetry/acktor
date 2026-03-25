/// Notifies all observers with the given event asynchronously, cleaning up closed observers.
#[macro_export]
macro_rules! notify_observers {
    ($observers:expr, $event:expr) => {
        let mut should_clean = false;
        for observer in $observers.iter() {
            #[cfg(feature = "bottleneck-warning")]
            if observer.capacity() == 0 {
                tracing::debug!("Actor {} is full", observer.index());
            }
            if observer.do_send($event.clone()).await.is_err() {
                should_clean = true;
            }
        }
        if should_clean {
            $observers.retain(|observer| !observer.is_closed())
        }
    };
}

/// Tries to notify all observers with the given event, cleaning up closed observers.
#[macro_export]
macro_rules! try_notify_observers {
    ($observers:expr, $event:expr) => {
        let mut should_clean = false;
        for observer in $observers.iter() {
            if let Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) =
                observer.try_do_send($event.clone())
            {
                should_clean = true;
            }
        }
        if should_clean {
            $observers.retain(|observer| !observer.is_closed())
        }
    };
}

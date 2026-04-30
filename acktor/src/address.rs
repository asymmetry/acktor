//! Traits and type definitions for actor address.
//!
//! In the actor model, an [`Address`] is a handle to an actor. It is the only way to interact
//! with an actor since the runtime will take the ownership of the actor itself after it is
//! spawned.
//!
//! This module defines the [`Address`] type for an actor. It also provides the [`Sender`] trait
//! and a [`Recipient`] type which are alternative ways to organize the addresses of actors.
//!

use std::sync::atomic::{AtomicU64, Ordering};

mod address_impl;
pub use address_impl::Address;

mod permit;
pub use permit::{OwnedSendPermit, SendPermit};

mod sender;
pub use sender::{
    DoSendResult, DoSendResultFuture, EmptyFuture, SendResult, SendResultFuture, Sender, SenderId,
};

mod recipient;
pub use recipient::Recipient;

mod mailbox;
pub use mailbox::Mailbox;

#[cfg(feature = "ipc")]
mod remote;
#[cfg(feature = "ipc")]
pub use remote::RemoteAddress;

#[cfg(feature = "ipc")]
mod remote_proxy;
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
pub use remote_proxy::RemoteProxy;

static INDEX_GENERATOR: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn next_actor_id() -> u64 {
    INDEX_GENERATOR.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::{Context as _, Result};
    use pretty_assertions::assert_eq;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::channel::mpsc;
    use crate::envelope::Envelope;
    use crate::error::SendError;
    use crate::test_utils::{Dummy, Ping};

    fn make_address(capacity: usize) -> (Address<Dummy>, Mailbox<Dummy>) {
        let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
        (Address::new(tx), Mailbox::new(rx))
    }

    #[tokio::test]
    async fn test_address() -> Result<()> {
        // clone + eq
        let (a1, _m1) = make_address(4);
        let clone = a1.clone();
        assert_eq!(a1, clone);
        assert_eq!(a1.index(), clone.index());

        // hash
        #[allow(clippy::mutable_key_type)]
        let mut map = HashSet::new();
        map.insert(a1);
        map.insert(clone);
        assert_eq!(map.len(), 1, "clones should have the same hash and equal");

        // index is unique
        let (a1, m1) = make_address(4);
        let (a2, _) = make_address(4);
        assert_ne!(a1, a2);
        assert_ne!(a1.index(), a2.index());

        // capacity + is_closed + closed
        assert_eq!(a1.capacity(), 4);
        assert!(!a1.is_closed());

        let closed = a1.closed();
        drop(m1);
        assert!(a1.is_closed());
        timeout(Duration::from_millis(500), closed)
            .await
            .context("closed() should resolve after mailbox drop")?;

        // do_send
        let (a1, m1) = make_address(1);
        a1.do_send(Ping(1)).await?;
        assert_eq!(m1.len(), 1);

        // send
        let (a1, m1) = make_address(1);
        a1.send(Ping(2)).await?;
        assert_eq!(m1.len(), 1);

        // try_do_send
        let (a1, m1) = make_address(1);
        a1.try_do_send(Ping(3))?;
        assert_eq!(m1.len(), 1);

        let result = a1.try_do_send(Ping(4));
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );

        drop(m1);
        let result = a1.try_do_send(Ping(5));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // try_send
        let (a1, m1) = make_address(1);
        a1.try_send(Ping(6))?;
        assert_eq!(m1.len(), 1);

        let result = a1.try_send(Ping(7));
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );
        drop(m1);

        let result = a1.try_send(Ping(8));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // do_send_timeout
        let (a1, m1) = make_address(1);
        a1.do_send_timeout(Ping(9), Duration::from_millis(10))
            .await?;
        assert_eq!(m1.len(), 1);

        let result = a1
            .do_send_timeout(Ping(10), Duration::from_millis(10))
            .await;
        assert!(
            matches!(result, Err(SendError::Timeout(_))),
            "expected Timeout, got {result:?}"
        );

        drop(m1);
        let result = a1
            .do_send_timeout(Ping(11), Duration::from_millis(10))
            .await;
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // send_timeout
        let (a1, m1) = make_address(1);
        a1.send_timeout(Ping(12), Duration::from_millis(10)).await?;
        assert_eq!(m1.len(), 1);

        let result = a1.send_timeout(Ping(13), Duration::from_millis(10)).await;
        assert!(
            matches!(result, Err(SendError::Timeout(_))),
            "expected Timeout, got {result:?}"
        );

        drop(m1);
        let result = a1.send_timeout(Ping(14), Duration::from_millis(10)).await;
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // blocking_do_send
        let (a1, m1) = make_address(1);
        tokio::task::spawn_blocking(move || -> Result<()> {
            a1.blocking_do_send(Ping(15))?;
            assert_eq!(m1.len(), 1);

            Ok(())
        })
        .await??;

        // blocking_send
        let (a1, m1) = make_address(1);
        tokio::task::spawn_blocking(move || -> Result<()> {
            a1.blocking_send(Ping(16))?;
            assert_eq!(m1.len(), 1);

            Ok(())
        })
        .await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_mailbox() -> Result<()> {
        // basics
        let (a1, mut m1) = make_address(4);
        assert!(m1.is_empty());
        assert_eq!(m1.len(), 0);
        assert_eq!(m1.capacity(), 4);
        assert_eq!(m1.max_capacity(), 4);
        assert!(m1.try_recv().is_err());
        assert!(!m1.is_closed());

        // len reflects pending messages
        a1.try_do_send(Ping(1))?;
        a1.try_do_send(Ping(2))?;
        assert_eq!(m1.len(), 2);
        assert!(!m1.is_empty());

        // close() propagates to the address and rejects further sends
        assert!(!a1.is_closed());
        m1.close();
        assert!(a1.is_closed());
        let result = a1.try_do_send(Ping(3));
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_recipient() -> Result<()> {
        // create() delivers to the receiver
        let (recipient, mut rx) = Recipient::<Ping>::create(4);
        recipient.do_send(Ping(1)).await?;
        let msg = rx.recv().await?;
        assert_eq!(msg.0, 1);

        // clone preserves identity
        let clone = recipient.clone();
        assert_eq!(recipient, clone);
        assert_eq!(recipient.index(), clone.index());

        // capacity + is_closed + closed
        assert!(!recipient.is_closed());
        assert_eq!(recipient.capacity(), 4);
        drop(rx);
        assert!(recipient.is_closed());
        timeout(Duration::from_millis(500), recipient.closed())
            .await
            .context("closed() should resolve after receiver drop")?;

        // send functions with create() recipient
        let (recipient, rx) = Recipient::<Ping>::create(8);
        recipient.send(Ping(2)).await?;
        recipient.do_send(Ping(3)).await?;
        recipient.try_send(Ping(4))?;
        recipient.try_do_send(Ping(5))?;
        recipient
            .send_timeout(Ping(6), Duration::from_millis(10))
            .await?;
        recipient
            .do_send_timeout(Ping(7), Duration::from_millis(10))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            recipient.blocking_send(Ping(8))?;
            recipient.blocking_do_send(Ping(9))?;
            Ok(())
        })
        .await??;
        assert_eq!(rx.len(), 8);

        // From<Address> preserves index
        let (a1, m1) = make_address(8);
        let index = a1.index();
        let recipient: Recipient<Ping> = a1.into();
        assert_eq!(recipient.index(), index);

        let clone = recipient.clone();
        assert_eq!(recipient, clone);
        assert_eq!(recipient.index(), clone.index());

        // send functions with From<Address> recipient
        recipient.send(Ping(10)).await?;
        recipient.do_send(Ping(11)).await?;
        recipient.try_send(Ping(12))?;
        recipient.try_do_send(Ping(13))?;
        recipient
            .send_timeout(Ping(14), Duration::from_millis(10))
            .await?;
        recipient
            .do_send_timeout(Ping(15), Duration::from_millis(10))
            .await?;
        tokio::task::spawn_blocking(move || -> Result<()> {
            recipient.blocking_send(Ping(16))?;
            recipient.blocking_do_send(Ping(17))?;
            Ok(())
        })
        .await??;
        assert_eq!(m1.len(), 8);

        assert!(!clone.is_closed());
        drop(m1);
        assert!(clone.is_closed());
        timeout(Duration::from_millis(100), clone.closed())
            .await
            .context("closed() should resolve after mailbox drop")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_permits() -> Result<()> {
        // reserve
        let (a1, m1) = make_address(2);
        let permit = a1.reserve().await?;
        permit.do_send(Ping(1));
        let permit = a1.try_reserve()?;
        permit.send(Ping(2));
        assert_eq!(m1.len(), 2);

        // capacity
        let (a1, m1) = make_address(2);
        let p1 = a1.reserve().await?;
        let _p2 = a1.reserve().await?;
        let result = a1.try_reserve();
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );

        // drop a permit releases the slot
        drop(p1);
        let _ = a1.try_reserve()?;

        // close
        drop(m1);
        let result = a1.try_reserve();
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        // reserve_owned
        let (a1, m1) = make_address(2);
        let permit = a1.reserve_owned().await?;
        permit.do_send(Ping(2));
        let permit = a1.try_reserve_owned()?;
        permit.send(Ping(3));
        assert_eq!(m1.len(), 2);

        // capacity
        let (a1, m1) = make_address(2);
        let p1 = a1.reserve_owned().await?;
        let _p2 = a1.reserve_owned().await?;
        let result = a1.try_reserve_owned();
        assert!(
            matches!(result, Err(SendError::Full(_))),
            "expected Full, got {result:?}"
        );

        // drop a permit releases the slot
        drop(p1);
        let _ = a1.try_reserve_owned()?;

        // close
        drop(m1);
        let result = a1.try_reserve_owned();
        assert!(
            matches!(result, Err(SendError::Closed(_))),
            "expected Closed, got {result:?}"
        );

        Ok(())
    }

    #[test]
    fn test_sender() {
        let sender_id = crate::actor::ActorId::new(42);
        assert_eq!(sender_id.index(), sender_id);

        #[cfg(feature = "ipc")]
        {
            let sender_id =
                crate::actor::ActorId::new_remote(42, std::num::NonZeroU64::new(1).unwrap());
            assert_eq!(sender_id.is_remote(), true);
        }
    }

    #[test]
    fn test_debug_fmt() {
        let (address, mailbox) = make_address(4);
        assert_eq!(
            format!("{address:?}"),
            format!("Address<Dummy>({})", address.index())
        );
        assert_eq!(format!("{mailbox:?}"), "Mailbox<Dummy>");

        let (recipient, _rx) = Recipient::<Ping>::create(4);
        assert_eq!(
            format!("{recipient:?}"),
            format!("Recipient<Ping>({})", recipient.index())
        );
    }
}

//! Traits and type definitions for the address of actor model.
//!
//! In the actor model, an [`Address`] is a handle to an actor. It is the only way to interact
//! with an actor since the runtime will take the ownership of the actor itself after it is
//! spawned.
//!
//! This modules defines the [`Address`] type for an actor. It also provides the [`Sender`] trait
//! and a [`Recipient`] type which are alternative ways to organize the addresses of actors.
//!

mod address_impl;
pub use address_impl::Address;

mod permit;
pub use permit::{OwnedSendPermit, SendPermit};

mod sender;
pub use sender::{
    ClosedResultFuture, DoSendResult, DoSendResultFuture, SendResult, SendResultFuture, Sender,
    SenderId,
};

mod recipient;
pub use recipient::Recipient;

mod mailbox;
pub use mailbox::Mailbox;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::*;
    use crate::actor::Actor;
    use crate::channel::mpsc;
    use crate::context::Context;
    use crate::envelope::Envelope;
    use crate::errors::SendError;
    use crate::message::{Handler, Message};

    #[derive(Debug)]
    struct Dummy;

    impl Actor for Dummy {
        type Context = Context<Self>;
        type Error = anyhow::Error;
    }

    #[derive(Debug)]
    struct Ping(u32);

    impl Message for Ping {
        type Result = ();
    }

    impl Handler<Ping> for Dummy {
        type Result = ();

        async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) -> Self::Result {}
    }

    fn make_address(capacity: usize) -> (Address<Dummy>, Mailbox<Dummy>) {
        let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
        (Address::new(tx), Mailbox::new(rx))
    }

    #[tokio::test]
    async fn test_address() {
        // basic properties
        let (a1, _) = make_address(4);
        let clone = a1.clone();
        assert_eq!(a1, clone);
        assert_eq!(a1.index(), clone.index());

        #[allow(clippy::mutable_key_type)]
        let mut map = HashSet::new();
        map.insert(a1);
        map.insert(clone);
        assert_eq!(
            map.len(),
            1,
            "clones should have the same hash and be equal"
        );

        // distinct addresses differ
        let (a1, m1) = make_address(4);
        let (a2, _) = make_address(4);
        assert_ne!(a1, a2);
        assert_ne!(a1.index(), a2.index());

        // capacity + is_closed + closed() future + mailbox delivery
        assert_eq!(a1.capacity(), 4);
        assert!(!a1.is_closed());

        let closed = a1.closed();
        drop(m1);
        assert!(a1.is_closed());
        tokio::time::timeout(Duration::from_secs(1), closed)
            .await
            .expect("closed() did not resolve after mailbox drop");

        // do_send
        let (a1, mut m1) = make_address(1);
        a1.do_send(Ping(42)).await.expect("do_send should succeed");
        assert_eq!(m1.len(), 1);
        m1.recv().await.expect("mailbox should receive an envelope");

        // try_do_send
        a1.try_do_send(Ping(1)).expect("first message fits");
        match a1.try_do_send(Ping(2)) {
            Err(SendError::Full(_)) => {}
            other => panic!("expected Full, got {other:?}"),
        }

        // do_send_timeout
        match a1.do_send_timeout(Ping(3), Duration::from_millis(10)).await {
            Err(SendError::Timeout(_)) => {}
            other => panic!("expected Timeout, got {other:?}"),
        }

        // try_do_send returns Closed after mailbox drop
        let (a1, m1) = make_address(4);
        drop(m1);
        match a1.try_do_send(Ping(1)) {
            Err(SendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_mailbox() {
        // empty accessors
        let (_tx, rx) = mpsc::channel::<Envelope<Dummy>>(4);
        let mut m1 = Mailbox::<Dummy>::new(rx);
        assert!(m1.is_empty());
        assert_eq!(m1.len(), 0);
        assert_eq!(m1.capacity(), 4);
        assert_eq!(m1.max_capacity(), 4);
        assert!(m1.try_recv().is_err());
        assert!(!m1.is_closed());

        // len reflects pending messages
        let (a1, m1) = make_address(4);
        a1.try_do_send(Ping(1)).unwrap();
        a1.try_do_send(Ping(2)).unwrap();
        assert_eq!(m1.len(), 2);
        assert!(!m1.is_empty());

        // close() propagates to the address and rejects further sends
        let (a1, mut m1) = make_address(4);
        assert!(!a1.is_closed());
        m1.close();
        assert!(a1.is_closed());
        match a1.try_do_send(Ping(1)) {
            Err(SendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recipient() {
        // create() delivers to the receiver
        let (recipient, mut rx) = Recipient::<Ping>::create(4);
        recipient.do_send(Ping(7)).await.expect("do_send");
        let msg = rx.recv().await.expect("recv");
        assert_eq!(msg.0, 7);

        // clone preserves identity
        let clone = recipient.clone();
        assert_eq!(recipient, clone);
        assert_eq!(recipient.index(), clone.index());

        // try_send and try_do_send
        let (recipient, rx) = Recipient::<Ping>::create(2);
        let _ = recipient.try_send(Ping(1)).expect("first fits").await;
        recipient.try_do_send(Ping(2)).expect("second fits");
        match recipient.try_do_send(Ping(3)) {
            Err(SendError::Full(_)) => {}
            other => panic!("expected Full, got {other:?}"),
        }
        drop(rx);
        match recipient.try_do_send(Ping(3)) {
            Err(SendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }

        // send_timeout and do_send_timeout
        let (recipient, _rx) = Recipient::<Ping>::create(2);
        let _ = recipient
            .send_timeout(Ping(1), Duration::from_millis(10))
            .await
            .expect("first fits")
            .await;
        recipient
            .do_send_timeout(Ping(2), Duration::from_millis(10))
            .await
            .expect("second fits");
        match recipient
            .do_send_timeout(Ping(3), Duration::from_millis(10))
            .await
        {
            Err(SendError::Timeout(_)) => {}
            other => panic!("expected Timeout, got {other:?}"),
        }

        // From<Address> preserves index
        let (a1, _) = make_address(4);
        let index = a1.index();
        let recipient: Recipient<Ping> = a1.into();
        assert_eq!(recipient.index(), index);
    }

    #[tokio::test]
    async fn test_permits() {
        // SendPermit
        let (a1, mut m1) = make_address(2);
        let permit = a1.reserve().await.expect("reserve");
        permit.do_send(Ping(1));
        assert_eq!(m1.len(), 1);
        m1.recv().await.expect("envelope delivered");

        // reservations consume capacity
        let (a1, _m1) = make_address(2);
        let p1 = a1.reserve().await.expect("first reserve");
        let p2 = a1.reserve().await.expect("second reserve");
        match a1.try_reserve() {
            Err(SendError::Full(())) => {}
            other => panic!("expected Full, got {other:?}"),
        }

        // drop a permit releases the slot
        drop(p1);
        a1.try_reserve().expect("slot freed after permit drop");
        p2.send(Ping(2));

        // try_reserve returns Closed after mailbox drop
        let (a1, m1) = make_address(4);
        drop(m1);
        match a1.try_reserve() {
            Err(SendError::Closed(())) => {}
            other => panic!("expected Closed, got {other:?}"),
        }

        // OwnedSendPermit
        let (a1, mut m1) = make_address(2);
        let permit = a1.reserve_owned().await.expect("reserve_owned");
        permit.do_send(Ping(2));
        assert_eq!(m1.len(), 1);
        m1.recv().await.expect("envelope delivered");

        // try_reserve_owned returns Full when no capacity
        let (a1, m1) = make_address(1);
        let owned = a1.try_reserve_owned().expect("first reserve");
        match a1.try_reserve_owned() {
            Err(SendError::Full(())) => {}
            other => panic!("expected Full, got {other:?}"),
        }
        owned.send(Ping(3));
        drop(m1);
        match a1.try_reserve_owned() {
            Err(SendError::Closed(())) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
    }
}

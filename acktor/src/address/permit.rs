use crate::actor::Actor;
use crate::channel::{mpsc, oneshot};
use crate::envelope::{Envelope, IntoEnvelope};
use crate::message::Message;

/// Permit to send one message to an actor.
#[derive(Debug)]
pub struct SendPermit<'a, A>
where
    A: Actor,
{
    pub(super) permit: mpsc::Permit<'a, Envelope<A>>,
}

impl<A> SendPermit<'_, A>
where
    A: Actor,
{
    /// Sends a message using the permit and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    ///
    /// This method will consume the permit.
    pub fn send<M, EP>(self, msg: M) -> oneshot::Receiver<M::Result>
    where
        M: Message + IntoEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.permit.send(msg.pack(Some(tx)));
        rx
    }

    /// Sends a message using the permit without expecting a response.
    ///
    /// This method will consume the permit.
    pub fn do_send<M, EP>(self, msg: M)
    where
        M: Message + IntoEnvelope<A, EP>,
    {
        self.permit.send(msg.pack(None));
    }
}

/// Owned permit to send one message to an actor.
#[derive(Debug)]
pub struct OwnedSendPermit<A>
where
    A: Actor,
{
    pub(super) permit: mpsc::OwnedPermit<Envelope<A>>,
}

impl<A> OwnedSendPermit<A>
where
    A: Actor,
{
    /// Sends a message using the permit and returns a
    /// [`Receiver`][crate::channel::oneshot::Receiver] which can be used to receive the message
    /// response.
    ///
    /// This method will consume the permit.
    pub fn send<M, EP>(self, msg: M) -> oneshot::Receiver<M::Result>
    where
        M: Message + IntoEnvelope<A, EP>,
    {
        let (tx, rx) = oneshot::channel();
        self.permit.send(msg.pack(Some(tx)));
        rx
    }

    /// Sends a message using the permit without expecting a response.
    ///
    /// This method will consume the permit.
    pub fn do_send<M, EP>(self, msg: M)
    where
        M: Message + IntoEnvelope<A, EP>,
    {
        self.permit.send(msg.pack(None));
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use pretty_assertions::assert_eq;

    use crate::error::SendError;
    use crate::test_utils::{Ping, make_address};

    #[tokio::test]
    async fn test_permit() -> Result<()> {
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
}

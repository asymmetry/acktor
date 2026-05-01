use std::ops::Deref;

use super::Actor;
use crate::address::{Address, Recipient};
use crate::codec::HasCodecTable;
use crate::message::{EncodedMessage, Handler};
use crate::stable_type_id::HasStableTypeId;

/// An actor which can be reached from other processes.
///
/// To make an actor reachable, the user should implement the [`HasCodecTable`] trait and the
/// [`Handler<EncodedMessage>`] trait for it.
///
/// The [`HasCodecTable`] trait provides a [`CodecTable`]. If an user wants to send a message to
/// an actor running in another process, the table is required to encode the message to bytes.
/// Each message type that can be handled by the actor should have a corresponding entry in the
/// table, which describes how to encode the message and decode the message response. Usually
/// the table is saved in the [`Address`][crate::address::Address] of a capable actor running in
/// another process. See [`Address`][crate::address::Address] for more details.
///
/// The [`Handler<EncodedMessage>`] trait defines how an actor processes inbound messages sent
/// from other processes. The actor should decode the message bytes to a concrete message type,
/// handle it, encode the message response to bytes and send the response back to the sender.
pub trait RemoteAccessible:
    Actor + HasCodecTable + Handler<EncodedMessage> + HasStableTypeId
{
}

/// A handle to an actor which can be reached from other processes.
///
/// It contains the address of the actor as a [`Recipient<EncodedMessage>`] so that it can be
/// used to send inbound [`EncodedMessage`]s to the actor.
pub struct RemoteAccessibleActorHandle(Recipient<EncodedMessage>);

impl Clone for RemoteAccessibleActorHandle {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Deref for RemoteAccessibleActorHandle {
    type Target = Recipient<EncodedMessage>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A> From<Address<A>> for RemoteAccessibleActorHandle
where
    A: Actor + RemoteAccessible,
{
    fn from(addr: Address<A>) -> Self {
        Self::new(addr.into())
    }
}

impl RemoteAccessibleActorHandle {
    /// Creates a new [`RemoteAccessibleActorHandle`] from a [`Recipient<EncodedMessage>`].
    pub fn new(recipient: Recipient<EncodedMessage>) -> Self {
        Self(recipient)
    }

    /// Returns the [`Recipient<EncodedMessage>`] contained in this handle.
    pub fn into_inner(self) -> Recipient<EncodedMessage> {
        self.0
    }
}

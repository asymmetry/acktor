use super::{Actor, JoinHandle};
use crate::address::Address;
use crate::codec::Codec;
use crate::message::{BinaryMessage, Handler};
use crate::stable_type_id::StableId;

/// An actor which can be reached from other processes.
///
/// To make an actor reachable, the user should implement [`Codec`] trait,
/// [`Handler<BinaryMessage>`] trait and [`StableId`] trait for it.
///
/// The [`Codec`] trait provides a [`CodecTable`][crate::codec::CodecTable], which is mandatory
/// when sending messages to a remote addressable actor from other processes. Each message type
/// that can be handled by the actor should have a corresponding entry in the table, which
/// describes how to encode the message and decode the message response. A copy of this table is
/// stored in the [`Address`][crate::address::Address] of a remote addressable actor. See
/// [`Address`][crate::address::Address] for more details on how the table is used to encode
/// messages.
///
/// The [`Handler<BinaryMessage>`] trait defines how a remote addressible actor handles inbound
/// binary messages received from other processes. The actor should decode the byte array in the
/// binary message to a concrete message type, handle it with the corresponding handler, and
/// encode the message response (if any) to bytes and send it back to the sender.
///
/// The [`StableId`] trait gives the actor a stable unique type identifier. This is required by
/// message types which has type generic parameters (e.g.
/// [`SupervisonEvent<A>`][crate::supervisor::SupervisionEvent]) to receive a message id by
/// deriving the [`MessageId`][crate::message::MessageId] trait.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(RemoteAddressable)]`][acktor_derive::RemoteAddressable] together with the
/// [`#[remote]`][acktor_derive::remote] attribute macro.
///
/// [`#[derive(RemoteAddressable)]`][acktor_derive::RemoteAddressable] will
/// emit implementations for all three required supertraits as well as the marker trait itself.
///
/// The [`#[remote]`][acktor_derive::remote] attribute macro is used to annotate the
/// `impl Actor for MyActor` block to override the `remote_mailbox` method. This internal method
/// is used to return a [`RemoteMailbox`] for a remote addressable actor. It is a temporary
/// workaround since specialization is not yet stable in Rust.
pub trait RemoteAddressable: Actor + Codec + Handler<BinaryMessage> + StableId {}

/// An actor type which can be created from another process.
pub trait RemoteSpawnable: RemoteAddressable {
    /// Creates an instance of this actor with the given label and config string.
    ///
    /// Implementations typically call [`Actor::start`] or [`Actor::create`] internally.
    fn create_remote(
        label: String,
        config: String,
    ) -> Result<(Address<Self>, JoinHandle<()>), <Self as Actor>::Error>;
}

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

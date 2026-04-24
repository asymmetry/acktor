//! Traits for actors which can be reached over IPC and a registry of this kind of actors.
//!

use std::sync::Arc;

use acktor::{Actor, Handler};
use ahash::HashMap;

use crate::remote_message::RemoteMessage;

mod registry;
pub use registry::RemoteActorRegistry;

mod factory;
pub use factory::RemoteActorFactory;
pub(crate) use factory::{DynRemoteActorFactory, RemoteActorShim};

pub(crate) type RemoteActorFactoryRegistry = HashMap<String, Arc<dyn DynRemoteActorFactory>>;

/// A marker trait for actors which can be reached over IPC.
///
/// To make an actor reachable over IPC, the user should implement the [`Handler<RemoteMessage>`]
/// trait for an actor to define how it processes inbound remote messages, and then register the
/// actor's address in the [`Node`][crate::Node]'s [`RemoteActorRegistry`] via the
/// [`with_actor`][crate::Node::with_actor] method or the
/// [`AddActor`][crate::node::command::AddActor] command.
///
/// It is highly recommended to use the [`#[acktor_ipc::remote]`][crate::remote] attribute macro
/// to annotate the `impl Actor for MyActor { ... }` block of a remote actor. The macro overrides
/// the [`Actor::type_erased_recipient_fn`] hook, which will opt-in the
/// `type-erased-recipient-hook` feature, and allow conversion from any `Recipient` type of this
/// actor to a `Recipient<RemoteMessage>`. With the help of this hook, the actor's address can be
/// registered in the [`Node`][crate::Node]'s [`RemoteActorRegistry`] automatically. This is more
/// convenient than registering the actor manually.
pub trait RemoteActor: Actor + Handler<RemoteMessage> {}

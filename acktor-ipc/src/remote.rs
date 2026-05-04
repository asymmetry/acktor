use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use acktor::utils::NopHasher;

pub use acktor::{RemoteAddressable, RemoteSpawnable};
pub(crate) use acktor::{RemoteProxy, address::RemoteMailbox, message::BinaryMessage};

mod factory;
pub(crate) use factory::{RemoteFactory, RemoteFactoryShim};

mod registry;
pub(crate) use registry::RemoteMailboxRegistry;

pub(crate) type RemoteFactoryRegistry =
    HashMap<u64, Arc<dyn RemoteFactory>, BuildHasherDefault<NopHasher>>;

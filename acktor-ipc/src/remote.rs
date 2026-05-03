use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use acktor::utils::NopHasher;

pub use acktor::actor::{RemoteAddressable, RemoteMailbox};

mod factory;
pub use factory::RemoteSpawnable;
pub(crate) use factory::{DynRemoteSpawnable, RemoteSpawnableShim};

mod registry;
pub use registry::RemoteMailboxRegistry;

pub(crate) type ActorFactoryRegistry =
    HashMap<u64, Arc<dyn DynRemoteSpawnable>, BuildHasherDefault<NopHasher>>;

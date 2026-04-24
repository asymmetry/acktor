use std::fmt::{self, Debug};
use std::sync::Arc;

use ahash::RandomState;
use dashmap::{DashMap, Entry};

use acktor::{Recipient, Sender, SenderId};

use crate::remote_message::RemoteMessage;

/// A registry of actors in the current process which can receive [`RemoteMessage`]s.
///
/// The registry is a cheaply-cloneable concurrent map shared between a [`Node`][crate::Node]
/// and all of its sessions. It lets sessions route inbound [`RemoteMessage`]s to the proper
/// actor in the same process.
///
/// An actor which implements the [`RemoteActor`][super::RemoteActor] trait will be
/// automatically registered in a [`Node`][crate::Node]'s registry when its address is encoded
/// as a [`RemoteMessage`] for the first time. Users can also manually register an actor with
/// a [`Node`][crate::Node]'s [`AddActor`][crate::node::command::AddActor] command.
#[derive(Clone, Default)]
pub struct RemoteActorRegistry {
    inner: Arc<DashMap<u64, Recipient<RemoteMessage>, RandomState>>,
}

impl Debug for RemoteActorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for entry in self.inner.iter() {
            list.entry(entry.key());
        }
        list.finish()
    }
}

impl RemoteActorRegistry {
    /// Constructs a new empty [`RemoteActorRegistry`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a new empty [`RemoteActorRegistry`] with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity_and_hasher(
                capacity,
                RandomState::new(),
            )),
        }
    }

    /// Returns the number of actors the registry can hold without reallocating.
    ///
    /// **Locking behavior:** May deadlock if called when holding a mutable reference into the
    /// registry.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns the number of actors currently in the registry.
    ///
    /// **Locking behavior:** May deadlock if called when holding a mutable reference into the
    /// registry.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the registry contains no actors.
    ///
    /// **Locking behavior:** May deadlock if called when holding a mutable reference into the
    /// registry.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Retains only the actors for which the `predicate` returns `true`.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn retain<F>(&self, mut predicate: F)
    where
        F: FnMut(u64, &Recipient<RemoteMessage>) -> bool,
    {
        self.inner
            .retain(|index, recipient| predicate(*index, recipient));
    }

    /// Returns an actor as a `Recipient<RemoteMessage>` corresponding to the given index.
    ///
    /// If the actor's mailbox is closed, this method removes it and returns `None`. This is
    /// different from the behavior of `get` on a normal `HashMap`.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn get(&self, index: u64) -> Option<Recipient<RemoteMessage>> {
        match self.inner.entry(index) {
            Entry::Occupied(entry) => {
                let recipient = entry.get();
                if recipient.is_closed() {
                    entry.remove();
                    None
                } else {
                    Some(recipient.clone())
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Returns `true` if the registry contains an actor for the given index.
    ///
    /// **Locking behavior:** May deadlock if called when holding a mutable reference into the
    /// registry.
    pub fn contains_index(&self, index: u64) -> bool {
        self.inner.contains_key(&index)
    }

    /// Removes an actor by its index, returning the actor as a `Recipient<RemoteMessage>` if it
    /// was present in the registry.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn remove(&self, index: u64) -> Option<Recipient<RemoteMessage>> {
        self.inner.remove(&index).map(|(_, recipient)| recipient)
    }

    /// Inserts a new actor keyed by its index.
    ///
    /// If the registry did not have this actor present, `true` is returned. If the registry did
    /// have this actor present, `false` is returned and the registry is not modified. This is
    /// different from the behavior of `insert` on a normal `HashMap`.
    ///
    /// Re-registering the same address is a cheap lookup-no-op.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn insert<A>(&self, actor: A) -> bool
    where
        A: Into<Recipient<RemoteMessage>> + SenderId,
    {
        let index = actor.index();
        match self.inner.entry(index) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(actor.into());
                true
            }
        }
    }
}

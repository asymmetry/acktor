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

    /// Inserts an actor keyed by its index. Returns `true` if a new entry was inserted,
    /// or `false` if the registry already contained an actor with the same index.
    ///
    /// Re-registering the same address is a cheap lookup-no-op.
    pub fn insert<A>(&self, actor: A) -> bool
    where
        A: Into<Recipient<RemoteMessage>>,
    {
        let recipient = actor.into();
        let index = recipient.index();
        match self.inner.entry(index) {
            Entry::Vacant(e) => {
                e.insert(recipient);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Removes and returns the actor with the given index or address, if any.
    pub fn remove(&self, index: u64) -> Option<Recipient<RemoteMessage>> {
        self.inner.remove(&index).map(|(_, recipient)| recipient)
    }

    /// Looks up an actor by index.
    ///
    /// If the stored actor's mailbox is closed, this method removes
    /// the stale entry and returns `None`.
    pub fn get(&self, index: u64) -> Option<Recipient<RemoteMessage>> {
        let recipient = self.inner.get(&index)?;
        if recipient.is_closed() {
            self.inner.remove(&index);
            return None;
        }
        Some(recipient.clone())
    }

    /// Returns `true` if the registry contains an entry for the given index.
    pub fn contains(&self, index: u64) -> bool {
        self.inner.contains_key(&index)
    }

    /// Returns the number of entries currently in the registry.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns the number of entries the registry can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns `true` if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Retains only the entries for which `predicate` returns `true`.
    pub fn retain<F>(&self, mut predicate: F)
    where
        F: FnMut(u64, &Recipient<RemoteMessage>) -> bool,
    {
        self.inner
            .retain(|index, recipient| predicate(*index, recipient));
    }
}

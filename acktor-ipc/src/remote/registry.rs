use std::fmt::{self, Debug};
use std::sync::Arc;

use dashmap::{DashMap, Entry};

use acktor::SenderInfo;

use super::RemoteMailbox;

/// A registry of actors which are [`RemoteAddressable`][super::RemoteAddressable].
///
/// The registry is a cheaply-cloneable concurrent map shared between a [`Node`][crate::Node]
/// and all of its sessions. It lets sessions route inbound
/// [`BinaryMessage`][super::BinaryMessage]s to the proper actor.
///
/// An actor which implements the [`RemoteAddressable`][super::RemoteAddressable] trait will be
/// registered in a [`Node`][crate::Node]'s registry when its address is encoded as a
/// [`BinaryMessage`][super::BinaryMessage] for the first time. Remote addressable actors can also
/// be registered with the [`AddActor`][crate::node::command::AddActor] command of a
/// [`Node`][crate::Node].
#[derive(Clone, Default)]
pub struct RemoteMailboxRegistry {
    inner: Arc<DashMap<u64, RemoteMailbox, ahash::RandomState>>,
}

impl Debug for RemoteMailboxRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for entry in self.inner.iter() {
            list.entry(entry.key());
        }
        list.finish()
    }
}

#[allow(dead_code)]
impl RemoteMailboxRegistry {
    /// Constructs a new empty [`RemoteMailboxRegistry`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a new empty [`RemoteMailboxRegistry`] with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::with_capacity_and_hasher(
                capacity,
                ahash::RandomState::new(),
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
        F: FnMut(u64, &RemoteMailbox) -> bool,
    {
        self.inner
            .retain(|index, recipient| predicate(*index, recipient));
    }

    /// Returns an actor as a [`RemoteMailbox`] corresponding to the given index.
    ///
    /// If the actor's mailbox is closed, this method removes it and returns `None`. This is
    /// different from the behavior of `get` on a normal `HashMap`.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn get(&self, index: u64) -> Option<RemoteMailbox> {
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
    /// If the actor's mailbox is closed, this method removes it and returns `false`.
    ///
    /// **Locking behavior:** May deadlock if called when holding a mutable reference into the
    /// registry.
    pub fn contains_index(&self, index: u64) -> bool {
        match self.inner.entry(index) {
            Entry::Occupied(entry) => {
                if entry.get().is_closed() {
                    entry.remove();
                    false
                } else {
                    true
                }
            }
            Entry::Vacant(_) => false,
        }
    }

    /// Removes an actor by its index, returning the actor as a [`RemoteMailbox`] if
    /// it was present in the registry.
    ///
    /// **Locking behavior:** May deadlock if called when holding any sort of reference into the
    /// registry.
    pub fn remove(&self, index: u64) -> Option<RemoteMailbox> {
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
    pub fn insert(&self, actor: RemoteMailbox) -> bool {
        let index = actor.index().as_local();
        match self.inner.entry(index) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(actor);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use acktor::{Recipient, channel::mpsc, message::BinaryMessage};

    use pretty_assertions::assert_eq;

    use super::*;

    fn make_mailbox() -> (RemoteMailbox, mpsc::Receiver<BinaryMessage>) {
        Recipient::<BinaryMessage>::create(4)
    }

    #[test]
    fn test_registry() {
        // new() / default
        let registry = RemoteMailboxRegistry::new();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());

        // with_capacity reserves space
        let sized = RemoteMailboxRegistry::with_capacity(64);
        assert!(sized.capacity() >= 64);
        assert!(sized.is_empty());

        // insert: first insert returns true, duplicate returns false (no-op)
        let (mailbox, _rx) = make_mailbox();
        let index = mailbox.index().as_local();
        assert!(registry.insert(mailbox.clone()));
        assert_eq!(registry.len(), 1);
        assert!(!registry.insert(mailbox));
        assert_eq!(registry.len(), 1);

        // get / contains_index find the inserted mailbox
        assert!(registry.contains_index(index));
        let fetched = registry.get(index).expect("mailbox should be present");
        assert_eq!(fetched.index().as_local(), index);

        // remove takes it out and is idempotent
        let removed = registry.remove(index).expect("mailbox should be removed");
        assert_eq!(removed.index().as_local(), index);
        assert!(registry.is_empty());
        assert!(registry.remove(index).is_none());
        assert!(registry.get(index).is_none());
    }

    #[test]
    fn test_cleanup_closed() {
        let registry = RemoteMailboxRegistry::new();

        // get() evicts a closed mailbox
        let (mailbox, rx) = make_mailbox();
        let index = mailbox.index().as_local();
        registry.insert(mailbox);
        drop(rx);
        assert!(registry.get(index).is_none());
        assert_eq!(registry.len(), 0);

        // contains_index() evicts a closed mailbox
        let (mailbox, rx) = make_mailbox();
        let index = mailbox.index().as_local();
        registry.insert(mailbox);
        drop(rx);
        assert!(!registry.contains_index(index));
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_clone_and_retain() {
        let registry = RemoteMailboxRegistry::new();
        let clone = registry.clone();

        let (m1, _r1) = make_mailbox();
        let (m2, _r2) = make_mailbox();
        let i1 = m1.index().as_local();
        let i2 = m2.index().as_local();
        registry.insert(m1);
        registry.insert(m2);

        // clone observes inserts through the shared map
        assert_eq!(clone.len(), 2);
        assert!(clone.contains_index(i1));
        assert!(clone.contains_index(i2));

        // retain filters in place, visible through both handles
        clone.retain(|index, _| index == i1);
        assert_eq!(registry.len(), 1);
        assert!(registry.contains_index(i1));
        assert!(!registry.contains_index(i2));
    }

    #[test]
    fn test_debug_fmt() {
        let registry = RemoteMailboxRegistry::new();
        assert_eq!(format!("{:?}", registry), "[]");

        let (mailbox, _rx) = make_mailbox();
        let index = mailbox.index().as_local();
        registry.insert(mailbox);

        assert_eq!(format!("{:?}", registry), format!("[{}]", index));
    }
}

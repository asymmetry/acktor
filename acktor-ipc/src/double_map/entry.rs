//! The [`Entry`] API for in-place manipulation of a [`DoubleMap`][super::DoubleMap].
//!
//! Mirrors [`std::collections::hash_map::Entry`] as closely as possible, adapted to
//! the two-key shape of `DoubleMap`.

use std::collections::hash_map;
use std::mem;

/// A view into a single entry in a [`DoubleMap`][super::DoubleMap], which may be
/// either vacant or occupied.
pub enum Entry<'a, K1, K2, V> {
    /// Both provided keys identify the same existing entry.
    Occupied(OccupiedEntry<'a, K1, K2, V>),
    /// Neither of the provided keys is present — a new entry can be inserted.
    Vacant(VacantEntry<'a, K1, K2, V>),
}

impl<K1, K2, V> Entry<'_, K1, K2, V> {
    /// Returns a reference to the `K1` key of this entry.
    pub fn key1(&self) -> &K1 {
        match self {
            Entry::Occupied(e) => e.key1(),
            Entry::Vacant(e) => e.key1(),
        }
    }

    /// Returns a reference to the `K2` key of this entry.
    pub fn key2(&self) -> &K2 {
        match self {
            Entry::Occupied(e) => e.key2(),
            Entry::Vacant(e) => e.key2(),
        }
    }

    /// Returns a reference to the keys of type `K1` and `K2` of this entry.
    pub fn keys(&self) -> (&K1, &K2) {
        match self {
            Entry::Occupied(e) => e.keys(),
            Entry::Vacant(e) => e.keys(),
        }
    }
}

impl<'a, K1, K2, V> Entry<'a, K1, K2, V>
where
    K1: Clone,
    K2: Clone,
{
    /// Ensures a value is in the entry by inserting `default` if vacant, and returns
    /// a mutable reference to the value.
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default),
        }
    }

    /// Ensures a value is in the entry by inserting the result of `default` if
    /// vacant, and returns a mutable reference to the value.
    pub fn or_insert_with<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce() -> V,
    {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default()),
        }
    }

    /// Ensures a value is in the entry by inserting the result of `default` if
    /// vacant, and returns a mutable reference to the value. The `default` closure
    /// receives references to the keys of the entry.
    pub fn or_insert_with_keys<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce(&K1, &K2) -> V,
    {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let (key1, key2) = e.keys();
                let v = default(key1, key2);
                e.insert(v)
            }
        }
    }

    /// Provides in-place mutation of an occupied entry before any potential
    /// insertion, then returns the entry unchanged for further chaining.
    pub fn and_modify<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        if let Entry::Occupied(e) = &mut self {
            f(e.get_mut());
        }
        self
    }

    /// Sets the value of the entry, and returns an `OccupiedEntry`.
    pub fn insert_entry(self, value: V) -> OccupiedEntry<'a, K1, K2, V> {
        match self {
            Entry::Occupied(mut e) => {
                e.insert(value);
                e
            }
            Entry::Vacant(e) => e.insert_entry(value),
        }
    }

    /// Ensures a value is in the entry by inserting the default value if vacant.
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        self.or_insert_with(V::default)
    }
}

/// A view into a vacant entry of a [`DoubleMap`][super::DoubleMap].
pub struct VacantEntry<'a, K1, K2, V> {
    primary: hash_map::VacantEntry<'a, K1, (K2, V)>,
    secondary: hash_map::VacantEntry<'a, K2, K1>,
}

impl<'a, K1, K2, V> VacantEntry<'a, K1, K2, V> {
    pub(super) fn new(
        primary: hash_map::VacantEntry<'a, K1, (K2, V)>,
        secondary: hash_map::VacantEntry<'a, K2, K1>,
    ) -> Self {
        Self { primary, secondary }
    }
}

impl<K1, K2, V> VacantEntry<'_, K1, K2, V> {
    /// Returns a reference to the `K1` key that would be inserted.
    pub fn key1(&self) -> &K1 {
        self.primary.key()
    }

    /// Returns a reference to the `K2` key that would be inserted.
    pub fn key2(&self) -> &K2 {
        self.secondary.key()
    }

    /// Returns a reference to the keys of type `K1` and `K2` that would be inserted.
    pub fn keys(&self) -> (&K1, &K2) {
        (self.key1(), self.key2())
    }

    /// Consumes this entry and returns the `(K1, K2)` keys it was holding.
    pub fn into_keys(self) -> (K1, K2) {
        (self.primary.into_key(), self.secondary.into_key())
    }
}

impl<'a, K1, K2, V> VacantEntry<'a, K1, K2, V>
where
    K1: Clone,
    K2: Clone,
{
    /// Sets the value of the entry with the VacantEntry’s key, and returns a mutable
    /// reference to it.
    pub fn insert(self, value: V) -> &'a mut V {
        let key1 = self.primary.key().clone();
        let key2 = self.secondary.key().clone();
        self.secondary.insert(key1);
        let (_, v) = self.primary.insert((key2, value));
        v
    }

    /// Sets the value of the entry with the VacantEntry’s key, and returns an
    /// OccupiedEntry.
    pub fn insert_entry(self, value: V) -> OccupiedEntry<'a, K1, K2, V> {
        let key1 = self.primary.key().clone();
        let key2 = self.secondary.key().clone();
        let occupied_secondary = self.secondary.insert_entry(key1.clone());
        let occupied_primary = self.primary.insert_entry((key2.clone(), value));
        OccupiedEntry {
            primary: occupied_primary,
            secondary: occupied_secondary,
        }
    }
}

/// A view into an occupied entry of a [`DoubleMap`][super::DoubleMap].
pub struct OccupiedEntry<'a, K1, K2, V> {
    primary: hash_map::OccupiedEntry<'a, K1, (K2, V)>,
    secondary: hash_map::OccupiedEntry<'a, K2, K1>,
}

impl<'a, K1, K2, V> OccupiedEntry<'a, K1, K2, V> {
    pub(super) fn new(
        primary: hash_map::OccupiedEntry<'a, K1, (K2, V)>,
        secondary: hash_map::OccupiedEntry<'a, K2, K1>,
    ) -> Self {
        Self { primary, secondary }
    }
}

impl<'a, K1, K2, V> OccupiedEntry<'a, K1, K2, V> {
    /// Returns a reference to the `K1` key of the entry.
    pub fn key1(&self) -> &K1 {
        self.primary.key()
    }

    /// Returns a reference to the `K2` key of the entry.
    pub fn key2(&self) -> &K2 {
        self.secondary.key()
    }

    /// Returns a reference to the keys of type `K1` and `K2` of the entry.
    pub fn keys(&self) -> (&K1, &K2) {
        (self.key1(), self.key2())
    }

    /// Returns a reference to the value in the entry.
    pub fn get(&self) -> &V {
        &self.primary.get().1
    }

    /// Returns a mutable reference to the value in the entry.
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.primary.get_mut().1
    }

    /// Converts this entry into a mutable reference to its value.
    pub fn into_mut(self) -> &'a mut V {
        &mut self.primary.into_mut().1
    }

    /// Replaces the value in the entry and returns the old value.
    pub fn insert(&mut self, value: V) -> V {
        mem::replace(self.get_mut(), value)
    }

    /// Removes the entry from the map and returns its value.
    pub fn remove(self) -> V {
        self.remove_entry().2
    }

    /// Removes the entry from the map and returns its full `(K1, K2, V)` triple.
    pub fn remove_entry(self) -> (K1, K2, V) {
        self.secondary.remove_entry();
        let (key1, (key2, value)) = self.primary.remove_entry();
        (key1, key2, value)
    }
}

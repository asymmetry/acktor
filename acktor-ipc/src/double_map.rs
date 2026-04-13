//! A map-like container supporting lookup by two independent key types.
//!
//! [`DoubleMap`] keeps two internal [`HashMap`]s: one owns the value (keyed by `K1`)
//! and the other is an index from `K2` to `K1`. Lookups by either key type are
//! `O(1)`, and insertions, removals, and clears keep the two maps in sync.
//!
//! The public API mirrors [`std::collections::HashMap`] as closely as possible, with
//! key-centric methods (`get`, `contains`, `remove`, …) split into `_by_key1` and
//! `_by_key2` variants.

use std::borrow::Borrow;
use std::collections::hash_map;
use std::fmt::{self, Debug, Formatter};
use std::hash::Hash;
use std::ops::Index;

use ahash::{HashMap, HashMapExt};

pub use std::collections::TryReserveError;

mod errors;
pub use errors::KeyConflictError;

mod entry;
pub use entry::{Entry, OccupiedEntry, VacantEntry};

mod iter;
pub use iter::{Drain, IntoIter, IntoKeys, IntoValues, Iter, IterMut, Keys, Values, ValuesMut};

/// A map-like container supporting `O(1)` lookup by two key types.
pub struct DoubleMap<K1, K2, V> {
    /// Owns the value. Each entry also carries the `K2` key so removals and
    /// iteration can expose it without extra bookkeeping.
    primary: HashMap<K1, (K2, V)>,
    /// Index from the `K2` key back to the `K1` key.
    secondary: HashMap<K2, K1>,
}

impl<K1, K2, V> DoubleMap<K1, K2, V> {
    /// Constructs a new, empty [`DoubleMap`].
    pub fn new() -> Self {
        Self {
            primary: HashMap::default(),
            secondary: HashMap::default(),
        }
    }

    /// Constructs a new, empty [`DoubleMap`] with at least the specified capacity for
    /// both internal maps.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            primary: HashMap::with_capacity(capacity),
            secondary: HashMap::with_capacity(capacity),
        }
    }

    /// Returns the minimum of the two internal capacities.
    pub fn capacity(&self) -> usize {
        self.primary.capacity().min(self.secondary.capacity())
    }

    /// Returns an iterator over the keys, yielding a `(&K1, &K2)` tuple for each entry.
    pub fn keys(&self) -> Keys<'_, K1, K2, V> {
        Keys::new(self.primary.iter())
    }

    /// Creates a consuming iterator yielding owned `(K1, K2)` key tuples in arbitrary
    /// order. The map cannot be used after calling this.
    pub fn into_keys(self) -> IntoKeys<K1, K2, V> {
        let Self {
            primary,
            secondary: _,
        } = self;
        IntoKeys::new(primary.into_iter())
    }

    /// Returns an iterator over the values.
    pub fn values(&self) -> Values<'_, K1, K2, V> {
        Values::new(self.primary.values())
    }

    /// Returns an iterator yielding mutable references to each value.
    pub fn values_mut(&mut self) -> ValuesMut<'_, K1, K2, V> {
        ValuesMut::new(self.primary.values_mut())
    }

    /// Creates a consuming iterator yielding owned values in arbitrary order. The map
    /// cannot be used after calling this.
    pub fn into_values(self) -> IntoValues<K1, K2, V> {
        let Self {
            primary,
            secondary: _,
        } = self;
        IntoValues::new(primary.into_values())
    }

    /// Returns an iterator yielding every entry as `(&K1, &K2, &V)`.
    pub fn iter(&self) -> Iter<'_, K1, K2, V> {
        Iter::new(self.primary.iter())
    }

    /// Returns an iterator yielding every entry as `(&K1, &K2, &mut V)`.
    ///
    /// Neither key is mutably accessible — mutating them would desync the two
    /// internal maps.
    pub fn iter_mut(&mut self) -> IterMut<'_, K1, K2, V> {
        IterMut::new(self.primary.iter_mut())
    }

    /// Returns the number of entries in the map.
    pub fn len(&self) -> usize {
        self.primary.len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.primary.is_empty()
    }

    /// Clears the map, returning all entries as an iterator.
    pub fn drain(&mut self) -> Drain<'_, K1, K2, V> {
        self.secondary.clear();
        Drain::new(self.primary.drain())
    }

    /// Removes all entries.
    pub fn clear(&mut self) {
        self.primary.clear();
        self.secondary.clear();
    }
}

impl<K1, K2, V> DoubleMap<K1, K2, V>
where
    K1: Eq + Hash,
    K2: Eq + Hash,
{
    /// Retains only the entries for which the predicate returns `true`.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K1, &K2, &mut V) -> bool,
    {
        self.primary.retain(|k1, (k2, v)| {
            let keep = f(k1, k2, v);
            if !keep {
                self.secondary.remove(k2);
            }
            keep
        });
    }

    /// Reserves capacity for at least `additional` more entries in both internal maps.
    pub fn reserve(&mut self, additional: usize) {
        self.primary.reserve(additional);
        self.secondary.reserve(additional);
    }

    /// Tries to reserve capacity for at least `additional` more entries in both
    /// internal maps. Returns an error on allocation failure.
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.primary.try_reserve(additional)?;
        self.secondary.try_reserve(additional)?;
        Ok(())
    }

    /// Shrinks the internal maps to fit the current number of entries.
    pub fn shrink_to_fit(&mut self) {
        self.primary.shrink_to_fit();
        self.secondary.shrink_to_fit();
    }

    /// Shrinks the internal maps toward the given lower bound.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.primary.shrink_to(min_capacity);
        self.secondary.shrink_to(min_capacity);
    }

    /// Returns a reference to the value corresponding to the `K1` key.
    pub fn get_by_key1<Q>(&self, key: &Q) -> Option<&V>
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.primary.get(key).map(|(_, v)| v)
    }

    /// Returns a reference to the value corresponding to the `K2` key.
    pub fn get_by_key2<Q>(&self, key: &Q) -> Option<&V>
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let k1 = self.secondary.get(key)?;
        self.primary.get(k1).map(|(_, v)| v)
    }

    /// Returns a reference to the value for the entry whose `K1` key equals `key1`
    /// **and** whose `K2` key equals `key2`. Returns `None` if either key is missing,
    /// or if they refer to different entries.
    pub fn get_by_keys<Q1, Q2>(&self, key1: &Q1, key2: &Q2) -> Option<&V>
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        let (k2_stored, v) = self.primary.get(key1)?;
        if k2_stored.borrow() == key2 {
            Some(v)
        } else {
            None
        }
    }

    /// Returns the stored `K1` key and value for the entry identified by the given
    /// `K1` key.
    pub fn get_key1_value<Q>(&self, key: &Q) -> Option<(&K1, &V)>
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.primary.get_key_value(key).map(|(k1, (_, v))| (k1, v))
    }

    /// Returns the stored `K2` key and value for the entry identified by the given
    /// `K2` key.
    pub fn get_key2_value<Q>(&self, key: &Q) -> Option<(&K2, &V)>
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (k2_stored, k1) = self.secondary.get_key_value(key)?;
        let (_, v) = self.primary.get(k1)?;
        Some((k2_stored, v))
    }

    /// Returns the stored `(K1, K2, V)` triple for the entry whose `K1` key equals
    /// `key1` **and** whose `K2` key equals `key2`. Returns `None` if either key is
    /// missing, or if they refer to different entries.
    pub fn get_keys_value<Q1, Q2>(&self, key1: &Q1, key2: &Q2) -> Option<(&K1, &K2, &V)>
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        let (k1_stored, (k2_stored, v)) = self.primary.get_key_value(key1)?;
        if k2_stored.borrow() == key2 {
            Some((k1_stored, k2_stored, v))
        } else {
            None
        }
    }

    /// Returns `true` if the map contains a value for the given `K1` key.
    pub fn contains_key1<Q>(&self, key: &Q) -> bool
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.primary.contains_key(key)
    }

    /// Returns `true` if the map contains a value for the given `K2` key.
    pub fn contains_key2<Q>(&self, key: &Q) -> bool
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.secondary.contains_key(key)
    }

    /// Returns `true` if the map contains an entry whose `K1` key equals `key1`
    /// **and** whose `K2` key equals `key2` (i.e. both keys refer to the same entry).
    pub fn contains_keys<Q1, Q2>(&self, key1: &Q1, key2: &Q2) -> bool
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        self.get_by_keys(key1, key2).is_some()
    }

    /// Returns a mutable reference to the value corresponding to the `K1` key.
    pub fn get_mut_by_key1<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.primary.get_mut(key).map(|(_, v)| v)
    }

    /// Returns a mutable reference to the value corresponding to the `K2` key.
    pub fn get_mut_by_key2<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let k1 = self.secondary.get(key)?;
        self.primary.get_mut(k1).map(|(_, v)| v)
    }

    /// Returns a mutable reference to the value for the entry whose `K1` key equals
    /// `key1` **and** whose `K2` key equals `key2`. Returns `None` if either key is
    /// missing, or if they refer to different entries.
    pub fn get_mut_by_keys<Q1, Q2>(&mut self, key1: &Q1, key2: &Q2) -> Option<&mut V>
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        let (k2_stored, v) = self.primary.get_mut(key1)?;
        if <K2 as Borrow<Q2>>::borrow(k2_stored) == key2 {
            Some(v)
        } else {
            None
        }
    }

    /// Removes an entry by its `K1` key, returning the value if it was present.
    pub fn remove_by_key1<Q>(&mut self, key: &Q) -> Option<V>
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (k2, value) = self.primary.remove(key)?;
        self.secondary.remove(&k2);
        Some(value)
    }

    /// Removes an entry by its `K2` key, returning the value if it was present.
    pub fn remove_by_key2<Q>(&mut self, key: &Q) -> Option<V>
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let k1 = self.secondary.remove(key)?;
        let (_, value) = self
            .primary
            .remove(&k1)
            .expect("primary map must contain key 1 whenever secondary map points to it");
        Some(value)
    }

    /// Removes an entry only if its `K1` key equals `key1` **and** its `K2` key equals
    /// `key2`. Returns the value on success, or `None` if either key is missing or they
    /// refer to different entries. On mismatch, the map is unchanged.
    pub fn remove_by_keys<Q1, Q2>(&mut self, key1: &Q1, key2: &Q2) -> Option<V>
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        let (k1, (k2, value)) = self.primary.remove_entry(key1)?;
        if <K2 as Borrow<Q2>>::borrow(&k2) != key2 {
            self.primary.insert(k1, (k2, value));
            return None;
        }
        self.secondary.remove::<K2>(&k2);
        Some(value)
    }

    /// Removes an entry by its `K1` key, returning the full `(K1, K2, V)` triple if it
    /// was present.
    pub fn remove_entry_by_key1<Q>(&mut self, key: &Q) -> Option<(K1, K2, V)>
    where
        K1: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (k1, (k2, value)) = self.primary.remove_entry(key)?;
        self.secondary.remove(&k2);
        Some((k1, k2, value))
    }

    /// Removes an entry by its `K2` key, returning the full `(K1, K2, V)` triple if it
    /// was present.
    pub fn remove_entry_by_key2<Q>(&mut self, key: &Q) -> Option<(K1, K2, V)>
    where
        K2: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let k1 = self.secondary.remove(key)?;
        let (k1, (k2, value)) = self
            .primary
            .remove_entry(&k1)
            .expect("primary map must contain key 1 whenever secondary map points to it");
        Some((k1, k2, value))
    }

    /// Removes an entry only if its `K1` key equals `key1` **and** its `K2` key equals
    /// `key2`. Returns the full `(K1, K2, V)` triple on success, or `None` if either
    /// key is missing or they refer to different entries. On mismatch, the map is
    /// unchanged.
    pub fn remove_entry_by_keys<Q1, Q2>(&mut self, key1: &Q1, key2: &Q2) -> Option<(K1, K2, V)>
    where
        K1: Borrow<Q1>,
        K2: Borrow<Q2>,
        Q1: Eq + Hash + ?Sized,
        Q2: Eq + Hash + ?Sized,
    {
        let (k1, (k2, value)) = self.primary.remove_entry(key1)?;
        if <K2 as Borrow<Q2>>::borrow(&k2) != key2 {
            self.primary.insert(k1, (k2, value));
            return None;
        }
        self.secondary.remove::<K2>(&k2);
        Some((k1, k2, value))
    }
}

impl<K1, K2, V> DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    /// Gets the given `(key1, key2)` pair's [`Entry`] in the map for in-place
    /// manipulation.
    ///
    /// - Returns `Ok(Entry::Occupied)` if both keys refer to the same existing entry.
    /// - Returns `Ok(Entry::Vacant)` if neither key is present.
    /// - Returns [`Err(KeyConflictError)`][KeyConflictError] if the two keys clash
    ///   with existing entries. In the error case the map is unchanged and the
    ///   rejected `(K1, K2)` pair is returned.
    pub fn entry(
        &mut self,
        key1: K1,
        key2: K2,
    ) -> Result<Entry<'_, K1, K2, V>, KeyConflictError<K1, K2>> {
        let key1_matches_key2 = self
            .primary
            .get(&key1)
            .map(|(k2_stored, _)| k2_stored == &key2);
        let key2_present = self.secondary.contains_key(&key2);

        match (key1_matches_key2, key2_present) {
            // both keys identify the same existing entry
            (Some(true), _) => {
                let hash_map::Entry::Occupied(e1) = self.primary.entry(key1) else {
                    unreachable!("primary.get just observed key 1 as present");
                };
                let hash_map::Entry::Occupied(e2) = self.secondary.entry(key2) else {
                    unreachable!(
                        "key 1's stored key 2 matches the argument, so secondary must contain it",
                    );
                };
                Ok(Entry::Occupied(OccupiedEntry::new(e1, e2)))
            }
            // neither key is present
            (None, false) => {
                let hash_map::Entry::Vacant(e1) = self.primary.entry(key1) else {
                    unreachable!("primary.get just observed key 1 as absent");
                };
                let hash_map::Entry::Vacant(e2) = self.secondary.entry(key2) else {
                    unreachable!("secondary.contains_key just observed key 2 as absent");
                };
                Ok(Entry::Vacant(VacantEntry::new(e1, e2)))
            }
            // `key1` is present but paired with a different `key2`
            (Some(false), true) => Err(KeyConflictError::BothKeysExist(key1, key2, ())),
            (Some(false), false) => Err(KeyConflictError::Key1Exists(key1, key2, ())),
            // `key1` is absent but `key2` is present (paired with a different `key1`)
            (None, true) => Err(KeyConflictError::Key2Exists(key1, key2, ())),
        }
    }

    /// Inserts a new entry, or updates the value if both keys are already present and
    /// refer to the same entry.
    ///
    /// - Returns `Ok(None)` when neither key was present and a fresh entry was inserted.
    /// - Returns `Ok(Some(old_value))` when both keys matched the same existing entry
    ///   and its value was replaced.
    /// - Returns [`Err(KeyConflictError)`][KeyConflictError] if the two keys clash
    ///   with existing entries. In all error cases the map is unchanged and the rejected
    ///   triple is returned.
    pub fn insert(
        &mut self,
        key1: K1,
        key2: K2,
        value: V,
    ) -> Result<Option<V>, KeyConflictError<K1, K2, V>> {
        if let Some((existing_k2, _)) = self.primary.get(&key1) {
            if existing_k2 == &key2 {
                // both keys refer to the same entry; update the value in place
                let old = self.primary.insert(key1, (key2, value));
                return Ok(old.map(|(_, v_old)| v_old));
            }
            // `key1` is in the map paired with a different `key2`. Distinguish
            // "only key1 clashes" from "both keys are present in different entries"
            if self.secondary.contains_key(&key2) {
                return Err(KeyConflictError::BothKeysExist(key1, key2, value));
            }
            return Err(KeyConflictError::Key1Exists(key1, key2, value));
        }

        // `key1` is not present; ensure `key2` is also absent
        if self.secondary.contains_key(&key2) {
            return Err(KeyConflictError::Key2Exists(key1, key2, value));
        }

        self.secondary.insert(key2.clone(), key1.clone());
        self.primary.insert(key1, (key2, value));

        Ok(None)
    }
}

impl<K1, K2, V> Clone for DoubleMap<K1, K2, V>
where
    K1: Clone,
    K2: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            primary: self.primary.clone(),
            secondary: self.secondary.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.primary.clone_from(&source.primary);
        self.secondary.clone_from(&source.secondary);
    }
}

impl<K1, K2, V> PartialEq for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash,
    K2: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.primary == other.primary
    }
}

impl<K1, K2, V> Eq for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash,
    K2: Eq + Hash,
    V: Eq,
{
}

impl<K1, K2, V> Debug for DoubleMap<K1, K2, V>
where
    K1: Debug,
    K2: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(k1, k2, v)| ((k1, k2), v)))
            .finish()
    }
}

impl<K1, K2, V> Default for DoubleMap<K1, K2, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K1, K2, V, Q> Index<&Q> for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Borrow<Q>,
    K2: Eq + Hash,
    Q: Eq + Hash + ?Sized,
{
    type Output = V;

    /// Returns a reference to the value corresponding to the supplied key1.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the map.
    fn index(&self, key: &Q) -> &V {
        self.get_by_key1(key).expect("no entry found for key")
    }
}

impl<K1, K2, V> IntoIterator for DoubleMap<K1, K2, V> {
    type Item = (K1, K2, V);
    type IntoIter = IntoIter<K1, K2, V>;

    /// Consumes the map, yielding owned `(K1, K2, V)` triples.
    fn into_iter(self) -> Self::IntoIter {
        let Self {
            primary,
            secondary: _,
        } = self;
        IntoIter::new(primary.into_iter())
    }
}

impl<'a, K1, K2, V> IntoIterator for &'a DoubleMap<K1, K2, V> {
    type Item = (&'a K1, &'a K2, &'a V);
    type IntoIter = Iter<'a, K1, K2, V>;

    /// Borrows the map, yielding `(&K1, &K2, &V)` triples.
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K1, K2, V> IntoIterator for &'a mut DoubleMap<K1, K2, V> {
    type Item = (&'a K1, &'a K2, &'a mut V);
    type IntoIter = IterMut<'a, K1, K2, V>;

    /// Mutably borrows the map, yielding `(&K1, &K2, &mut V)` triples. Keys are exposed
    /// immutably to preserve the invariant that both internal maps stay in sync.
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<K1, K2, V> Extend<(K1, K2, V)> for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    /// Extends the map with `(K1, K2, V)` triples from any iterator.
    ///
    /// Triples whose keys conflict with an existing entry (same `K1` mapped to a
    /// different `K2`, or same `K2` mapped to a different `K1`) are silently skipped
    /// and the map is left unchanged for that triple. A consistent repeat (same `K1`
    /// **and** same `K2`) updates the value.
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K1, K2, V)>,
    {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        self.reserve(lower);
        for (k1, k2, v) in iter {
            let _ = self.insert(k1, k2, v);
        }
    }
}

impl<'a, K1, K2, V> Extend<(&'a K1, &'a K2, &'a V)> for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Copy,
    K2: Eq + Hash + Copy,
    V: Copy,
{
    /// Extends the map with borrowed `(&K1, &K2, &V)` triples, copying each element into
    /// place.
    ///
    /// Conflicting triples are silently skipped, same as the owned-triple [`Extend`] impl.
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (&'a K1, &'a K2, &'a V)>,
    {
        self.extend(iter.into_iter().map(|(k1, k2, v)| (*k1, *k2, *v)));
    }
}

impl<K1, K2, V> FromIterator<(K1, K2, V)> for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    /// Collects `(K1, K2, V)` triples into a fresh [`DoubleMap`].
    ///
    /// Conflicting triples are silently skipped, same as [`Extend`].
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (K1, K2, V)>,
    {
        let mut map = Self::new();
        map.extend(iter);
        map
    }
}

impl<K1, K2, V, const N: usize> From<[(K1, K2, V); N]> for DoubleMap<K1, K2, V>
where
    K1: Eq + Hash + Clone,
    K2: Eq + Hash + Clone,
{
    /// Builds a [`DoubleMap`] from an array of `(K1, K2, V)` triples.
    ///
    /// Conflicting triples are silently skipped, same as [`FromIterator`].
    fn from(arr: [(K1, K2, V); N]) -> Self {
        Self::from_iter(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> DoubleMap<u64, String, i32> {
        DoubleMap::new()
    }

    fn populated() -> DoubleMap<u64, String, i32> {
        let mut map = fresh();
        map.insert(1, "foo".to_string(), 10).unwrap();
        map.insert(2, "bar".to_string(), 20).unwrap();
        map
    }

    #[test]
    fn test_new() {
        let map = fresh();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let map: DoubleMap<u64, String, i32> = DoubleMap::with_capacity(64);
        assert!(map.capacity() >= 64);
        assert!(map.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut map = fresh();

        // New entry.
        assert_eq!(map.insert(1, "foo".to_string(), 10).unwrap(), None);
        assert_eq!(map.get_by_key1(&1), Some(&10));
        assert_eq!(map.get_by_key2("foo"), Some(&10));

        // Same (key1, key2) pair replaces the value.
        assert_eq!(map.insert(1, "foo".to_string(), 99).unwrap(), Some(10));
        assert_eq!(map.get_by_key1(&1), Some(&99));

        // key1 collides with a different key2.
        let err = map.insert(1, "bar".to_string(), 20).unwrap_err();
        assert!(matches!(err, KeyConflictError::Key1Exists(1, _, 20)));

        // key2 collides with a different key1.
        let err = map.insert(2, "foo".to_string(), 30).unwrap_err();
        assert!(matches!(err, KeyConflictError::Key2Exists(2, _, 30)));

        // Both keys present in different entries.
        map.insert(2, "bar".to_string(), 20).unwrap();
        let err = map.insert(1, "bar".to_string(), 42).unwrap_err();
        assert!(matches!(err, KeyConflictError::BothKeysExist(1, _, 42)));
    }

    #[test]
    fn test_get_by_key1() {
        let map = populated();
        assert_eq!(map.get_by_key1(&1), Some(&10));
        assert_eq!(map.get_by_key1(&99), None);
    }

    #[test]
    fn test_get_by_key2() {
        let map = populated();
        // `&str` for a `String` key exercises the `Borrow<Q>` path.
        assert_eq!(map.get_by_key2("foo"), Some(&10));
        assert_eq!(map.get_by_key2("missing"), None);
    }

    #[test]
    fn test_get_by_keys() {
        let map = populated();
        assert_eq!(map.get_by_keys(&1, "foo"), Some(&10));
        // Mismatch.
        assert_eq!(map.get_by_keys(&1, "bar"), None);
        // Missing.
        assert_eq!(map.get_by_keys(&99, "foo"), None);
    }

    #[test]
    fn test_get_key1_value() {
        let map = populated();
        let (k1, v) = map.get_key1_value(&1).unwrap();
        assert_eq!((*k1, *v), (1, 10));
        assert!(map.get_key1_value(&99).is_none());
    }

    #[test]
    fn test_get_key2_value() {
        let map = populated();
        let (k2, v) = map.get_key2_value("foo").unwrap();
        assert_eq!((k2.as_str(), *v), ("foo", 10));
        assert!(map.get_key2_value("missing").is_none());
    }

    #[test]
    fn test_get_keys_value() {
        let map = populated();
        let (k1, k2, v) = map.get_keys_value(&1, "foo").unwrap();
        assert_eq!((*k1, k2.as_str(), *v), (1, "foo", 10));
        assert!(map.get_keys_value(&1, "bar").is_none());
    }

    #[test]
    fn test_contains_key1() {
        let map = populated();
        assert!(map.contains_key1(&1));
        assert!(!map.contains_key1(&99));
    }

    #[test]
    fn test_contains_key2() {
        let map = populated();
        assert!(map.contains_key2("foo"));
        assert!(!map.contains_key2("missing"));
    }

    #[test]
    fn test_contains_keys() {
        let map = populated();
        assert!(map.contains_keys(&1, "foo"));
        assert!(!map.contains_keys(&1, "bar"));
        assert!(!map.contains_keys(&99, "foo"));
    }

    #[test]
    fn test_get_mut_by_key1() {
        let mut map = populated();
        *map.get_mut_by_key1(&1).unwrap() = 42;
        assert_eq!(map.get_by_key1(&1), Some(&42));
        assert_eq!(map.get_by_key2("foo"), Some(&42));
    }

    #[test]
    fn test_get_mut_by_key2() {
        let mut map = populated();
        *map.get_mut_by_key2("foo").unwrap() = 42;
        assert_eq!(map.get_by_key1(&1), Some(&42));
    }

    #[test]
    fn test_get_mut_by_keys() {
        let mut map = populated();
        *map.get_mut_by_keys(&1, "foo").unwrap() = 42;
        assert_eq!(map.get_by_key1(&1), Some(&42));
        // Mismatch: no mutation.
        assert!(map.get_mut_by_keys(&1, "bar").is_none());
    }

    #[test]
    fn test_remove_by_key1() {
        let mut map = populated();
        assert_eq!(map.remove_by_key1(&1), Some(10));
        assert!(!map.contains_key1(&1));
        assert!(!map.contains_key2("foo"));
        assert_eq!(map.remove_by_key1(&99), None);
    }

    #[test]
    fn test_remove_by_key2() {
        let mut map = populated();
        assert_eq!(map.remove_by_key2("foo"), Some(10));
        assert!(!map.contains_key1(&1));
        assert!(!map.contains_key2("foo"));
        assert_eq!(map.remove_by_key2("missing"), None);
    }

    #[test]
    fn test_remove_by_keys() {
        let mut map = populated();

        // Mismatch: no removal, map unchanged.
        assert_eq!(map.remove_by_keys(&1, "bar"), None);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key1(&1));

        // Match: both indexes cleaned.
        assert_eq!(map.remove_by_keys(&1, "foo"), Some(10));
        assert!(!map.contains_key1(&1));
        assert!(!map.contains_key2("foo"));
    }

    #[test]
    fn test_remove_entry_by_key1() {
        let mut map = populated();
        assert_eq!(
            map.remove_entry_by_key1(&1),
            Some((1, "foo".to_string(), 10))
        );
        assert!(map.remove_entry_by_key1(&99).is_none());
    }

    #[test]
    fn test_remove_entry_by_key2() {
        let mut map = populated();
        assert_eq!(
            map.remove_entry_by_key2("foo"),
            Some((1, "foo".to_string(), 10))
        );
        assert!(map.remove_entry_by_key2("missing").is_none());
    }

    #[test]
    fn test_remove_entry_by_keys() {
        let mut map = populated();
        assert_eq!(
            map.remove_entry_by_keys(&1, "foo"),
            Some((1, "foo".to_string(), 10))
        );
        // Mismatch: no removal.
        assert!(map.remove_entry_by_keys(&2, "foo").is_none());
    }

    #[test]
    fn test_clear() {
        let mut map = populated();
        map.clear();
        assert!(map.is_empty());
        assert!(map.get_by_key1(&1).is_none());
    }

    #[test]
    fn test_drain() {
        let mut map = populated();
        let mut drained: Vec<_> = map.drain().collect();
        drained.sort_by_key(|(k1, _, _)| *k1);
        assert_eq!(
            drained,
            vec![(1, "foo".to_string(), 10), (2, "bar".to_string(), 20)]
        );
        assert!(map.is_empty());
    }

    #[test]
    fn test_retain() {
        let mut map = populated();
        map.retain(|_, _, v| *v >= 20);
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key1(&1));
        assert!(!map.contains_key2("foo"));
        assert!(map.contains_key1(&2));
    }

    #[test]
    fn test_iter() {
        let map = populated();
        let mut entries: Vec<_> = map
            .iter()
            .map(|(k1, k2, v)| (*k1, k2.clone(), *v))
            .collect();
        entries.sort_by_key(|(k1, _, _)| *k1);
        assert_eq!(
            entries,
            vec![(1, "foo".to_string(), 10), (2, "bar".to_string(), 20)]
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut map = populated();
        // Uses `IntoIterator for &mut DoubleMap`, which delegates to `iter_mut`.
        for (_, _, v) in &mut map {
            *v *= 10;
        }
        assert_eq!(map.get_by_key1(&1), Some(&100));
        assert_eq!(map.get_by_key1(&2), Some(&200));
    }

    #[test]
    fn test_keys() {
        let map = populated();
        let mut keys: Vec<_> = map.keys().map(|(k1, k2)| (*k1, k2.clone())).collect();
        keys.sort_by_key(|(k1, _)| *k1);
        assert_eq!(keys, vec![(1, "foo".to_string()), (2, "bar".to_string())]);
    }

    #[test]
    fn test_values() {
        let map = populated();
        let mut values: Vec<_> = map.values().copied().collect();
        values.sort();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_values_mut() {
        let mut map = populated();
        for v in map.values_mut() {
            *v += 1;
        }
        assert_eq!(map.get_by_key1(&1), Some(&11));
    }

    #[test]
    fn test_into_iter() {
        let mut collected: Vec<_> = populated().into_iter().collect();
        collected.sort_by_key(|(k1, _, _)| *k1);
        assert_eq!(
            collected,
            vec![(1, "foo".to_string(), 10), (2, "bar".to_string(), 20)]
        );
    }

    #[test]
    fn test_into_keys() {
        let mut keys: Vec<_> = populated().into_keys().collect();
        keys.sort_by_key(|(k1, _)| *k1);
        assert_eq!(keys, vec![(1, "foo".to_string()), (2, "bar".to_string())]);
    }

    #[test]
    fn test_into_values() {
        let mut values: Vec<_> = populated().into_values().collect();
        values.sort();
        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn test_entry() {
        let mut map = populated();

        // Occupied: both keys identify the same entry.
        match map.entry(1, "foo".to_string()).unwrap() {
            Entry::Occupied(mut occ) => {
                assert_eq!(*occ.get(), 10);
                *occ.get_mut() = 42;
                let old = occ.insert(7);
                assert_eq!(old, 42);
                assert_eq!(occ.remove_entry(), (1, "foo".to_string(), 7));
            }
            Entry::Vacant(_) => panic!("expected Occupied"),
        }
        assert!(!map.contains_key1(&1));

        // Vacant: neither key is present.
        match map.entry(3, "baz".to_string()).unwrap() {
            Entry::Vacant(vac) => {
                assert_eq!(*vac.key1(), 3);
                assert_eq!(vac.key2(), "baz");
                vac.insert(30);
            }
            Entry::Occupied(_) => panic!("expected Vacant"),
        }
        assert_eq!(map.get_by_key1(&3), Some(&30));

        // Err: key1 clashes with a different key2.
        assert!(matches!(
            map.entry(2, "quux".to_string()),
            Err(KeyConflictError::Key1Exists(2, _, _))
        ));

        // Err: key2 clashes with a different key1.
        assert!(matches!(
            map.entry(99, "bar".to_string()),
            Err(KeyConflictError::Key2Exists(99, _, _))
        ));

        // Err: both keys present in different entries.
        assert!(matches!(
            map.entry(2, "baz".to_string()),
            Err(KeyConflictError::BothKeysExist(2, _, _))
        ));
    }

    #[test]
    fn test_entry_or_insert() {
        let mut map = fresh();

        // Vacant: inserts and returns a mutable reference.
        let v = map.entry(1, "foo".to_string()).unwrap().or_insert(10);
        *v = 42;
        assert_eq!(map.get_by_key1(&1), Some(&42));

        // Occupied: returns existing value, does not overwrite.
        let v = map.entry(1, "foo".to_string()).unwrap().or_insert(99);
        assert_eq!(*v, 42);
    }

    #[test]
    fn test_entry_or_default() {
        let mut map: DoubleMap<u64, String, i32> = fresh();
        let v = map.entry(1, "foo".to_string()).unwrap().or_default();
        assert_eq!(*v, 0);
    }

    #[test]
    fn test_entry_and_modify() {
        let mut map = populated();

        // Occupied: closure runs.
        map.entry(1, "foo".to_string())
            .unwrap()
            .and_modify(|v| *v *= 2)
            .or_insert(0);
        assert_eq!(map.get_by_key1(&1), Some(&20));

        // Vacant: closure is a no-op, or_insert inserts.
        map.entry(3, "baz".to_string())
            .unwrap()
            .and_modify(|v| *v *= 2)
            .or_insert(5);
        assert_eq!(map.get_by_key1(&3), Some(&5));
    }

    #[test]
    fn test_from() {
        // Conflicting triples are silently skipped.
        let map: DoubleMap<u64, String, i32> = DoubleMap::from([
            (1, "foo".to_string(), 10),
            (2, "bar".to_string(), 20),
            (1, "zzz".to_string(), 99),
        ]);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get_by_key1(&1), Some(&10));
        assert!(!map.contains_key2("zzz"));
    }

    #[test]
    fn test_from_iter() {
        let map: DoubleMap<u64, String, i32> =
            vec![(1u64, "foo".to_string(), 10), (2u64, "bar".to_string(), 20)]
                .into_iter()
                .collect();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get_by_key1(&1), Some(&10));
    }

    #[test]
    fn test_extend() {
        let mut map = fresh();
        map.insert(1, "foo".to_string(), 10).unwrap();

        // Conflicting triples are silently skipped; consistent repeats update the value.
        map.extend(vec![
            (2, "bar".to_string(), 20),
            (1, "zzz".to_string(), 99),
            (1, "foo".to_string(), 42),
        ]);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get_by_key1(&1), Some(&42));
        assert_eq!(map.get_by_key1(&2), Some(&20));
        assert!(!map.contains_key2("zzz"));
    }

    #[test]
    fn test_index() {
        let map = populated();
        assert_eq!(map[&1], 10);
        assert_eq!(map[&2], 20);
    }

    #[test]
    #[should_panic(expected = "no entry found for key")]
    fn test_index_panics() {
        let _ = fresh()[&1];
    }

    #[test]
    fn test_clone() {
        let cloned = populated().clone();
        assert_eq!(cloned.len(), 2);
        assert_eq!(cloned.get_by_key1(&1), Some(&10));
        assert_eq!(cloned.get_by_key2("bar"), Some(&20));
    }

    #[test]
    fn test_eq() {
        let a = populated();

        // Insertion order does not matter.
        let mut b = fresh();
        b.insert(2, "bar".to_string(), 20).unwrap();
        b.insert(1, "foo".to_string(), 10).unwrap();
        assert_eq!(a, b);

        // Differing value.
        let mut c = fresh();
        c.insert(1, "foo".to_string(), 10).unwrap();
        c.insert(2, "bar".to_string(), 99).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn test_reserve() {
        let mut map = fresh();
        map.reserve(128);
        assert!(map.capacity() >= 128);
        map.insert(1, "foo".to_string(), 10).unwrap();
        assert_eq!(map.get_by_key1(&1), Some(&10));
    }

    #[test]
    fn test_try_reserve() {
        let mut map: DoubleMap<u64, String, i32> = fresh();
        map.try_reserve(64).unwrap();
        assert!(map.capacity() >= 64);
    }

    #[test]
    fn test_shrink_to_fit() {
        let mut map = populated();
        map.reserve(128);
        map.shrink_to_fit();
        assert_eq!(map.get_by_key1(&1), Some(&10));
        assert_eq!(map.get_by_key2("bar"), Some(&20));
    }
}

//! Iterator types returned by [`DoubleMap`][super::DoubleMap]'s iteration methods
//! and its [`IntoIterator`] impls.

use std::collections::hash_map;
use std::iter::FusedIterator;

/// An owning iterator over the entries of a [`DoubleMap`][super::DoubleMap], yielding
/// `(K1, K2, V)` triples.
pub struct IntoIter<K1, K2, V> {
    inner: hash_map::IntoIter<K1, (K2, V)>,
}

impl<K1, K2, V> IntoIter<K1, K2, V> {
    pub(super) fn new(inner: hash_map::IntoIter<K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<K1, K2, V> Iterator for IntoIter<K1, K2, V> {
    type Item = (K1, K2, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, v))| (k1, k2, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for IntoIter<K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for IntoIter<K1, K2, V> {}

/// A borrowing iterator over the entries of a [`DoubleMap`][super::DoubleMap], yielding
/// `(&'a K1, &'a K2, &'a V)` triples.
pub struct Iter<'a, K1, K2, V> {
    inner: hash_map::Iter<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> Iter<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::Iter<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<'a, K1, K2, V> Iterator for Iter<'a, K1, K2, V> {
    type Item = (&'a K1, &'a K2, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, v))| (k1, k2, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for Iter<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for Iter<'_, K1, K2, V> {}

impl<K1, K2, V> Clone for Iter<'_, K1, K2, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A mutably borrowing iterator over the entries of a [`DoubleMap`][super::DoubleMap],
/// yielding `(&'a K1, &'a K2, &'a mut V)` triples.
pub struct IterMut<'a, K1, K2, V> {
    inner: hash_map::IterMut<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> IterMut<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::IterMut<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<'a, K1, K2, V> Iterator for IterMut<'a, K1, K2, V> {
    type Item = (&'a K1, &'a K2, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, v))| (k1, &*k2, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for IterMut<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for IterMut<'_, K1, K2, V> {}

/// A draining iterator over the entries of a [`DoubleMap`][super::DoubleMap], yielding
/// `(K1, K2, V)` triples while emptying the map in place.
pub struct Drain<'a, K1, K2, V> {
    inner: hash_map::Drain<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> Drain<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::Drain<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<K1, K2, V> Iterator for Drain<'_, K1, K2, V> {
    type Item = (K1, K2, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, v))| (k1, k2, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for Drain<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for Drain<'_, K1, K2, V> {}

/// A borrowing iterator over the keys of a [`DoubleMap`][super::DoubleMap], yielding
/// `(&'a K1, &'a K2)` tuples.
pub struct Keys<'a, K1, K2, V> {
    inner: hash_map::Iter<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> Keys<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::Iter<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<'a, K1, K2, V> Iterator for Keys<'a, K1, K2, V> {
    type Item = (&'a K1, &'a K2);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, _))| (k1, k2))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for Keys<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for Keys<'_, K1, K2, V> {}

impl<K1, K2, V> Clone for Keys<'_, K1, K2, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A consuming iterator over the keys of a [`DoubleMap`][super::DoubleMap], yielding
/// owned `(K1, K2)` tuples.
pub struct IntoKeys<K1, K2, V> {
    inner: hash_map::IntoIter<K1, (K2, V)>,
}

impl<K1, K2, V> IntoKeys<K1, K2, V> {
    pub(super) fn new(inner: hash_map::IntoIter<K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<K1, K2, V> Iterator for IntoKeys<K1, K2, V> {
    type Item = (K1, K2);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k1, (k2, _))| (k1, k2))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for IntoKeys<K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for IntoKeys<K1, K2, V> {}

/// A borrowing iterator over the values of a [`DoubleMap`][super::DoubleMap], yielding
/// `&'a V`.
pub struct Values<'a, K1, K2, V> {
    inner: hash_map::Values<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> Values<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::Values<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<'a, K1, K2, V> Iterator for Values<'a, K1, K2, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for Values<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for Values<'_, K1, K2, V> {}

impl<K1, K2, V> Clone for Values<'_, K1, K2, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A mutably borrowing iterator over the values of a [`DoubleMap`][super::DoubleMap],
/// yielding `&'a mut V`.
pub struct ValuesMut<'a, K1, K2, V> {
    inner: hash_map::ValuesMut<'a, K1, (K2, V)>,
}

impl<'a, K1, K2, V> ValuesMut<'a, K1, K2, V> {
    pub(super) fn new(inner: hash_map::ValuesMut<'a, K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<'a, K1, K2, V> Iterator for ValuesMut<'a, K1, K2, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for ValuesMut<'_, K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for ValuesMut<'_, K1, K2, V> {}

/// A consuming iterator over the values of a [`DoubleMap`][super::DoubleMap], yielding
/// owned `V`.
pub struct IntoValues<K1, K2, V> {
    inner: hash_map::IntoValues<K1, (K2, V)>,
}

impl<K1, K2, V> IntoValues<K1, K2, V> {
    pub(super) fn new(inner: hash_map::IntoValues<K1, (K2, V)>) -> Self {
        Self { inner }
    }
}

impl<K1, K2, V> Iterator for IntoValues<K1, K2, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K1, K2, V> ExactSizeIterator for IntoValues<K1, K2, V> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K1, K2, V> FusedIterator for IntoValues<K1, K2, V> {}

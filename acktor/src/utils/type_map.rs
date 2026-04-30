use std::any::TypeId;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// A hasher which passes through pre-hashed values (e.g. [`TypeId`]) without rehashing.
#[derive(Default)]
pub struct NoOpHasher {
    hash: u64,
}

impl Hasher for NoOpHasher {
    #[inline]
    fn write_u8(&mut self, n: u8) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n as u64;
    }

    #[inline]
    fn write_u16(&mut self, n: u16) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n as u64;
    }

    #[inline]
    fn write_u32(&mut self, n: u32) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n as u64;
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n;
    }

    #[inline]
    fn write_u128(&mut self, n: u128) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n as u64;
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        // only a single value can be hashed, so the old hash should be zero
        debug_assert_eq!(self.hash, 0);
        self.hash = n as u64;
    }

    #[inline]
    fn write(&mut self, _: &[u8]) {
        panic!("NoOpHasher is only intended for pre-hashed integer values")
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// A special HashMap which uses [`TypeId`] as keys and a custom [`NoOpHasher`] to avoid rehashing
/// the `TypeId`s.
pub type TypeMap<V> = HashMap<TypeId, V, BuildHasherDefault<NoOpHasher>>;

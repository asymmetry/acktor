use std::any::TypeId;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// A hasher which passes through pre-hashed values (e.g. [`TypeId`]) without rehashing.
#[derive(Default)]
pub struct NopHasher {
    hash: u64,
}

impl Hasher for NopHasher {
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
        panic!("NopHasher is only intended for pre-hashed integer values")
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// A special HashMap which uses [`TypeId`] as keys and a custom [`NoOpHasher`] to avoid rehashing
/// the [`TypeId`]s.
pub type TypeMap<V> = HashMap<TypeId, V, BuildHasherDefault<NopHasher>>;

#[cfg(test)]
mod tests {
    use std::hash::Hasher;

    use pretty_assertions::assert_eq;

    use super::*;

    fn finish_after<F>(write: F) -> u64
    where
        F: FnOnce(&mut NopHasher),
    {
        let mut h = NopHasher::default();
        write(&mut h);
        h.finish()
    }

    #[test]
    fn test_nop_hasher() {
        // default state
        assert_eq!(NopHasher::default().finish(), 0);

        // every integer width passes through; u128 truncates to the low 64 bits
        assert_eq!(finish_after(|h| h.write_u8(0x7f)), 0x7f);
        assert_eq!(finish_after(|h| h.write_u16(0xbeef)), 0xbeef);
        assert_eq!(finish_after(|h| h.write_u32(0xdead_beef)), 0xdead_beef);
        assert_eq!(
            finish_after(|h| h.write_u64(0x0123_4567_89ab_cdef)),
            0x0123_4567_89ab_cdef,
        );
        assert_eq!(finish_after(|h| h.write_usize(42)), 42);
        assert_eq!(
            finish_after(|h| h.write_u128(0xffff_ffff_ffff_ffff_0123_4567_89ab_cdef)),
            0x0123_4567_89ab_cdef,
        );
    }

    #[test]
    #[should_panic(expected = "NopHasher is only intended for pre-hashed integer values")]
    fn test_write_bytes() {
        NopHasher::default().write(&[1, 2, 3]);
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn test_write_twice() {
        let mut h = NopHasher::default();
        h.write_u64(1);
        h.write_u64(2);
    }

    #[test]
    fn test_type_map() {
        let mut map: TypeMap<&'static str> = TypeMap::default();
        map.insert(TypeId::of::<u32>(), "u32");
        map.insert(TypeId::of::<String>(), "String");

        assert_eq!(map.get(&TypeId::of::<u32>()).copied(), Some("u32"));
        assert_eq!(map.get(&TypeId::of::<String>()).copied(), Some("String"));
        assert_eq!(map.get(&TypeId::of::<bool>()).copied(), None);
        assert_eq!(map.len(), 2);
    }
}

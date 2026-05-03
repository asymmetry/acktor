//! Stable type identifier.
//!

use std::sync::Arc;

#[doc(hidden)]
pub const fn take16(arr: [u8; 32]) -> [u8; 16] {
    [
        arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8], arr[9], arr[10],
        arr[11], arr[12], arr[13], arr[14], arr[15],
    ]
}

/// A `StableTypeId` represents a stable unique identifier for a type, which is consistent across
/// different compilations in contrast to the standard [`std::any::TypeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableTypeId([u8; 16]);

impl StableTypeId {
    /// Build a `StableTypeId` from a fully-qualified type name by hashing it with SHA-256 and
    /// taking the first 16 bytes of the digest as the id.
    pub const fn from_stable_type_name(type_name: &'static str) -> Self {
        let result = sha2_const::Sha256::new()
            .update(type_name.as_bytes())
            .finalize();
        StableTypeId(take16(result))
    }

    /// Folds an additional 16 byte chunk into this id by concatenating the two, hashing the
    /// result with SHA-256, and taking the first 16 bytes of the digest as the new id.
    pub const fn combine(self, other: &[u8; 16]) -> Self {
        let result = sha2_const::Sha256::new()
            .update(&self.0)
            .update(other)
            .finalize();
        StableTypeId(take16(result))
    }

    /// Returns the first 8 bytes of the id interpreted as a big-endian `u64`.
    pub const fn as_u64(&self) -> u64 {
        u64::from_be_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7],
        ])
    }

    /// Returns the id interpreted as a big-endian `u128`.
    pub const fn as_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }

    /// Returns the id as a byte array.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// A trait for types that have a stable unique iidentifier.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(StableId)]`][acktor_derive::StableId].
pub trait StableId {
    const TYPE_ID: StableTypeId;
}

impl<T> StableId for Box<T>
where
    T: StableId + ?Sized,
{
    const TYPE_ID: StableTypeId =
        StableTypeId::from_stable_type_name("alloc::boxed::Box").combine(T::TYPE_ID.as_bytes());
}

impl<T> StableId for Arc<T>
where
    T: StableId + ?Sized,
{
    const TYPE_ID: StableTypeId =
        StableTypeId::from_stable_type_name("alloc::sync::Arc").combine(T::TYPE_ID.as_bytes());
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn to_hex(input: &[u8; 16]) -> String {
        let mut out = String::with_capacity(64);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for b in *input {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0F) as usize] as char);
        }
        out
    }

    #[test]
    fn test_from_stable_type_name() {
        let foo = StableTypeId::from_stable_type_name("Foo");
        let expected = "1cbec737f863e4922cee63cc2ebbfaaf";
        assert_eq!(to_hex(foo.as_bytes()), expected);
    }

    #[test]
    fn test_combine() {
        let foo = StableTypeId::from_stable_type_name("Foo");
        let bar = StableTypeId::from_stable_type_name("Bar");

        let combined = foo.combine(bar.as_bytes());
        let expected = "c20a2ca5f63fd290f8c3fab454f15f4b";
        assert_eq!(to_hex(combined.as_bytes()), expected);
    }

    #[test]
    fn test_byte_projection() {
        let bytes: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, //
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, //
        ];
        let id = StableTypeId(bytes);
        assert_eq!(*id.as_bytes(), bytes);
        assert_eq!(id.as_u64(), u64::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8]));
        assert_eq!(
            id.as_u128(),
            u128::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        );
    }
}

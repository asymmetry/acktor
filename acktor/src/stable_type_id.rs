//! Stable type identifier.
//!

use std::sync::Arc;

/// A `StableTypeId` represents a stable unique identifier for a type, which is consistent across
/// different compilations in contrast to the standard [`std::any::TypeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableTypeId([u8; 32]);

impl StableTypeId {
    /// Build a `StableTypeId` from a fully-qualified type name by hashing it with SHA-256.
    pub const fn from_stable_type_name(type_name: &'static str) -> Self {
        let result = sha2_const::Sha256::new()
            .update(type_name.as_bytes())
            .finalize();
        StableTypeId(result)
    }

    /// Fold an additional 32-byte chunk into this id, returning the SHA-256 of the concatenation.
    pub const fn combine(self, other: &[u8; 32]) -> Self {
        let result = sha2_const::Sha256::new()
            .update(&self.0)
            .update(other)
            .finalize();
        StableTypeId(result)
    }

    /// Truncate the id to its first 8 bytes interpreted as a `u64`.
    pub const fn as_u64(&self) -> u64 {
        u64::from_le_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7],
        ])
    }

    /// Truncate the id to its first 16 bytes interpreted as a `u128`.
    pub const fn as_u128(&self) -> u128 {
        u128::from_le_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7],
            self.0[8], self.0[9], self.0[10], self.0[11], self.0[12], self.0[13], self.0[14],
            self.0[15],
        ])
    }

    /// Borrow the id's full 32-byte SHA-256 digest.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A trait for types that have a stable unique iidentifier.
///
/// # Implementation
///
/// **Do not implement this trait yourself!** Instead, use
/// [`#[derive(HasStableTypeId)`][acktor_derive::HasStableTypeId].
pub trait HasStableTypeId {
    const STABLE_TYPE_ID: StableTypeId;
}

impl<T> HasStableTypeId for Box<T>
where
    T: HasStableTypeId + ?Sized,
{
    const STABLE_TYPE_ID: StableTypeId = StableTypeId::from_stable_type_name("alloc::boxed::Box")
        .combine(T::STABLE_TYPE_ID.as_bytes());
}

impl<T> HasStableTypeId for Arc<T>
where
    T: HasStableTypeId + ?Sized,
{
    const STABLE_TYPE_ID: StableTypeId = StableTypeId::from_stable_type_name("alloc::sync::Arc")
        .combine(T::STABLE_TYPE_ID.as_bytes());
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn to_hex(input: &[u8; 32]) -> String {
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
        let foo = StableTypeId::from_stable_type_name("acktor::Foo");
        let expected = "75fd38722663cb21ff10fa48dbf98720c0e40c4aa11bb27f6b8bf22b33a230dd";
        assert_eq!(to_hex(foo.as_bytes()), expected);
    }

    #[test]
    fn test_combine() {
        let base = StableTypeId::from_stable_type_name("acktor::Foo");
        let a = StableTypeId::from_stable_type_name("acktor::A");

        let combined = base.combine(a.as_bytes());
        let expected = "88743029fab3a1776ceb9736eb81e97b37ba965356d7a6975718b0754e1a049a";
        assert_eq!(to_hex(combined.as_bytes()), expected);
    }

    #[test]
    fn test_byte_projection() {
        // forge a digest with controlled bytes so the projection arithmetic can be checked
        // independently of any real SHA-256 output
        let bytes = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, //
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, //
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // ignored tail
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //
        ];
        let id = StableTypeId(bytes);

        assert_eq!(*id.as_bytes(), bytes);
        assert_eq!(id.as_u64(), u64::from_le_bytes([1, 2, 3, 4, 5, 6, 7, 8]));
        assert_eq!(
            id.as_u128(),
            u128::from_le_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        );
    }
}

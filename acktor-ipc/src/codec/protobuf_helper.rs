/// Pre-computed proto wire tag bytes for length-delimited (wire type 2) fields, indexed by
/// field number.
///
/// Each entry is `(field << 3) | 2`. Index 0 is unused; index `N` holds the tag byte for
/// field `N`. Field numbers `1..=15` all fit in a single byte — higher numbers would need
/// multi-byte varint encoding and are not covered here.
pub(super) const LENGTH_DELIMITED_TAGS: [u8; 16] = [
    0x00, // index 0 unused
    0x0A, // field 1
    0x12, // field 2
    0x1A, // field 3
    0x22, // field 4
    0x2A, // field 5
    0x32, // field 6
    0x3A, // field 7
    0x42, // field 8
    0x4A, // field 9
    0x52, // field 10
    0x5A, // field 11
    0x62, // field 12
    0x6A, // field 13
    0x72, // field 14
    0x7A, // field 15
];

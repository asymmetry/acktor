use thiserror::Error;

/// Error returned by [`DoubleMap::insert`][super::DoubleMap::insert] and
/// [`DoubleMap::entry`][super::DoubleMap::entry] when one or both of the supplied
/// keys clash with an existing entry in the map.
///
/// The rejected keys are returned in the variant payload so the caller can reuse
/// them without cloning. `insert` additionally returns the rejected value, so its
/// error type is `KeyConflictError<K1, K2, V>`; `entry` has no value to hand back
/// and uses the default `V = ()`.
#[derive(Debug, Error)]
pub enum KeyConflictError<K1, K2, V = ()> {
    /// `key1` is already present in the map (paired with a different `key2`),
    /// while the supplied `key2` is not in the map.
    #[error("key 1 is already present in the map paired with a different key 2")]
    Key1Exists(K1, K2, V),

    /// `key2` is already present in the map (paired with a different `key1`),
    /// while the supplied `key1` is not in the map.
    #[error("key 2 is already present in the map paired with a different key 1")]
    Key2Exists(K1, K2, V),

    /// Both `key1` and `key2` are already present in the map, but in different
    /// entries.
    #[error("both key 1 and key 2 are already present in the map but in different entries")]
    BothKeysExist(K1, K2, V),
}

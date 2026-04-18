use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::Message;
use acktor_ipc::{Decode, Encode};

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[codec(zerocopy)]
#[repr(C)]
#[result_type(())]
pub struct Ping {
    pub id: u64,
    pub timestamp: i64,
}

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[codec(zerocopy)]
#[repr(C)]
#[result_type(())]
pub struct Pong {
    pub id: u64,
    pub timestamp: i64,
}

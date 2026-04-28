use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{Message, MessageId};
use acktor_ipc::{Decode, Encode};

#[derive(
    Debug,
    Clone,
    Copy,
    KnownLayout,
    Immutable,
    FromBytes,
    IntoBytes,
    Message,
    MessageId,
    Encode,
    Decode,
)]
#[codec(zerocopy)]
#[result_type(())]
#[repr(C)]
pub struct Ping {
    pub id: u64,
    pub timestamp: i64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    KnownLayout,
    Immutable,
    FromBytes,
    IntoBytes,
    Message,
    MessageId,
    Encode,
    Decode,
)]
#[codec(zerocopy)]
#[result_type(())]
#[repr(C)]
pub struct Pong {
    pub id: u64,
    pub timestamp: i64,
}

use acktor_derive::Encode;

#[derive(Encode)]
#[codec(prost, Ping)] // bridge type is Self
struct Ping(u64);

fn main() {}

use acktor_derive::Encode;

#[derive(Encode)]
#[codec(prost, Ping)]
struct Ping(u64);

fn main() {}

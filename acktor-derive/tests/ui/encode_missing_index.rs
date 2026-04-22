use acktor_derive::Encode;

#[derive(Encode)]
#[codec(prost)]
struct Ping(u64);

fn main() {}

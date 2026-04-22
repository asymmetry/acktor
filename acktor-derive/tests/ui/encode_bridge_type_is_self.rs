use acktor_derive::Encode;

#[derive(Encode)]
#[codec(prost, Ping)]
#[index(1)]
struct Ping(u64);

fn main() {}

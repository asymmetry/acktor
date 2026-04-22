use acktor_derive::Decode;

#[derive(Decode)]
#[codec(prost)]
#[index = 1]
struct Ping(u64);

fn main() {}

use acktor_derive::Decode;

#[derive(Decode)]
#[codec = "prost"]
struct Ping(u64);

fn main() {}

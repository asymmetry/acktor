use acktor_derive::Decode;

#[derive(Decode)]
#[codec = "prost"] // wrong syntax
struct Ping(u64);

fn main() {}

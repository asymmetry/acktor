use acktor_derive::Encode;

#[derive(Encode)]
#[codec(json)]
#[index(1)]
struct Ping(u64);

fn main() {}

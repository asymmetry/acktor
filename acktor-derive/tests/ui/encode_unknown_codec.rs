use acktor_derive::Encode;

#[derive(Encode)]
#[codec(json)]
struct Ping(u64);

fn main() {}

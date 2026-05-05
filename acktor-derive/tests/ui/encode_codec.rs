use acktor_derive::Encode;

#[derive(Encode)] // missing codec
struct Ping(u64);

#[derive(Encode)]
#[codec(json)] // unknown codec
struct Pong(u64);

fn main() {}

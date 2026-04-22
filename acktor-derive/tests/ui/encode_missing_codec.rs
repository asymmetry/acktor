use acktor_derive::Encode;

#[derive(Encode)]
#[index(1)]
struct Ping(u64);

fn main() {}

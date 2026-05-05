use acktor_derive::StableId;

#[derive(StableId)]
struct Bad<const N: [u8; 4]>; // wrong const generic type

fn main() {}

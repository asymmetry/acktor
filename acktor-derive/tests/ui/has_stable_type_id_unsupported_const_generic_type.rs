use acktor_derive::HasStableTypeId;

#[derive(HasStableTypeId)]
struct Bad<const N: u128>;

fn main() {}

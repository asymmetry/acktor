use acktor_derive::MessageId;

#[derive(MessageId)]
#[custom_id = 42] // wrong syntax
struct Ping;

fn main() {}

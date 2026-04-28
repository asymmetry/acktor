use acktor_derive::MessageId;

#[derive(MessageId)]
#[custom_id = 42]
struct Ping;

fn main() {}

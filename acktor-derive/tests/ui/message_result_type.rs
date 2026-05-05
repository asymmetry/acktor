use acktor_derive::Message;

#[derive(Message)] // missing result type
struct Ping;

#[derive(Message)]
#[result_type = "()"] // wrong syntax
struct Pong;

fn main() {}

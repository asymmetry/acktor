use acktor_derive::remote;

struct MyActor;

trait NotActor {}

#[remote]
impl NotActor for MyActor {}

fn main() {}

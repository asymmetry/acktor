use acktor_derive::remote_actor;

struct MyActor;

trait NotActor {}

#[remote_actor]
impl NotActor for MyActor {}

fn main() {}

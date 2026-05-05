use acktor_derive::remote;

#[remote]
struct NotAnImpl; // not impl

struct MyActor;

trait NotActor {}

#[remote]
impl NotActor for MyActor {} // wrong trait

#[remote]
impl MyActor {} // not a trait

fn main() {}

use acktor_derive::RemoteAddressable;

#[derive(RemoteAddressable)]
#[message()] // empty message list
struct MyActor1;

#[derive(RemoteAddressable)] // missing message list
struct MyActor2;

fn main() {}

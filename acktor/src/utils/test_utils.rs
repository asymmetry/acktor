use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::actor::Actor;
use crate::address::{Address, Mailbox};
use crate::channel::mpsc;
use crate::context::Context;
use crate::envelope::Envelope;
use crate::message::{Handler, Message};

pub fn hash_of<T>(value: &T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug)]
pub struct Dummy;

impl Actor for Dummy {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

#[derive(Debug)]
pub struct Ping(pub u32);

impl Message for Ping {
    type Result = ();
}

impl Handler<Ping> for Dummy {
    type Result = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) {}
}

pub fn make_address(capacity: usize) -> (Address<Dummy>, Mailbox<Dummy>) {
    let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
    (Address::new(tx), Mailbox::new(rx))
}

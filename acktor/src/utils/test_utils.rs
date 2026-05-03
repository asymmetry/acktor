use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[cfg(feature = "ipc")]
use bytes::{Bytes, BytesMut};

use crate::actor::Actor;
use crate::address::{Address, Mailbox};
use crate::channel::mpsc;
#[cfg(feature = "ipc")]
use crate::codec::{Decode, DecodeContext, DecodeError, Encode, EncodeContext, EncodeError};
use crate::context::Context;
use crate::envelope::Envelope;
#[cfg(feature = "identifier")]
use crate::message::MessageId;
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

#[cfg(feature = "identifier")]
impl MessageId for Ping {
    const ID: u64 = 1;
}

#[cfg(feature = "ipc")]
impl Encode for Ping {
    #[inline]
    fn encoded_len(&self) -> usize {
        4
    }

    #[inline]
    fn encode(
        &self,
        buf: &mut BytesMut,
        _ctx: Option<&dyn EncodeContext>,
    ) -> Result<(), EncodeError> {
        buf.extend_from_slice(&self.0.to_le_bytes());
        Ok(())
    }
}

#[cfg(feature = "ipc")]
impl Decode for Ping {
    #[inline]
    fn decode(buf: Bytes, _ctx: Option<&dyn DecodeContext>) -> Result<Self, DecodeError> {
        // used in test only so it is implemented to be infallible
        let mut arr = [0u8; 4];
        let len = buf.len().min(4);
        arr[..len].copy_from_slice(&buf[..len]);
        Ok(Ping(u32::from_le_bytes(arr)))
    }
}

impl Handler<Ping> for Dummy {
    type Result = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Self::Context) {}
}

pub fn make_address(capacity: usize) -> (Address<Dummy>, Mailbox<Dummy>) {
    let (tx, rx) = mpsc::channel::<Envelope<Dummy>>(capacity);
    (Address::new(tx), Mailbox::new(rx))
}

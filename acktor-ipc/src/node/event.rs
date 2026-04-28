use acktor::{Address, Message};

use crate::session::Session;

/// A message which is used to notify the receiver of an IPC session is created or deleted.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    SessionCreated(Address<Session>, String),
    SessionDeleted(Address<Session>),
}

impl Message for NodeEvent {
    type Result = ();
}

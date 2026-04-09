use rustc_hash::FxHashMap as HashMap;

use acktor::{Actor, Handler, Message, Recipient, observer::SubjectActor};

use crate::remote_message::{RemoteMessage, RemoteObserver, RemoteSupervisor};

/// Describes an actor which can receive messages from remote actors.
pub trait RemoteActor: Actor + Handler<RemoteMessage> {}

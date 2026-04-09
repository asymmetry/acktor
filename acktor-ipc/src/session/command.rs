use tokio::sync::oneshot;

use acktor::{Message, Recipient};

use crate::errors::SessionError;
use crate::remote_address::RemoteAddress;
use crate::remote_message::RemoteMessage;

type Result<T> = std::result::Result<T, SessionError>;

/// A command which is used to add a local actor to a session.
#[derive(Debug, Message)]
#[result_type(())]
pub struct AddActor(pub Recipient<RemoteMessage>);

/// A command which is used to remove a local actor from a session.
#[derive(Debug, Message)]
#[result_type(())]
pub struct RemoveActor(pub usize);

/// A command which is used by a local actor to create an actor in a remote node.
#[derive(Debug, Message)]
#[result_type(())]
pub struct CreateRemoteActor {
    pub label: String,
    pub r#type: String,
    pub config: String,
    pub tx: oneshot::Sender<Result<RemoteAddress>>,
}

/// A command which is used by a local actor to get the address of a remote actor.
#[derive(Debug, Message)]
#[result_type(())]
pub struct GetRemoteActor {
    pub label: String,
    pub tx: oneshot::Sender<Result<RemoteAddress>>,
}

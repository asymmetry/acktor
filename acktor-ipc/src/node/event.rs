use acktor::Message;

/// A message which is used to notify the receiver of an IPC session is created or deleted.
#[derive(Debug, Clone, Message)]
#[result_type(())]
pub enum NodeEvent {
    SessionCreated(usize),
    SessionDeleted(usize),
}

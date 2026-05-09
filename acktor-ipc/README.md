# acktor-ipc

Interprocess communication for the [`acktor`](https://github.com/asymmetry/acktor) actor framework.

## About

`acktor-ipc` lets actors in different processes talk to each other over a transport of your choice. It introduces a `Node` actor that owns one or more listeners and per-connection `Session` actors that mediate traffic.

## Installation

```toml
[dependencies]
acktor-ipc = "1.0"
```

`acktor-ipc` already depends on `acktor` with the `ipc` feature enabled.

## Concepts

- **Node** — a long-lived actor that holds the listener(s) for inbound connections, tracks the
  active sessions, and owns the registry of remote-addressable / remote-spawnable actor types.
  Send `Connect<C>` to it to dial out.
- **Session** — a per-connection actor that wraps a single `IpcConnection`, routes inbound
  frames to the right local actor, forwards outbound messages, and correlates request/response
  tags.
- **RemoteAddressable** _(re-exported from `acktor`)_ — derive on an actor (with
  `#[message(M1, M2, ...)]`) to declare the message types it can receive from other processes.
  Messages must implement `MessageId`, `Encode`, and `Decode`; their `Result`s must implement
  `Encode` + `Decode`.
- **RemoteSpawnable** _(re-exported from `acktor`)_ — implement to allow other processes to spawn
  this actor on this node.
- **`#[remote]`** — attribute applied to the `impl Actor for ...` block of a remote-addressable
  actor; required so the actor exposes a remote mailbox.

## Example

A minimal message definition for IPC looks like:

```rust,ignore
use acktor::{Message, MessageId};
use acktor_ipc::{Decode, Encode};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(
    Debug, Clone, Copy,
    KnownLayout, Immutable, FromBytes, IntoBytes,
    Message, MessageId, Encode, Decode,
)]
#[codec(zerocopy)]
#[result_type(())]
#[repr(C)]
pub struct Ping {
    pub id: u64,
    pub timestamp: i64,
}
```

See [`examples/pingpong`](./examples/pingpong) for a complete client/server walkthrough using the
WebSocket transport.

## Feature Flags

Defaults: `derive`, `websocket`.

| Feature     | Purpose                                        |
| ----------- | ---------------------------------------------- |
| `derive`    | Re-exports the `#[remote]` attribute macro.    |
| `pipe`      | Pipe transport (Unix sockets / Windows pipes). |
| `websocket` | WebSocket transport.                           |

`pipe` and `websocket` are not mutually exclusive — enable whichever transports you need.

## License

This project is licensed under [MIT](../LICENSE).

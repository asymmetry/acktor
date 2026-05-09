# Project Notes for Claude

## Workspace Layout

```
acktor/                  # workspace root
├── acktor/              # main library crate (published)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── actor[.rs|/]       # actor trait, execution context trait, lifecycle hooks, identifier
│   │   ├── context.rs         # default actor context implementation
│   │   ├── message[.rs|/]     # message + handler + response traits and helper result types
│   │   ├── address[.rs|/]     # actor handles, type-erased recipients, mailbox, send permits
│   │   ├── envelope[.rs|/]    # type-erased message envelope dispatched into the mailbox
│   │   ├── channel[.rs|/]     # underlying mailbox and one-shot response channels
│   │   ├── codec[.rs|/]       # wire-format encode/decode for IPC messages (feature: ipc)
│   │   ├── error[.rs|/]       # framework-wide error types and reporting
│   │   ├── stable_type_id.rs  # compilation-stable type identifier (feature: identifier)
│   │   ├── cron.rs            # actor that runs a periodic task (feature: cron)
│   │   ├── observer.rs        # observer / subject pattern over actors (feature: observer)
│   │   ├── supervisor.rs      # supervision events and supervisor wiring
│   │   ├── signal.rs          # built-in stop / terminate control message
│   │   └── utils[.rs|/]       # internal helpers shared across modules
│   ├── examples/              # ring
│   └── tests/                 # integration tests (gated by required-features)
├── acktor-derive/       # proc-macro crate: Message, MessageResponse, StableId, MessageId,
│                        # Encode, Decode, RemoteAddressable derives + #[remote] attribute
├── acktor-ipc/          # interprocess communication
│   ├── src/
│   │   ├── lib.rs
│   │   ├── node[.rs|/]        # top-level actor that owns listeners, sessions, and remote registries
│   │   ├── session[.rs|/]     # per-connection actor that mediates traffic over one IPC connection
│   │   ├── remote[.rs|/]      # registries/factories for remote-addressable & remote-spawnable actors (traits themselves live in `acktor`)
│   │   ├── ipc_method[.rs|/]  # IPC transport abstraction with pipe and websocket backends
│   │   ├── actor_ref.rs       # handle to an actor spawned on a remote node
│   │   ├── double_map[.rs|/]  # bi-keyed map used for indexing sessions
│   │   └── error.rs           # IPC-layer error types
│   ├── examples/              # pingpong
│   └── tests/                 # integration tests (gated by required-features)
├── acktor-ipc-proto/    # prost-generated IPC wire protocol + helpers
└── ipc-proto-gen/       # standalone build script that regenerates the prost code (excluded from workspace)
```

## Build & Test Commands

```sh
# Test
RUSTFLAGS="--cfg tokio_unstable" cargo test --all-features --workspace

# Lint
RUSTFLAGS="--cfg tokio_unstable" cargo clippy --all-features --workspace -- -D warnings

# Format check
cargo fmt --all -- --check

# Build docs (requires nightly for docsrs attrs)
RUSTFLAGS="--cfg tokio_unstable" RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features --workspace --no-deps

# Build
RUSTFLAGS="--cfg tokio_unstable" cargo build --workspace --all-features
```

## Key Design Decisions

- Rust edition 2024, MSRV 1.85. No `async_trait` — uses RPITIT instead.
- `Context<A>` is the default `ActorContext`; set `Actor::Context = Context<Self>`.
- Actors are started via `Actor::start(label)` or `Actor::create(label, f)`, returning `(Address<A>, JoinHandle<()>)`.
- `Address<A>` is an enum of `Local | Remote` (Remote variant only when feature `ipc` is enabled).
- `Recipient<M, EP>` is a type-erased address for sending a single message type `M` to any actor.
- Messages are sent through an `Envelope<A>` channel; `EnvelopeProxy` handles the type erasure.
- Default mailbox capacity is 8 (`DEFAULT_MAILBOX_CAPACITY`).
- Actor panics in `pre_start`/`post_start`/`run_loop`/`post_stop` are caught with `catch_unwind`. Pre-start panics propagate to the caller of `start`/`create`; later panics terminate the actor and notify the supervisor (if any) via `SupervisionEvent::Panicked`.

## Feature Flags

### `acktor`

Defaults: `derive`, `observer`, `cron`.

| Feature              | Purpose                                                         |
| -------------------- | --------------------------------------------------------------- |
| `derive`             | Re-exports the derive macros from `acktor-derive`.              |
| `observer`           | Enables the observer module.                                    |
| `cron`               | Enables the cron module.                                        |
| `identifier`         | Enables stable type identifiers.                                |
| `ipc`                | Enables IPC support (codec module, remote addressing).          |
| `prost-codec`        | Use an all-prost primitive codec instead of the zerocopy mix.   |
| `bottleneck-warning` | Logs when an observer's mailbox is full.                        |
| `tokio-tracing`      | Names actor tasks for `tokio-console` (needs `tokio_unstable`). |

### `acktor-derive`

Defaults: none. The `ipc` feature enables the IPC-related derives (`Encode`, `Decode`, `RemoteAddressable`, `#[remote]`).

### `acktor-ipc`

Defaults: `derive`.

| Feature     | Purpose                                        |
| ----------- | ---------------------------------------------- |
| `derive`    | Re-exports the `#[remote]` attribute macro.    |
| `pipe`      | Pipe transport (Unix sockets / Windows pipes). |
| `websocket` | WebSocket transport.                           |

## Derive Macros (`acktor_derive`)

Always available: `Message` (requires `#[result_type(T)]`), `MessageResponse`, `StableId`, `MessageId` (optional `#[custom_id(u64)]`).

IPC-gated: `Encode`, `Decode` (both share `#[codec(prost|serde_json|zerocopy|rkyv)]` with optional bridge type), `RemoteAddressable` (requires `#[message(M1, M2, ...)]`), and the `#[remote]` attribute macro that overrides `Actor::remote_mailbox` on the `impl Actor for ...` block.

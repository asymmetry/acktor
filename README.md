# acktor

[![Crates.io](https://img.shields.io/crates/v/acktor)](https://crates.io/crates/acktor)
[![docs.rs](https://img.shields.io/docsrs/acktor)](https://docs.rs/acktor)
[![CI](https://github.com/asymmetry/acktor/actions/workflows/ci.yml/badge.svg)](https://github.com/asymmetry/acktor/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/asymmetry/acktor/graph/badge.svg?token=DKT26DR5E8)](https://codecov.io/gh/asymmetry/acktor)
[![License: MIT](https://img.shields.io/crates/l/acktor)](LICENSE)

A pure-Rust actor framework built on top of the [Tokio](https://tokio.rs) async runtime, inspired by Alice Ryhl's [Actors with Tokio](https://ryhl.io/blog/actors-with-tokio/).

## About

`acktor` is an actor framework for Rust that builds on the patterns described in Alice Ryhl's blog post and extends them into a structured library. Each actor runs as an independent `tokio` task with its own mailbox, processing messages one at a time. Actors communicate exclusively through message passing — there is no shared mutable state. The framework provides lifecycle hooks, supervision, an observer pattern, and support for periodic tasks.

## Installation

Install `acktor` by adding it to your `Cargo.toml`:

```toml
[dependencies]
acktor = "1.0"
```

Requires Rust 1.85 or later.

## Quick Start

An example `Counter` actor that handles arithmetic messages might be the following:

```rust
use acktor::{Actor, Context, Handler, Message, Signal};

// 1. Define your actor
#[derive(Debug)]
struct Counter(i64);

impl Actor for Counter {
    type Context = Context<Self>;
    type Error = String;
}

// 2. Define a message
#[derive(Debug, Message)]
#[result_type(i64)]
enum CounterMsg {
    Increment,
    Get,
}

// 3. Implement the handler
impl Handler<CounterMsg> for Counter {
    type Result = i64;

    async fn handle(&mut self, msg: CounterMsg, _ctx: &mut Self::Context) -> i64 {
        match msg {
            CounterMsg::Increment => self.0 += 1,
            CounterMsg::Get => {}
        }
        self.0
    }
}

#[tokio::main]
async fn main() {
    let (addr, handle) = Counter(0).start("counter").unwrap();

    // fire-and-forget
    addr.do_send(CounterMsg::Increment).await.unwrap();

    // request-reply
    let result = addr.send(CounterMsg::Get).await.unwrap().await.unwrap();
    println!("Counter: {result}"); // Counter: 1

    addr.do_send(Signal::Stop).await.unwrap();
    handle.await.unwrap();
}
```

## Supervision

Implement `Handler<SupervisionEvent<A>>` on the supervisor actor. Use the command `Supervisor::Set` to attach the supervisor actor to the child (or `Supervisor::Unset` to detach). Since every actor handles `Supervisor<A>` automatically, no extra wiring is needed on the child side.

```rust
use acktor::{Actor, Context, Handler, supervisor::SupervisionEvent};

struct Worker;

impl Actor for Worker {
    type Context = Context<Self>;
    type Error = String;
}

#[derive(Default)]
struct Watchdog;

impl Actor for Watchdog {
    type Context = Context<Self>;
    type Error = String;
}

impl Handler<SupervisionEvent<Worker>> for Watchdog {
    type Result = ();

    async fn handle(&mut self, event: SupervisionEvent<Worker>, _ctx: &mut Self::Context) {
        println!("worker event: {:?}", event);
    }
}
```

## Observer

For a subject actor, implement the `SubjectActor<Event>` trait so it can emit `Event`s to registered observers. Every subject actor automatically gets a `Handler<Observer<Event>>` implementation that manages the observers, so the observers can be registered by sending `Observer::Register` commands to the subject actor (or `Observer::Unregister` to stop receiving events).

```rust
use acktor::{Actor, Context, Message, observer::{ObserverSet, SubjectActor}};

#[derive(Clone, Message)]
#[result_type(())]
struct Tick;

#[derive(Default)]
struct Clock { observers: ObserverSet<Tick> }

impl Actor for Clock {
    type Context = Context<Self>;
    type Error = String;
}

impl SubjectActor<Tick> for Clock {
    fn observers_mut(&mut self) -> &mut ObserverSet<Tick> { &mut self.observers }
}
```

## Cron Tasks

Use `CronContext<Self>` as the actor's context and implement `CronActor` trait to opt in this feature. `CronActor` trait defines a `task` method that is invoked repeatedly with a delay determined by its return value.

```rust
use std::time::Duration;
use acktor::{Actor, cron::{CronActor, CronContext}};

struct Heartbeat;

impl Actor for Heartbeat {
    type Context = CronContext<Self>;
    type Error = String;
}

impl CronActor for Heartbeat {
    async fn task(&mut self, _ctx: &mut Self::Context) -> Result<Duration, Self::Error> {
        println!("tick");
        Ok(Duration::from_secs(1))
    }
}
```

## IPC Support

Actors in different processes can talk to each other through the [`acktor-ipc`](./acktor-ipc) crate. Enable the `ipc` feature on `acktor`, add `acktor-ipc` to your dependencies, mark the actors you want to expose with `#[derive(RemoteAddressable)]` + `#[remote]`, and connect them through a `Node` over a `pipe` or `websocket` transport. See the [`acktor-ipc` README](./acktor-ipc/README.md) and the [`pingpong` example](./acktor-ipc/examples/pingpong) for a full walkthrough.

## Feature Flags

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

## License

This project is licensed under [MIT](LICENSE).

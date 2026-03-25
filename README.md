# acktor

A pure-Rust actor framework built on top of the [Tokio](https://tokio.rs) async runtime, inspired by [Actors with Tokio](https://ryhl.io/blog/actors-with-tokio/) by Alice Ryhl.

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
use acktor::{Actor, Context, Signal, message::{Handler, Message}};

// 1. Define your actor
#[derive(Debug)]
struct Counter(i64);

impl Actor for Counter {
    type Context = Context<Self>;
    type Error = String;
}

// 2. Define a message
#[derive(Debug)]
enum CounterMsg {
    Increment,
    Get,
}

impl Message for CounterMsg {
    type Result = i64;
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
    let (addr, handle) = Counter(0).run("counter").unwrap();

    // fire-and-forget
    addr.do_send(CounterMsg::Increment).await.unwrap();

    // request-reply
    let result = addr.send(CounterMsg::Get).await.unwrap().await.unwrap();
    println!("Counter: {result}"); // Counter: 1

    addr.do_send(Signal::Stop).await.unwrap();
    handle.await.unwrap();
}
```

## License

This project is licensed under [MIT](LICENSE).

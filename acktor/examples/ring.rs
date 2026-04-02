use clap::Parser;
use tokio::time::Instant;

use acktor::{Actor, ActorContext, Address, Context, Handler, Message};

// daisy chain actors together and pass a message around all nodes in the ring to benchmark actor
// performance

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(help = "Number of nodes in the ring")]
    pub nodes: usize,
    #[arg(help = "Number of times to pass the message around the ring")]
    pub rounds: usize,
}

#[derive(Debug)]
pub struct Node {
    id: usize,
    limit: usize,
    next: Address<Node>,
}

impl Actor for Node {
    type Context = Context<Self>;
    type Error = String;
}

#[derive(Debug, Message)]
#[result_type = "()"]
pub struct Payload(usize);

impl Handler<Payload> for Node {
    type Result = ();

    async fn handle(&mut self, msg: Payload, ctx: &mut Self::Context) -> Self::Result {
        if msg.0 >= self.limit {
            println!(
                "Actor {} reached limit of {} (payload was {})",
                self.id, self.limit, msg.0
            );

            ctx.stop();

            return;
        }

        if msg.0 % 498989 == 0 {
            println!(
                "Actor {} received message {} of {} ({:.2}%)",
                self.id,
                msg.0,
                self.limit,
                100.0 * msg.0 as f32 / self.limit as f32
            );
        }

        self.next.do_send(Payload(msg.0 + 1)).await.unwrap();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();

    let n_nodes = args.nodes;
    let n_rounds = args.rounds;

    if n_nodes < 1 {
        println!("Number of nodes must be at least 1");
        return;
    }

    let limit = n_nodes * n_rounds;

    println!("Setting up {n_nodes} nodes");

    let (address, join_handle) = Node::create(format!("Node {}", 0), |ctx| {
        if n_nodes == 1 {
            return Ok(Node {
                id: 0,
                limit,
                next: ctx.address(),
            });
        }

        let (mut next_address, _) = Node {
            id: n_nodes - 1,
            limit,
            next: ctx.address(),
        }
        .run(format!("Node {}", n_nodes - 1))
        .unwrap();

        for id in (1..n_nodes - 1).rev() {
            (next_address, _) = Node {
                id,
                limit,
                next: next_address,
            }
            .run(format!("Node {id}"))
            .unwrap();
        }

        Ok(Node {
            id: 0,
            limit,
            next: next_address,
        })
    })
    .unwrap();

    println!("Sending start message and waiting for termination after {limit} messages...");

    let now = Instant::now();

    address.do_send(Payload(0)).await.unwrap();

    join_handle.await.unwrap();

    let elapsed = now.elapsed();
    println!(
        "Time taken: {}.{:06} seconds ({} msg/second)",
        elapsed.as_secs(),
        elapsed.subsec_micros(),
        (n_nodes * n_rounds * 1000000) as u128 / elapsed.as_micros()
    );
}

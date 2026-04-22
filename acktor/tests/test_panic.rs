use pretty_assertions::assert_eq;

use acktor::{
    Actor, ActorContext, Context, Handler, Message, Signal, supervisor::SupervisionEvent,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum PanicAt {
    #[default]
    Nowhere,
    PreStart,
    PostStart,
    Handler,
    PostStop,
}

#[derive(Debug, Default)]
struct Panicker {
    panic_at: PanicAt,
}

impl Actor for Panicker {
    type Context = Context<Self>;
    type Error = anyhow::Error;

    fn pre_start(&mut self, _ctx: &mut Self::Context) -> Result<(), Self::Error> {
        if self.panic_at == PanicAt::PreStart {
            panic!("pre_start exploded");
        }
        Ok(())
    }

    async fn post_start(&mut self, _ctx: &mut Self::Context) -> Result<(), Self::Error> {
        if self.panic_at == PanicAt::PostStart {
            panic!("post_start exploded");
        }
        Ok(())
    }

    async fn post_stop(&mut self, _ctx: &mut Self::Context) -> Result<(), Self::Error> {
        if self.panic_at == PanicAt::PostStop {
            panic!("post_stop exploded");
        }
        Ok(())
    }
}

#[derive(Debug, Message)]
#[result_type(())]
struct Boom;

impl Handler<Boom> for Panicker {
    type Result = ();

    async fn handle(&mut self, _msg: Boom, _ctx: &mut Self::Context) -> Self::Result {
        panic!("handler exploded");
    }
}

#[derive(Debug, Default)]
struct Watcher {
    panics: Vec<String>,
}

impl Actor for Watcher {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<SupervisionEvent<Panicker>> for Watcher {
    type Result = ();

    async fn handle(
        &mut self,
        msg: SupervisionEvent<Panicker>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        if let SupervisionEvent::Panicked(_, info) = msg {
            self.panics.push(info);
        }
    }
}

#[derive(Debug, Message)]
#[result_type(Vec<String>)]
struct Collect;

impl Handler<Collect> for Watcher {
    type Result = Vec<String>;

    async fn handle(&mut self, _msg: Collect, _ctx: &mut Self::Context) -> Self::Result {
        std::mem::take(&mut self.panics)
    }
}

async fn spawn_actors(
    panic_at: PanicAt,
) -> (
    acktor::Address<Panicker>,
    acktor::JoinHandle<()>,
    acktor::Address<Watcher>,
    acktor::JoinHandle<()>,
) {
    let (watcher_address, watcher_join_handle) = Watcher::default().run("watcher").unwrap();

    let (panicker_address, panicker_join_handle) = Panicker::create("panicker", |ctx| {
        ctx.set_supervisor(Some(watcher_address.clone().into()));
        Ok(Panicker { panic_at })
    })
    .unwrap();

    (
        panicker_address,
        panicker_join_handle,
        watcher_address,
        watcher_join_handle,
    )
}

#[tokio::test]
#[should_panic(expected = "pre_start exploded")]
async fn test_panic_in_pre_start() {
    // `pre_start` runs synchronously before the actor task is spawned. The runtime
    // catches the panic and calls `resume_unwind`, so the panic propagates to the
    // caller (this test) instead of being reported to the supervisor.
    let _ = spawn_actors(PanicAt::PreStart).await;
}

#[tokio::test]
async fn test_panic_in_handler() {
    let (panicker, panicker_join_handle, watcher, _) = spawn_actors(PanicAt::Handler).await;

    panicker.do_send(Boom).await.unwrap();
    panicker_join_handle.await.unwrap();

    let panics = watcher.send(Collect).await.unwrap().await.unwrap();
    assert_eq!(panics.len(), 1);
    assert!(
        panics[0].contains("handler exploded"),
        "unexpected panic message: {}",
        panics[0],
    );
}

#[tokio::test]
async fn test_panic_in_post_start() {
    let (_panicker, panicker_join_handle, watcher, _) = spawn_actors(PanicAt::PostStart).await;

    panicker_join_handle.await.unwrap();

    let panics = watcher.send(Collect).await.unwrap().await.unwrap();
    assert_eq!(panics.len(), 1);
    assert!(
        panics[0].contains("post_start exploded"),
        "unexpected panic message: {}",
        panics[0],
    );
}

#[tokio::test]
async fn test_panic_in_post_stop() {
    let (panicker, panicker_join, watcher, _) = spawn_actors(PanicAt::PostStop).await;

    panicker.do_send(Signal::Terminate).await.unwrap();
    panicker_join.await.unwrap();

    let panics = watcher.send(Collect).await.unwrap().await.unwrap();
    assert_eq!(panics.len(), 1);
    assert!(
        panics[0].contains("post_stop exploded"),
        "unexpected panic message: {}",
        panics[0],
    );
}

use anyhow::Result;
use bytes::BytesMut;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use acktor::{
    Actor, ActorState, Context, Handler, Message, Recipient, SenderId, Signal,
    cron::CronSignal,
    observer::Observer,
    supervisor::{SupervisionEvent, Supervisor},
};
use acktor_ipc::{
    Decode, DecodeContext, Encode, EncodeContext, RemoteActor, RemoteAddress,
    ipc_method::websocket::WebSocketConnection, remote, remote_actor::RemoteActorRegistry,
    remote_message::RemoteSupervisionEvent,
};

mod common;
use common::{connect, pick_free_port, start_client, start_websocket_server};

#[derive(
    Debug, Clone, Copy, KnownLayout, Immutable, FromBytes, IntoBytes, Message, Encode, Decode,
)]
#[result_type(())]
#[codec(zerocopy)]
#[index(1)]
#[repr(C)]
pub struct Ping {
    pub value: u64,
}

#[derive(Debug, RemoteActor)]
#[message(Ping)]
pub struct Probe;

#[remote]
impl Actor for Probe {
    type Context = Context<Self>;
    type Error = anyhow::Error;
}

impl Handler<Ping> for Probe {
    type Result = ();
    async fn handle(&mut self, _: Ping, _: &mut Self::Context) {}
}

// Required so `Address<Probe>` can convert to `Recipient<SupervisionEvent<Probe>>`.
impl Handler<SupervisionEvent<Probe>> for Probe {
    type Result = ();
    async fn handle(&mut self, _: SupervisionEvent<Probe>, _: &mut Self::Context) {}
}

#[tokio::test]
async fn test_codec_with_session() -> Result<()> {
    let port = pick_free_port().await?;
    let bind_addr = format!("127.0.0.1:{port}");
    let endpoint = format!("ws://{bind_addr}");

    let (server, server_join_handle) = start_websocket_server(&bind_addr).await?;
    let (client, client_join_handle) = start_client()?;
    let session = connect::<WebSocketConnection>(&client, endpoint).await?;

    // use an external RemoteActorRegistry so we can check
    let registry = RemoteActorRegistry::with_capacity(8);
    assert!(registry.capacity() >= 8);
    let encode_ctx = EncodeContext::new(registry.clone());
    let decode_ctx = DecodeContext::new(session.clone(), registry.clone());

    let (address, join_handle) = Probe.run("probe")?;
    let expected_index =
        RemoteAddress::REMOTE_FLAG | ((session.index().reverse_bits() >> 1) ^ address.index());

    let verify_registry = || {
        assert_eq!(
            registry.len(),
            1,
            "expected one auto-registered entry after encode"
        );
        assert!(
            registry.contains_index(address.index()),
            "expected address.index() to be registered"
        );
        registry.retain(|_, _| false);
        assert!(registry.is_empty());
    };

    // Signal — no context, no registry interaction.
    for signal in [Signal::Stop, Signal::Terminate] {
        let bytes = signal.encode_to_bytes(None)?;
        assert_eq!(Signal::decode(bytes, None)?, signal);
    }
    assert!(registry.is_empty());

    // CronSignal — no context, no registry interaction.
    for signal in [CronSignal::Pause, CronSignal::Resume] {
        let bytes = signal.encode_to_bytes(None)?;
        assert_eq!(CronSignal::decode(bytes, None)?, signal);
    }
    assert!(registry.is_empty());

    // Address<A>
    assert!(registry.is_empty());
    let bytes = address.encode_to_bytes(Some(&encode_ctx))?;
    let debug_str = format!("{:?}", registry);
    assert_eq!(debug_str, "[6]");
    verify_registry();
    let decoded = RemoteAddress::decode(bytes, Some(&decode_ctx))?;
    assert_eq!(decoded.index(), expected_index);

    // Recipient<M>
    let recipient: Recipient<Ping> = address.clone().into();
    let buf = recipient.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    let decoded: Recipient<Ping> = RemoteAddress::decode(buf, Some(&decode_ctx))?.into();
    assert_eq!(decoded.index(), expected_index);

    // Supervisor<A>::Set — registers the recipient
    let recipient: Recipient<SupervisionEvent<Probe>> = address.clone().into();
    let set = Supervisor::Set(recipient);
    let bytes = set.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match <Supervisor<Probe>>::decode(bytes, Some(&decode_ctx))? {
        Supervisor::Set(r) => assert_eq!(r.index(), expected_index),
        Supervisor::Unset => panic!("expected Supervisor::Set"),
    }

    // Supervisor<A>::Unset — does not register anything
    let unset: Supervisor<Probe> = Supervisor::Unset;
    let bytes = unset.encode_to_bytes(Some(&encode_ctx))?;
    assert!(
        registry.is_empty(),
        "Supervisor::Unset must not touch the registry"
    );
    let decoded = <Supervisor<Probe> as Decode>::decode(bytes, Some(&decode_ctx))?;
    assert!(matches!(decoded, Supervisor::Unset));

    // Observer<M>::Register
    let register: Observer<Ping> = Observer::Register(address.clone().into());
    let bytes = register.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match <Observer<Ping> as Decode>::decode(bytes, Some(&decode_ctx))? {
        Observer::Register(r) => assert_eq!(r.index(), expected_index),
        Observer::Unregister(_) => panic!("expected Observer::Register"),
    }

    // Observer<M>::Unregister
    let unregister: Observer<Ping> = Observer::Unregister(address.clone().into());
    let bytes = unregister.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match <Observer<Ping> as Decode>::decode(bytes, Some(&decode_ctx))? {
        Observer::Unregister(r) => assert_eq!(r.index(), expected_index),
        Observer::Register(_) => panic!("expected Observer::Unregister"),
    }

    // SupervisionEvent<A>::Warn
    let warn: SupervisionEvent<Probe> =
        SupervisionEvent::Warn(address.clone(), anyhow::anyhow!("boom"));
    let bytes = warn.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match RemoteSupervisionEvent::decode(bytes, Some(&decode_ctx))? {
        RemoteSupervisionEvent::Warn(address, error) => {
            assert_eq!(address.index(), expected_index);
            assert_eq!(error, "boom");
        }
        other => panic!("expected Warn, got {other:?}"),
    }

    // SupervisionEvent<A>::Terminated
    let terminated: SupervisionEvent<Probe> =
        SupervisionEvent::Terminated(address.clone(), Some(anyhow::anyhow!("boom")));
    let bytes = terminated.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match RemoteSupervisionEvent::decode(bytes, Some(&decode_ctx))? {
        RemoteSupervisionEvent::Terminated(address, error) => {
            assert_eq!(address.index(), expected_index);
            assert_eq!(error.as_deref(), Some("boom"));
        }
        other => panic!("expected Terminated, got {other:?}"),
    }

    // SupervisionEvent<A>::Panicked
    let panicked: SupervisionEvent<Probe> =
        SupervisionEvent::Panicked(address.clone(), "boom".to_string());
    let bytes = panicked.encode_to_bytes(Some(&encode_ctx))?;
    verify_registry();
    match RemoteSupervisionEvent::decode(bytes, Some(&decode_ctx))? {
        RemoteSupervisionEvent::Panicked(address, info) => {
            assert_eq!(address.index(), expected_index);
            assert_eq!(info, "boom");
        }
        other => panic!("expected Panicked, got {other:?}"),
    }

    // SupervisionEvent<A>::State — exercises the `encode` (not `encode_to_bytes`) path
    let state: SupervisionEvent<Probe> =
        SupervisionEvent::State(address.clone(), ActorState::Running);
    let encoded_len = state.encoded_len();
    let mut bytes = BytesMut::with_capacity(encoded_len);
    state.encode(&mut bytes, Some(&encode_ctx))?;
    let bytes = bytes.freeze();
    verify_registry();
    match RemoteSupervisionEvent::decode(bytes, Some(&decode_ctx))? {
        RemoteSupervisionEvent::State(address, state) => {
            assert_eq!(address.index(), expected_index);
            assert_eq!(state, ActorState::Running);
        }
        other => panic!("expected State, got {other:?}"),
    }

    acktor::utils::terminate_actor(address, join_handle).await;
    acktor::utils::terminate_actor(client, client_join_handle).await;
    acktor::utils::terminate_actor(server, server_join_handle).await;

    Ok(())
}

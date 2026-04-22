use proc_macro::TokenStream;

mod common;

mod decode;
mod encode;
mod message;
mod message_response;
mod remote;
mod remote_actor;

/// Derive the [`Message`] trait for a struct or enum.
///
/// A `#[result_type(..)]` attribute must be present to specify the type returned when the message
/// is handled by an actor.
///
/// # Examples
///
/// ```ignore
/// use acktor_derive::{Message, MessageResponse};
///
/// #[derive(MessageResponse)]
/// struct Sum(i64);
///
/// #[derive(Message)]
/// #[result_type(Sum)]
/// struct Add(i64, i64);
/// ```
///
/// [`Message`]: https://docs.rs/acktor/latest/acktor/message/trait.Message.html
#[proc_macro_derive(Message, attributes(result_type))]
pub fn message_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    message::expand(&ast).into()
}

/// Derive the [`MessageResponse`] trait for a struct or enum.
///
/// This implements the default response handling, which sends the value back through an oneshot
/// channel to the sender of the message.
///
/// # Examples
///
/// ```ignore
/// use acktor_derive::MessageResponse;
///
/// #[derive(MessageResponse)]
/// struct Sum(i64);
///
/// #[derive(Message)]
/// #[result_type(Sum)]
/// struct Add(i64, i64);
/// ```
///
/// [`MessageResponse`]: https://docs.rs/acktor/latest/acktor/message/trait.MessageResponse.html
#[proc_macro_derive(MessageResponse)]
pub fn message_response_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    message_response::expand(&ast).into()
}

/// Derive the [`Encode`] trait for a message.
///
/// A `#[codec(..)]` attribute must be present to select the serialization method and the same
/// attribute is shared with [`Decode`]. Encoding and decoding of the same message type must use
/// the same method. The attribute also supports an optional bridge type that serves as an
/// intermediary for encoding and decoding, which is useful when the message type itself cannot
/// directly implement the required traits. Currently there are three supported codec methods:
///
/// - `#[codec(prost)]` — delegates to [`prost::Message::encode_to_vec`]. The target type (or the
///   bridge type) must implement [`prost::Message`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::IntoBytes::as_bytes`]. The target type (or
///   the bridge type) must implement [`zerocopy::IntoBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::to_bytes`]. The target type (or the bridge type)
///   must implement [`rkyv::Serialize`].
///
/// If a bridge type `T` is specified, the bridge type must be convertible from the target type
/// with `impl From<&Self> for T`.
///
/// A `#[index(N)]` attribute must also be present to set the `Encode::ID` constant with the given
/// `u64` literal. The index in [`Encode`] and [`Decode`] must be the same for the same message
/// type.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::Encode;
///
/// #[derive(zerocopy::IntoBytes, Encode)]
/// #[codec(zerocopy)]
/// #[index(1)]
/// struct Ping(u64);
/// ```
///
/// [`Encode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::encode_to_vec`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.encode_to_vec
/// [`zerocopy::IntoBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html
/// [`zerocopy::IntoBytes::as_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html#method.as_bytes
/// [`rkyv::to_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.to_bytes.html
/// [`rkyv::Serialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Serialize.html
#[proc_macro_derive(Encode, attributes(codec, index))]
pub fn encode_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    encode::expand(&ast).into()
}

/// Derive the [`Decode`] trait for a message.
///
/// A `#[codec(..)]` attribute must be present to select the deserialization method and the same
/// attribute is shared with [`Decode`]. Encoding and decoding of the same message type must use
/// the same method. The attribute also supports an optional bridge type that serves as an
/// intermediary for encoding and decoding, which is useful when the message type itself cannot
/// directly implement the required traits. Currently there are three supported codec methods:
///
/// - `#[codec(prost)]` — delegates to [`prost::Message::decode`]. The target type (or the bridge
///   type) must implement [`prost::Message`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::FromBytes::read_from_bytes`]. The target
///   type (or the bridge type) must implement [`zerocopy::FromBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::from_bytes`]. The target type (or the bridge type)
///   must implement [`rkyv::Archive`] and [`rkyv::Deserialize`].
///
/// If a bridge type `T` is specified, the target type must be convertible from the bridge type
/// with `impl TryFrom<T> for Self` and use [`DecodeError`] as the error type.
///
/// A `#[index(N)]` attribute must also be present to set the `Decode::ID` constant with the given
/// `u64` literal. The index in [`Encode`] and [`Decode`] must be the same for the same message
/// type.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::Decode;
///
/// #[derive(zerocopy::FromBytes, Decode)]
/// #[codec(zerocopy)]
/// #[index(1)]
/// struct Ping(u64);
/// ```
///
/// [`Encode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::decode`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.decode
/// [`zerocopy::FromBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html
/// [`zerocopy::FromBytes::read_from_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html#method.read_from_bytes
/// [`rkyv::from_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.from_bytes.html
/// [`rkyv::Archive`]: https://docs.rs/rkyv/latest/rkyv/trait.Archive.html
/// [`rkyv::Deserialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Deserialize.html
/// [`DecodeError`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/errors/enum.DecodeError.html
#[proc_macro_derive(Decode, attributes(codec, index))]
pub fn decode_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    decode::expand(&ast).into()
}

/// Derive the [`RemoteActor`] trait for an actor.
///
/// Without any attribute, only the marker `impl RemoteActor for Self {}` is emitted.
///
/// With an optional `#[message(M1, M2, ...)]` attribute, an additional
/// `impl Handler<RemoteMessage> for Self` is emitted which dispatches inbound messages by
/// matching their `message_id` against `<Mi as Decode>::ID` and invoking the corresponding
/// message handler `<Self as Handler<Mi>>::handle`. After handling the message, the response is
/// encoded and sent back through an oneshot channel to the sender of the [`RemoteMessage`].
///
/// For each `Mi`, the actor must implement [`Handler<Mi>`] trait and the result type of the
/// trait must implement [`Encode`] trait.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::RemoteActor;
///
/// #[derive(RemoteActor)]
/// #[message(Ping, Echo)]
/// pub struct MyActor;
/// ```
///
/// [`RemoteActor`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/remote_actor/trait.RemoteActor.html
/// [`RemoteMessage`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/remote_message/struct.RemoteMessage.html
/// [`Handler<Mi>`]: https://docs.rs/acktor/latest/acktor/message/trait.Handler.html
/// [`Encode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Encode.html
#[proc_macro_derive(RemoteActor, attributes(message))]
pub fn remote_actor_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    remote_actor::expand(&ast).into()
}

/// Attribute macro applies to the `impl Actor for MyActor { ... }` block, which overrides the
/// [`Actor::type_erased_recipient_fn`] used by `acktor-ipc` with a custom implementation that
/// converts [`Address<Self>`] to `Recipient<RemoteMessage>` first and then erases the type.
///
/// See the documentation of [`Actor::type_erased_recipient_fn`] for more details.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::remote;
///
/// #[remote]
/// impl Actor for MyActor {
///     type Error = anyhow::Error;
///     type Context = Context<Self>;
/// }
/// ```
///
/// [`Actor::type_erased_recipient_fn`]: https://docs.rs/acktor/latest/acktor/trait.Actor.html#method.type_erased_recipient_fn
/// [`Address<Self>`]: https://docs.rs/acktor/latest/acktor/address/struct.Address.html
#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    remote::expand(item.into()).into()
}

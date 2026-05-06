#![cfg_attr(docsrs, feature(doc_cfg))]

use proc_macro::TokenStream;

mod message;
mod message_id;
mod message_response;
mod stable_id;

#[cfg(feature = "ipc")]
mod common;
#[cfg(feature = "ipc")]
mod decode;
#[cfg(feature = "ipc")]
mod encode;
#[cfg(feature = "ipc")]
mod remote;
#[cfg(feature = "ipc")]
mod remote_addressable;

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

/// Derive the [`StableId`] trait for a type.
///
/// The generated `TYPE_ID` is the first 16 bytes of the SHA-256 digest of the type's
/// fully-qualified path (`module_path!() + "::" + ident`).
///
/// If the type contains type generic parameters, the generated `TYPE_ID` is combined with each
/// type generic parameter's `TYPE_ID` with [`StableTypeId::combine`] in their declaration order.
///
/// If the type contains const generic parameters, the generated `TYPE_ID` is combined with the
/// first 16 bytes of the SHA-256 digest of the big-endian form of each const generic parameter
/// with [`StableTypeId::combine`] in their declaration order.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::StableId;
///
/// #[derive(StableId)]
/// struct Ping(u64);
/// ```
///
/// [`StableId`]: https://docs.rs/acktor/latest/acktor/stable_type_id/trait.StableId.html
/// [`StableTypeId::combine`]: https://docs.rs/acktor/latest/acktor/stable_type_id/struct.StableTypeId.html#method.combine
#[proc_macro_derive(StableId)]
pub fn stable_id_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    stable_id::expand(&ast).into()
}

/// Derive the [`MessageId`] trait for a [`Message`].
///
/// By default, the derive also emits a [`StableId`] impl and sets
/// `MessageId::ID = StableId::TYPE_ID.as_u64()`. In this case, do **not** also derive
/// [`StableId`] separately, as that would produce conflicting impls. See derive macro
/// [`StableId`][macro@StableId] for the hashing scheme and the rules around generic parameters.
///
/// An optional `#[custom_id(<u64 value>)]` attribute lets the user supply the id directly. When
/// present, no [`StableId`] impl is emitted, and it is the user's responsibility to ensure the
/// id is unique across all messages an actor can handle.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::MessageId;
///
/// #[derive(MessageId)]
/// struct Ping(u64);
///
/// #[derive(MessageId)]
/// #[custom_id(0xdead_beef)]
/// struct Pong;
/// ```
///
/// [`MessageId`]: https://docs.rs/acktor/latest/acktor/message/trait.MessageId.html
/// [`Message`]: https://docs.rs/acktor/latest/acktor/message/trait.Message.html
/// [`StableId`]: https://docs.rs/acktor/latest/acktor/stable_type_id/trait.StableId.html
#[proc_macro_derive(MessageId, attributes(custom_id))]
pub fn message_id_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    message_id::expand(&ast).into()
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
/// - `#[codec(serde_json)]` — delegates to [`serde_json::to_vec`]. The target type (or the bridge
///   type) must implement [`serde::Serialize`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::IntoBytes::as_bytes`]. The target type (or
///   the bridge type) must implement [`zerocopy::IntoBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::to_bytes`]. The target type (or the bridge type)
///   must implement [`rkyv::Serialize`].
///
/// If a bridge type `T` is specified, the bridge type must be convertible from the target type
/// with `impl From<&Self> for T`.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::Encode;
///
/// #[derive(zerocopy::IntoBytes, Encode)]
/// #[codec(zerocopy)]
/// struct Ping(u64);
/// ```
///
/// [`Encode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::encode_to_vec`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.encode_to_vec
/// [`serde::Serialize`]: https://docs.rs/serde/latest/serde/ser/trait.Serialize.html
/// [`serde_json::to_vec`]: https://docs.rs/serde_json/latest/serde_json/fn.to_vec.html
/// [`zerocopy::IntoBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html
/// [`zerocopy::IntoBytes::as_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html#method.as_bytes
/// [`rkyv::to_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.to_bytes.html
/// [`rkyv::Serialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Serialize.html
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
#[proc_macro_derive(Encode, attributes(codec))]
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
/// - `#[codec(serde_json)]` — delegates to [`serde_json::from_slice`]. The target type (or the
///   bridge type) must implement [`serde::Deserialize`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::FromBytes::read_from_bytes`]. The target
///   type (or the bridge type) must implement [`zerocopy::FromBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::from_bytes`]. The target type (or the bridge type)
///   must implement [`rkyv::Archive`] and [`rkyv::Deserialize`].
///
/// If a bridge type `T` is specified, the target type must be convertible from the bridge type
/// with `impl TryFrom<T> for Self` and use [`DecodeError`] as the error type.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::Decode;
///
/// #[derive(zerocopy::FromBytes, Decode)]
/// #[codec(zerocopy)]
/// struct Ping(u64);
/// ```
///
/// [`Encode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::decode`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.decode
/// [`serde::Deserialize`]: https://docs.rs/serde/latest/serde/de/trait.Deserialize.html
/// [`serde_json::from_slice`]: https://docs.rs/serde_json/latest/serde_json/fn.from_slice.html
/// [`zerocopy::FromBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html
/// [`zerocopy::FromBytes::read_from_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html#method.read_from_bytes
/// [`rkyv::from_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.from_bytes.html
/// [`rkyv::Archive`]: https://docs.rs/rkyv/latest/rkyv/trait.Archive.html
/// [`rkyv::Deserialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Deserialize.html
/// [`DecodeError`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/errors/enum.DecodeError.html
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
#[proc_macro_derive(Decode, attributes(codec))]
pub fn decode_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    decode::expand(&ast).into()
}

/// Derive the [`RemoteAddressable`] trait for an actor.
///
/// A `#[message(M1, M2, ...)]` attribute must be present to specify the list of messages the
/// actor can handle remotely. For each message `Mi`, the actor must have implemented the
/// [`Handler`] trait; `Mi` itself must have implemented the [`MessageId`] trait, the [`Encode`]
/// trait and the [`Decode`] trait; `Mi::Result` must also have implemented the [`Encode`] trait
/// and the [`Decode`] trait.
///
/// The macro emits a [`Codec`] impl and a `Handler<BinaryMessage>` impl for the actor based on
/// the message list specified in the `#[message(..)]` attribute. The [`Codec`] impl provides a
/// codec table which defines how to encode the message and decode the message response for each
/// message type `Mi`. The `Handler<BinaryMessage>` impl dispatches inbound messages by matching
/// the message id and invoking the corresponding message handler.
///
/// # Example
///
/// ```ignore
/// use acktor_derive::RemoteAddressable;
///
/// #[derive(RemoteAddressable)]
/// #[message(Ping, Echo)]
/// pub struct MyActor;
/// ```
///
/// [`RemoteAddressable`]: https://docs.rs/acktor/latest/acktor/actor/remote/trait.RemoteAddressable.html
/// [`Handler`]: https://docs.rs/acktor/latest/acktor/message/trait.Handler.html
/// [`MessageId`]: https://docs.rs/acktor/latest/acktor/message/index/trait.MessageId.html
/// [`Encode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor/latest/acktor/codec/trait.Decode.html
/// [`Codec`]: https://docs.rs/acktor/latest/acktor/codec/table/trait.Codec.html
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
#[proc_macro_derive(RemoteAddressable, attributes(message))]
pub fn remote_addressable_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    remote_addressable::expand(&ast).into()
}

/// Attribute macro applies to the `impl Actor for MyActor` block, which overrides the internal
/// method `Actor::remote_mailbox` to return a [`RemoteMailbox`] for a remote addressable actor.
///
/// This is a temporary workaround since specialization is not yet stable in Rust.
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
/// [`RemoteMailbox`]: https://docs.rs/acktor/latest/acktor/address/type.RemoteMailbox.html
#[cfg(feature = "ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "ipc")))]
#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    remote::expand(item.into()).into()
}

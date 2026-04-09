use proc_macro::TokenStream;

mod decode;
mod detect_backend;
mod encode;
mod message;
mod message_response;

/// Derive the [`Message`] trait for a struct or enum.
///
/// The `result_type` attribute is required and specifies the type returned
/// when the message is handled by an actor.
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
/// This implements the default response handling, which sends the value
/// back through the oneshot channel to the caller.
///
/// # Examples
///
/// ```ignore
/// use acktor_derive::MessageResponse;
///
/// #[derive(MessageResponse)]
/// struct Sum(i64);
///
/// #[derive(MessageResponse)]
/// enum Status {
///     Ok,
///     Error(String),
/// }
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
/// A `#[codec(..)]` attribute must be present to select the serialization backend.
/// The same attribute is shared with [`Decode`] — encoding and decoding of the same type
/// must use the same backend, so there is no need to distinguish them:
///
/// - `#[codec(prost)]` — delegates to [`prost::Message::encode_to_vec`]. The target
///   type must also implement [`prost::Message`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::IntoBytes::as_bytes`]. The target
///   type must also implement [`zerocopy::IntoBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::to_bytes`]. The target type must also
///   implement [`rkyv::Serialize`].
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
/// [`Encode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::encode_to_vec`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.encode_to_vec
/// [`zerocopy::IntoBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html
/// [`zerocopy::IntoBytes::as_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.IntoBytes.html#method.as_bytes
/// [`rkyv::to_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.to_bytes.html
/// [`rkyv::Serialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Serialize.html
#[proc_macro_derive(Encode, attributes(codec))]
pub fn encode_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    encode::expand(&ast).into()
}

/// Derive the [`Decode`] trait for a message.
///
/// A `#[codec(..)]` attribute must be present to select the deserialization backend.
/// The same attribute is shared with [`Encode`] — encoding and decoding of the same type
/// must use the same backend, so there is no need to distinguish them:
///
/// - `#[codec(prost)]` — delegates to [`prost::Message::decode`]. The target type
///   must also implement [`prost::Message`].
/// - `#[codec(zerocopy)]` — delegates to [`zerocopy::FromBytes::read_from_bytes`].
///   The target type must also implement [`zerocopy::FromBytes`].
/// - `#[codec(rkyv)]` — delegates to [`rkyv::from_bytes`]. The target type must also
///   implement [`rkyv::Archive`] + [`rkyv::Deserialize`].
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
/// [`Encode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Encode.html
/// [`Decode`]: https://docs.rs/acktor-ipc/latest/acktor_ipc/codec/trait.Decode.html
/// [`prost::Message`]: https://docs.rs/prost/latest/prost/trait.Message.html
/// [`prost::Message::decode`]: https://docs.rs/prost/latest/prost/trait.Message.html#method.decode
/// [`zerocopy::FromBytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html
/// [`zerocopy::FromBytes::read_from_bytes`]: https://docs.rs/zerocopy/latest/zerocopy/trait.FromBytes.html#method.read_from_bytes
/// [`rkyv::from_bytes`]: https://docs.rs/rkyv/latest/rkyv/fn.from_bytes.html
/// [`rkyv::Archive`]: https://docs.rs/rkyv/latest/rkyv/trait.Archive.html
/// [`rkyv::Deserialize`]: https://docs.rs/rkyv/latest/rkyv/trait.Deserialize.html
#[proc_macro_derive(Decode, attributes(codec))]
pub fn decode_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    decode::expand(&ast).into()
}

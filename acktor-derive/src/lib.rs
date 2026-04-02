use proc_macro::TokenStream;
use syn::DeriveInput;

mod message;
mod message_response;

/// Derive the [`Message`] trait for a struct or enum.
///
/// The `result_type` attribute is required and specifies the type returned
/// when the message is handled by an actor.
///
/// # Examples
///
/// ```
/// use acktor_derive::{Message, MessageResponse};
///
/// #[derive(MessageResponse)]
/// struct Sum(i64);
///
/// #[derive(Message)]
/// #[result_type = "Sum"]
/// struct Add(i64, i64);
/// ```
#[proc_macro_derive(Message, attributes(result_type))]
pub fn message_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();

    message::expand(&ast).into()
}

/// Derive the [`MessageResponse`] trait for a struct or enum.
///
/// This implements the default response handling, which sends the value
/// back through the oneshot channel to the caller.
///
/// # Examples
///
/// ```
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
#[proc_macro_derive(MessageResponse)]
pub fn message_response_derive(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();

    message_response::expand(&ast).into()
}

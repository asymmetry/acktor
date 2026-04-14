use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, parse2};

pub fn expand(input: TokenStream) -> TokenStream {
    let mut item: ItemImpl = match parse2(input) {
        Ok(item) => item,
        Err(err) => return err.to_compile_error(),
    };

    let shim_method: syn::ImplItem = syn::parse_quote! {
        fn erased_recipient_fn() -> ::core::option::Option<
            ::acktor::ErasedRecipientFn<Self>,
        > {
            ::core::option::Option::Some(|addr: &::acktor::Address<Self>| {
                let recipient: ::acktor::Recipient<::acktor_ipc::RemoteMessage> =
                    ::core::convert::From::from(
                        <::acktor::Address<Self> as ::core::clone::Clone>::clone(addr),
                    );
                ::std::boxed::Box::new(recipient)
            })
        }
    };
    item.items.push(shim_method);

    quote! { #item }
}

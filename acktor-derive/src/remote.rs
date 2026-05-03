use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, parse2};

pub fn expand(input: TokenStream) -> TokenStream {
    let mut item: ItemImpl = match parse2(input) {
        Ok(item) => item,
        Err(err) => return err.to_compile_error(),
    };

    match &item.trait_ {
        Some((_, path, _)) if path.segments.last().is_some_and(|s| s.ident == "Actor") => {}
        Some((_, path, _)) => {
            return syn::Error::new_spanned(
                path,
                "#[remote] must be applied to an `impl Actor for ..` block",
            )
            .to_compile_error();
        }
        None => {
            return syn::Error::new_spanned(
                &item.self_ty,
                "#[remote] must be applied to an `impl Actor for ..` block",
            )
            .to_compile_error();
        }
    }

    let shim_method: syn::ImplItem = syn::parse_quote! {
        fn remote_mailbox(
            address: ::acktor::Address<Self>
        ) -> ::core::option::Option<::acktor::actor::RemoteMailbox<Self>> {
            ::core::option::Option::Some(::core::convert::Into::into(address))
        }
    };
    item.items.push(shim_method);

    quote! { #item }
}

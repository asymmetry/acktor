use proc_macro2::TokenStream;
use quote::quote;
use syn::{Token, Type, punctuated::Punctuated};

fn parse_message_list(ast: &syn::DeriveInput) -> syn::Result<Option<Vec<Type>>> {
    let Some(attr) = ast.attrs.iter().find(|a| a.path().is_ident("message")) else {
        return Ok(None);
    };

    let list = attr.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
    if list.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "`#[message(...)]` requires at least one message type; \
             omit the attribute to use the marker-only derive",
        ));
    }
    Ok(Some(list.into_iter().collect()))
}

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let messages = match parse_message_list(ast) {
        Ok(list) => list,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let marker_impl = quote! {
        impl #impl_generics ::acktor_ipc::RemoteActor for #name #ty_generics #where_clause {}
    };

    let Some(messages) = messages else {
        return marker_impl;
    };

    let arms = messages.iter().map(|m| {
        quote! {
            <#m as ::acktor_ipc::Decode>::ID => {
                match <#m as ::acktor_ipc::Decode>::decode(message, decode_context.as_ref()) {
                    ::core::result::Result::Ok(decoded) => {
                        let result = <Self as ::acktor::Handler<#m>>::handle(self, decoded, ctx).await;
                        if let ::core::option::Option::Some(tx) = result_tx {
                            match ::acktor_ipc::Encode::encode_to_bytes(
                                &result,
                                encode_ctx.as_ref(),
                            ) {
                                ::core::result::Result::Ok(bytes) => {
                                    let _ = tx.send(bytes);
                                }
                                ::core::result::Result::Err(e) => {
                                    let _ = tx.send_err(e);
                                }
                            }
                        }
                    }
                    ::core::result::Result::Err(e) => {
                        if let ::core::option::Option::Some(tx) = result_tx {
                            let _ = tx.send_err(e);
                        } else {
                            ::acktor_ipc::tracing::debug!(
                                error = %e,
                                message_id = message_id,
                                "discarding remote message decode error (no result_tx)",
                            );
                        }
                    }
                }
            }
        }
    });

    let ids = messages.iter().map(|m| {
        quote! { <#m as ::acktor_ipc::Decode>::ID }
    });
    let n = messages.len();
    let uniqueness_check = quote! {
        const _: () = {
            const IDS: [u64; #n] = [#(#ids),*];
            let mut i = 0;
            while i < IDS.len() {
                let mut j = i + 1;
                while j < IDS.len() {
                    assert!(
                        IDS[i] != IDS[j],
                        "duplicate message ids in #[message(...)]",
                    );
                    j += 1;
                }
                i += 1;
            }
        };
    };

    let handler_impl = quote! {
        impl #impl_generics ::acktor::Handler<::acktor_ipc::RemoteMessage> for #name #ty_generics #where_clause {
            type Result = ();

            async fn handle(
                &mut self,
                msg: ::acktor_ipc::RemoteMessage,
                ctx: &mut <Self as ::acktor::Actor>::Context,
            ) -> Self::Result {
                let ::acktor_ipc::RemoteMessage {
                    message_id,
                    message,
                    result_tx,
                    decode_context,
                    ..
                } = msg;
                let encode_ctx = decode_context
                    .as_ref()
                    .map(|c| c.create_encode_context());

                match message_id {
                    #(#arms)*
                    _ => {
                        if let ::core::option::Option::Some(tx) = result_tx {
                            let _ = tx.send_err(
                                ::acktor_ipc::errors::DecodeError::UnknownMessageId(message_id),
                            );
                        }
                    }
                }
            }
        }
    };

    quote! {
        #marker_impl
        #uniqueness_check
        #handler_impl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(src: &str) -> syn::DeriveInput {
        syn::parse_str(src).unwrap()
    }

    #[test]
    fn no_attribute_returns_none() {
        let ast = input("struct Foo;");
        assert!(parse_message_list(&ast).unwrap().is_none());
    }

    #[test]
    fn non_empty_attribute_returns_types() {
        let ast = input("#[message(Ping, Echo)] struct Foo;");
        let list = parse_message_list(&ast).unwrap().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn generic_path_is_accepted() {
        let ast = input("#[message(Observer<Pong>)] struct Foo;");
        let list = parse_message_list(&ast).unwrap().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn empty_attribute_is_rejected() {
        let ast = input("#[message()] struct Foo;");
        match parse_message_list(&ast) {
            Err(err) => {
                assert!(err.to_string().contains("at least one message type"));
            }
            Ok(_) => panic!("expected error for empty message attribute"),
        }
    }
}

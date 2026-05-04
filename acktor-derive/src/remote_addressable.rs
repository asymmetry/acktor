use proc_macro2::TokenStream;
use quote::quote;
use syn::{Token, Type, punctuated::Punctuated};

use crate::stable_id;

fn parse_message_list(ast: &syn::DeriveInput) -> syn::Result<Option<Vec<Type>>> {
    let Some(attr) = ast.attrs.iter().find(|a| a.path().is_ident("message")) else {
        return Ok(None);
    };

    let list = attr.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
    if list.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "`#[message(..)]` requires at least one message type; omit the attribute to use the \
             marker-only derive",
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

    // also emit the `StableId` impl so `#[derive(RemoteAddressable)]` alone is enough
    let has_stable_type_id_impl = stable_id::expand(ast);

    let marker_impl = quote! {
        impl #impl_generics ::acktor::RemoteAddressable for #name #ty_generics #where_clause {}

        #has_stable_type_id_impl
    };

    let Some(messages) = messages else {
        return marker_impl;
    };

    let arms = messages.iter().map(|m| {
        quote! {
            <#m as ::acktor::MessageId>::ID => {
                match <#m as ::acktor::codec::Decode>::decode(
                    bytes,
                    decode_msg_ctx.as_deref(),
                ) {
                    ::core::result::Result::Ok(msg) => {
                        let result =
                            <Self as ::acktor::Handler<#m>>::handle(self, msg, ctx).await;
                        if let ::core::option::Option::Some(tx) = result_tx {
                            send_result(tx, &result, encode_res_ctx.as_deref());
                        }
                    }
                    ::core::result::Result::Err(e) => {
                        ::acktor::tracing::debug!(
                            "Could not decode the message: {}", ::acktor::ErrorReport::report(&e)
                        );
                        if let ::core::option::Option::Some(tx) = result_tx {
                            send_err(tx, e);
                        }
                    }
                }
            }
        }
    });

    let ids = messages.iter().map(|m| {
        quote! { <#m as ::acktor::MessageId>::ID }
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

    let codec_impl = quote! {
        impl #impl_generics ::acktor::codec::Codec for #name #ty_generics #where_clause {
            fn codec_table() -> &'static ::acktor::codec::CodecTable {
                static TABLE: ::std::sync::OnceLock<::acktor::codec::CodecTable> =
                    ::std::sync::OnceLock::new();
                TABLE.get_or_init(|| {
                    let mut map: ::acktor::utils::TypeMap<::acktor::codec::MessageCodec> =
                        ::std::default::Default::default();
                    #(
                        map.insert(
                            ::std::any::TypeId::of::<#messages>(),
                            ::acktor::codec::MessageCodec {
                                message_id: <#messages as ::acktor::MessageId>::ID,
                                encode_msg: |any, ctx| {
                                    let m = any.downcast_ref::<#messages>()
                                        .expect("TypeId invariant");
                                    ::acktor::codec::Encode::encode_to_bytes(m, ctx)
                                },
                                decode_res: |bytes, ctx| {
                                    let r = <<#messages as ::acktor::Message>::Result
                                        as ::acktor::codec::Decode>::decode(bytes, ctx)?;
                                    ::core::result::Result::Ok(
                                        ::std::boxed::Box::new(r)
                                            as ::std::boxed::Box<dyn ::std::any::Any>
                                    )
                                },
                            },
                        );
                    )*
                    ::acktor::codec::CodecTable::new(map)
                })
            }
        }
    };

    let handler_impl = quote! {
        impl #impl_generics ::acktor::Handler<::acktor::BinaryMessage>
            for #name #ty_generics #where_clause
        {
            type Result = ();

            async fn handle(
                &mut self,
                msg: ::acktor::BinaryMessage,
                ctx: &mut <Self as ::acktor::Actor>::Context,
            ) -> Self::Result {
                let ::acktor::BinaryMessage {
                    message_id,
                    bytes,
                    result_tx,
                    decode_msg_ctx,
                    encode_res_ctx,
                    ..
                } = msg;

                #[inline]
                fn send_err(
                    tx: ::acktor::channel::oneshot::Sender<::acktor::bytes::Bytes>,
                    err: impl ::core::convert::Into<::acktor::error::BoxError>,
                ) {
                    if let Err(e) = tx.send_err(err) {
                        ::acktor::tracing::debug!(
                            "Could not report the error to the sender: {}",
                            ::acktor::ErrorReport::report(&e)
                        );
                    }
                }

                #[inline]
                fn send_result(
                    tx: ::acktor::channel::oneshot::Sender<::acktor::bytes::Bytes>,
                    result: &impl ::acktor::codec::Encode,
                    encode_ctx: ::core::option::Option<&dyn ::acktor::codec::EncodeContext>,
                ) {
                    match ::acktor::codec::Encode::encode_to_bytes(result, encode_ctx) {
                        ::core::result::Result::Ok(bytes) => {
                            if let Err(e) = tx.send(bytes) {
                                ::acktor::tracing::debug!(
                                    "Could not send the message response to the sender: {}",
                                    ::acktor::ErrorReport::report(&e)
                                );
                            }
                        }
                        ::core::result::Result::Err(e) => {
                            ::acktor::tracing::debug!(
                                "Could not encode the message response: {}",
                                ::acktor::ErrorReport::report(&e)
                            );
                            send_err(tx, e);
                        }
                    }
                }

                match message_id {
                    #(#arms)*
                    _ => {
                        ::acktor::tracing::debug!(
                            "Received a message with unknown message id {}",
                            message_id
                        );
                        if let ::core::option::Option::Some(tx) = result_tx {
                            send_err(
                                tx,
                                ::acktor::codec::DecodeError::UnknownMessageId(message_id),
                            );
                        }
                    }
                }
            }
        }
    };

    quote! {
        #marker_impl
        #codec_impl
        #handler_impl
        #uniqueness_check
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    fn input(src: &str) -> syn::DeriveInput {
        syn::parse_str(src).unwrap()
    }

    #[test]
    fn test_parse_message_list() -> Result<()> {
        // absent attribute → None
        assert!(parse_message_list(&input("struct Foo;"))?.is_none());

        // list of plain types
        let list = parse_message_list(&input("#[message(Ping, Echo)] struct Foo;"))?.unwrap();
        assert_eq!(list.len(), 2);

        // generic paths are accepted
        let list = parse_message_list(&input("#[message(Observer<Pong>)] struct Foo;"))?.unwrap();
        assert_eq!(list.len(), 1);

        Ok(())
    }
}

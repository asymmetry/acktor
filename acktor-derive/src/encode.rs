use proc_macro2::TokenStream;
use quote::quote;

use crate::detect_backend::{Backend, detect_backend};

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let config = match detect_backend(ast) {
        Ok(config) => config,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let (encoded_len_body, encode_body) = match (config.backend, config.bridge) {
        (Backend::Prost, None) => (
            quote! { ::prost::Message::encoded_len(self) },
            quote! {
                buf.extend_from_slice(&::prost::Message::encode_to_vec(self));
                ::core::result::Result::Ok(())
            },
        ),
        (Backend::Prost, Some(bridge)) => (
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                ::prost::Message::encoded_len(&bridge)
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                buf.extend_from_slice(&::prost::Message::encode_to_vec(&bridge));
                ::core::result::Result::Ok(())
            },
        ),
        (Backend::Zerocopy, None) => (
            quote! { ::core::mem::size_of::<Self>() },
            quote! {
                buf.extend_from_slice(
                    <Self as ::zerocopy::IntoBytes>::as_bytes(self),
                );
                ::core::result::Result::Ok(())
            },
        ),
        (Backend::Zerocopy, Some(bridge)) => (
            quote! {
                ::core::mem::size_of::<#bridge>()
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                buf.extend_from_slice(
                    <#bridge as ::zerocopy::IntoBytes>::as_bytes(&bridge),
                );
                ::core::result::Result::Ok(())
            },
        ),
        (Backend::Rkyv, None) => (
            quote! { 0 },
            quote! {
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(self) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor_ipc::errors::EncodeError::from(err.to_string()),
                    ),
                }
            },
        ),
        (Backend::Rkyv, Some(bridge)) => (
            quote! { 0 },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(&bridge) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor_ipc::errors::EncodeError::from(err.to_string()),
                    ),
                }
            },
        ),
    };

    quote! {
        impl #impl_generics ::acktor_ipc::Encode for #name #ty_generics #where_clause {
            #[inline]
            fn encoded_len(&self) -> usize {
                #encoded_len_body
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut ::acktor_ipc::bytes::BytesMut,
            ) -> ::core::result::Result<(), ::acktor_ipc::errors::EncodeError> {
                #encode_body
            }
        }
    }
}

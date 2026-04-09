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

    let body = match (config.backend, config.bridge) {
        (Backend::Prost, None) => quote! {
            <Self as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (Backend::Prost, Some(bridge)) => quote! {
            let bridge = <#bridge as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (Backend::Zerocopy, None) => quote! {
            <Self as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (Backend::Zerocopy, Some(bridge)) => quote! {
            let bridge = <#bridge as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (Backend::Rkyv, None) => quote! {
            match ::rkyv::from_bytes::<Self, ::rkyv::rancor::Error>(&buf[..]) {
                ::core::result::Result::Ok(value) => ::core::result::Result::Ok(value),
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor_ipc::errors::DecodeError::from(err.to_string()),
                ),
            }
        },
        (Backend::Rkyv, Some(bridge)) => quote! {
            match ::rkyv::from_bytes::<#bridge, ::rkyv::rancor::Error>(&buf[..]) {
                ::core::result::Result::Ok(bridge) => {
                    <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                        .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
                }
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor_ipc::errors::DecodeError::from(err.to_string()),
                ),
            }
        },
    };

    quote! {
        impl #impl_generics ::acktor_ipc::Decode for #name #ty_generics #where_clause {
            #[inline]
            fn decode(
                buf: ::acktor_ipc::bytes::Bytes,
                _context: ::core::option::Option<&::acktor_ipc::DecodeContext>,
            ) -> ::core::result::Result<Self, ::acktor_ipc::errors::DecodeError> {
                #body
            }
        }
    }
}

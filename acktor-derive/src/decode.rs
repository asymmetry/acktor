use proc_macro2::TokenStream;
use quote::quote;

use crate::common::{CodecMethod, detect_codec_config};

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let config = match detect_codec_config(ast) {
        Ok(config) => config,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let body = match (config.method, config.bridge) {
        (CodecMethod::Prost, None) => quote! {
            <Self as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)
        },
        (CodecMethod::Prost, Some(bridge)) => quote! {
            let bridge = <#bridge as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)
        },
        (CodecMethod::SerdeJson, None) => quote! {
            match ::serde_json::from_slice::<Self>(&buf) {
                ::core::result::Result::Ok(value) => ::core::result::Result::Ok(value),
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor::error::DecodeError::other(err),
                ),
            }
        },
        (CodecMethod::SerdeJson, Some(bridge)) => quote! {
            match ::serde_json::from_slice::<#bridge>(&buf) {
                ::core::result::Result::Ok(bridge) => {
                    <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                        .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)
                }
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor::error::DecodeError::other(err),
                ),
            }
        },
        (CodecMethod::Zerocopy, None) => quote! {
            <Self as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(|e| {
                    ::core::convert::Into::<::acktor::error::DecodeError>::into(e.to_string())
                })
        },
        (CodecMethod::Zerocopy, Some(bridge)) => quote! {
            let bridge = <#bridge as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(|e| e.to_string())?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)
        },
        (CodecMethod::Rkyv, None) => quote! {
            match ::rkyv::from_bytes::<Self, ::rkyv::rancor::Error>(&buf[..]) {
                ::core::result::Result::Ok(value) => ::core::result::Result::Ok(value),
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor::error::DecodeError::other(err),
                ),
            }
        },
        (CodecMethod::Rkyv, Some(bridge)) => quote! {
            match ::rkyv::from_bytes::<#bridge, ::rkyv::rancor::Error>(&buf[..]) {
                ::core::result::Result::Ok(bridge) => {
                    <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                        .map_err(::core::convert::Into::<::acktor::error::DecodeError>::into)
                }
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor::error::DecodeError::other(err),
                ),
            }
        },
    };

    quote! {
        impl #impl_generics ::acktor::codec::Decode for #name #ty_generics #where_clause {
            #[inline]
            fn decode(
                buf: ::acktor::bytes::Bytes,
                _ctx: ::core::option::Option<&dyn ::acktor::codec::DecodeContext>,
            ) -> ::core::result::Result<Self, ::acktor::error::DecodeError> {
                #body
            }
        }
    }
}

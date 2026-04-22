use proc_macro2::TokenStream;
use quote::quote;

use crate::common::{CodecMethod, detect_codec_config, detect_index};

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let config = match detect_codec_config(ast) {
        Ok(config) => config,
        Err(err) => return err.to_compile_error(),
    };

    let index = match detect_index(ast) {
        Ok(index) => index,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let body = match (config.method, config.bridge) {
        (CodecMethod::Prost, None) => quote! {
            <Self as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (CodecMethod::Prost, Some(bridge)) => quote! {
            let bridge = <#bridge as ::prost::Message>::decode(buf)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (CodecMethod::Zerocopy, None) => quote! {
            <Self as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (CodecMethod::Zerocopy, Some(bridge)) => quote! {
            let bridge = <#bridge as ::zerocopy::FromBytes>::read_from_bytes(&buf[..])
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)?;
            <Self as ::core::convert::TryFrom<#bridge>>::try_from(bridge)
                .map_err(::core::convert::Into::<::acktor_ipc::errors::DecodeError>::into)
        },
        (CodecMethod::Rkyv, None) => quote! {
            match ::rkyv::from_bytes::<Self, ::rkyv::rancor::Error>(&buf[..]) {
                ::core::result::Result::Ok(value) => ::core::result::Result::Ok(value),
                ::core::result::Result::Err(err) => ::core::result::Result::Err(
                    ::acktor_ipc::errors::DecodeError::from(err.to_string()),
                ),
            }
        },
        (CodecMethod::Rkyv, Some(bridge)) => quote! {
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
            const ID: u64 = #index;

            #[inline]
            fn decode(
                buf: ::acktor_ipc::bytes::Bytes,
                _ctx: ::core::option::Option<&::acktor_ipc::DecodeContext>,
            ) -> ::core::result::Result<Self, ::acktor_ipc::errors::DecodeError> {
                #body
            }
        }
    }
}

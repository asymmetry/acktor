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

    let (encoded_len_body, encode_body, encode_to_bytes_body) = match (config.method, config.bridge)
    {
        (CodecMethod::Prost, None) => (
            quote! { ::prost::Message::encoded_len(self) },
            quote! {
                ::prost::Message::encode(self, buf).map_err(
                    ::core::convert::Into::<::acktor::error::EncodeError>::into
                )
            },
            quote! {
                let mut buf = ::acktor::bytes::BytesMut::with_capacity(
                    <Self as ::acktor::codec::Encode>::encoded_len(self)
                );
                <Self as ::acktor::codec::Encode>::encode(&self, &mut buf, ctx)?;
                ::core::result::Result::Ok(buf.freeze())
            },
        ),
        (CodecMethod::Prost, Some(bridge)) => (
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                ::prost::Message::encoded_len(&bridge)
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                ::prost::Message::encode(&bridge, buf).map_err(
                    ::core::convert::Into::<::acktor::error::EncodeError>::into
                )
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                let mut buf = ::acktor::bytes::BytesMut::with_capacity(
                    ::prost::Message::encoded_len(&bridge)
                );
                ::prost::Message::encode(&bridge, &mut buf)?;
                ::core::result::Result::Ok(buf.freeze())
            },
        ),
        (CodecMethod::SerdeJson, None) => (
            quote! {
                match ::serde_json::to_vec(self) {
                    ::core::result::Result::Ok(vec) => vec.len(),
                    ::core::result::Result::Err(_) => 0,
                }
            },
            quote! {
                match ::serde_json::to_vec(self) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
            quote! {
                match ::serde_json::to_vec(self) {
                    ::core::result::Result::Ok(vec) => {
                        ::core::result::Result::Ok(
                            ::core::convert::Into::<::acktor::bytes::Bytes>::into(vec)
                        )
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
        ),
        (CodecMethod::SerdeJson, Some(bridge)) => (
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::serde_json::to_vec(&bridge) {
                    ::core::result::Result::Ok(vec) => vec.len(),
                    ::core::result::Result::Err(_) => 0,
                }
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::serde_json::to_vec(&bridge) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::serde_json::to_vec(&bridge) {
                    ::core::result::Result::Ok(vec) => {
                        ::core::result::Result::Ok(
                            ::core::convert::Into::<::acktor::bytes::Bytes>::into(vec)
                        )
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
        ),
        (CodecMethod::Zerocopy, None) => (
            quote! { ::core::mem::size_of::<Self>() },
            quote! {
                buf.extend_from_slice(
                    <Self as ::zerocopy::IntoBytes>::as_bytes(self),
                );
                ::core::result::Result::Ok(())
            },
            quote! {
                let mut buf = ::acktor::bytes::BytesMut::with_capacity(
                    <Self as ::acktor::codec::Encode>::encoded_len(self)
                );
                <Self as ::acktor::codec::Encode>::encode(&self, &mut buf, ctx)?;
                ::core::result::Result::Ok(buf.freeze())
            },
        ),
        (CodecMethod::Zerocopy, Some(bridge)) => (
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
            quote! {
                let mut buf = ::acktor::bytes::BytesMut::with_capacity(
                    <Self as ::acktor::codec::Encode>::encoded_len(self)
                );
                <Self as ::acktor::codec::Encode>::encode(&self, &mut buf, ctx)?;
                ::core::result::Result::Ok(buf.freeze())
            },
        ),
        (CodecMethod::Rkyv, None) => (
            quote! {
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(self) {
                    ::core::result::Result::Ok(vec) => vec.len(),
                    ::core::result::Result::Err(_) => 0,
                }
            },
            quote! {
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(self) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
            quote! {
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(self) {
                    ::core::result::Result::Ok(vec) => {
                        let mut buf = ::acktor::bytes::BytesMut::with_capacity(vec.len());
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(buf.freeze())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err)
                    ),
                }
            },
        ),
        (CodecMethod::Rkyv, Some(bridge)) => (
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(&bridge) {
                    ::core::result::Result::Ok(vec) => vec.len(),
                    ::core::result::Result::Err(_) => 0,
                }
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(&bridge) {
                    ::core::result::Result::Ok(vec) => {
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err),
                    ),
                }
            },
            quote! {
                let bridge = <#bridge as ::core::convert::From<&Self>>::from(self);
                match ::rkyv::to_bytes::<::rkyv::rancor::Error>(&bridge) {
                    ::core::result::Result::Ok(vec) => {
                        let mut buf = ::acktor::bytes::BytesMut::with_capacity(vec.len());
                        buf.extend_from_slice(vec.as_slice());
                        ::core::result::Result::Ok(buf.freeze())
                    }
                    ::core::result::Result::Err(err) => ::core::result::Result::Err(
                        ::acktor::error::EncodeError::other(err),
                    ),
                }
            },
        ),
    };

    quote! {
        impl #impl_generics ::acktor::codec::Encode for #name #ty_generics #where_clause {
            #[inline]
            fn encoded_len(&self) -> usize {
                #encoded_len_body
            }

            #[inline]
            fn encode(
                &self,
                buf: &mut ::acktor::bytes::BytesMut,
                ctx: ::core::option::Option<&dyn ::acktor::codec::EncodeContext>,
            ) -> ::core::result::Result<(), ::acktor::error::EncodeError> {
                #encode_body
            }

            #[inline]
            fn encode_to_bytes(
                &self,
                ctx: ::core::option::Option<&dyn ::acktor::codec::EncodeContext>
            ) -> ::core::result::Result<::acktor::bytes::Bytes, ::acktor::error::EncodeError> {
                #encode_to_bytes_body
            }
        }
    }
}

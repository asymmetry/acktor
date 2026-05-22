use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};

pub enum CodecMethod {
    Prost,
    Zerocopy,
    Rkyv,
    SerdeJson,
}

pub struct CodecConfig {
    pub method: CodecMethod,
    /// Optional bridge type. When present, encode/decode goes through this
    /// intermediary type instead of operating on `Self` directly.
    pub bridge: Option<syn::Type>,
}

struct CodecArgs {
    ident: syn::Ident,
    bridge: Option<syn::Type>,
}

impl Parse for CodecArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: syn::Ident = input.parse()?;
        let bridge = if input.peek(syn::Token![,]) {
            input.parse::<syn::Token![,]>()?;
            Some(input.parse::<syn::Type>()?)
        } else {
            None
        };

        Ok(CodecArgs { ident, bridge })
    }
}

pub fn detect_codec_config(ast: &syn::DeriveInput) -> syn::Result<CodecConfig> {
    let attr = ast
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("codec"))
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "missing required attribute `#[codec(method)]`, where `method` is one of \
                 `prost`, `serde_json`, `zerocopy`, `rkyv`, optionally followed by a bridge \
                 type: `#[codec(method, Bridge)]`",
            )
        })?;

    match &attr.meta {
        syn::Meta::List(list) => {
            let args = list.parse_args::<CodecArgs>()?;
            let backend = if args.ident == "prost" {
                CodecMethod::Prost
            } else if args.ident == "serde_json" {
                CodecMethod::SerdeJson
            } else if args.ident == "zerocopy" {
                CodecMethod::Zerocopy
            } else if args.ident == "rkyv" {
                CodecMethod::Rkyv
            } else {
                return Err(syn::Error::new_spanned(
                    &args.ident,
                    "unknown codec method, expected one of: `prost`, `serde_json`, `zerocopy`, \
                     `rkyv`",
                ));
            };
            if let Some(bridge) = &args.bridge
                && is_self_type(bridge, &ast.ident)
            {
                Err(syn::Error::new_spanned(
                    bridge,
                    "bridge type must differ from the derived type",
                ))
            } else {
                Ok(CodecConfig {
                    method: backend,
                    bridge: args.bridge,
                })
            }
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "the correct syntax is `#[codec(method)]` or `#[codec(method, Bridge)]`",
        )),
    }
}

/// Returns `true` if `ty` is a simple path that matches the identifier `name`.
fn is_self_type(ty: &syn::Type, name: &syn::Ident) -> bool {
    if let syn::Type::Path(syn::TypePath { qself: None, path }) = ty {
        path.is_ident(name)
    } else {
        false
    }
}

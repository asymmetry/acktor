use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};

pub enum Backend {
    Prost,
    Zerocopy,
    Rkyv,
}

pub struct CodecConfig {
    pub backend: Backend,
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

pub fn detect_backend(ast: &syn::DeriveInput) -> syn::Result<CodecConfig> {
    let attr = ast
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("codec"))
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "Expect an attribute `#[codec(..)]` with one of: `prost`, `zerocopy`, `rkyv`",
            )
        })?;

    match &attr.meta {
        syn::Meta::List(list) => {
            let args = list.parse_args::<CodecArgs>()?;
            let backend = if args.ident == "prost" {
                Backend::Prost
            } else if args.ident == "zerocopy" {
                Backend::Zerocopy
            } else if args.ident == "rkyv" {
                Backend::Rkyv
            } else {
                return Err(syn::Error::new_spanned(
                    &args.ident,
                    "Unknown codec backend, expected one of: `prost`, `zerocopy`, `rkyv`",
                ));
            };
            if let Some(bridge) = &args.bridge {
                if is_self_type(bridge, &ast.ident) {
                    return Err(syn::Error::new_spanned(
                        bridge,
                        "Bridge type must differ from the derived type",
                    ));
                }
            }
            Ok(CodecConfig {
                backend,
                bridge: args.bridge,
            })
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "The correct syntax is #[codec(..)]",
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

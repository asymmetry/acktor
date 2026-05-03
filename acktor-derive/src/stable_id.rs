use proc_macro2::TokenStream;
use quote::quote;
use syn::{GenericParam, Type};

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    // one `.combine(...)` per generic param (lifetimes skipped), folded into the base id
    let mut combines = Vec::<TokenStream>::new();
    // extra `T: StableId` bounds for the impl's where-clause
    let mut extra_bounds = Vec::<TokenStream>::new();

    for param in &ast.generics.params {
        match param {
            GenericParam::Lifetime(_) => {}
            GenericParam::Type(t) => {
                let ident = &t.ident;
                combines.push(quote! {
                    .combine(
                        <#ident as ::acktor::StableId>::TYPE_ID.as_bytes()
                    )
                });
                extra_bounds.push(quote! {
                    #ident: ::acktor::StableId
                });
            }
            GenericParam::Const(c) => {
                let ident = &c.ident;
                match combine_const_param(ident, &c.ty) {
                    Ok(tokens) => combines.push(tokens),
                    Err(err) => return err.to_compile_error(),
                }
            }
        }
    }

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let where_clause_tokens = match (where_clause, extra_bounds.is_empty()) {
        (Some(wc), true) => quote! { #wc },
        (Some(wc), false) => {
            let sep = if wc.predicates.empty_or_trailing() {
                quote! {}
            } else {
                quote! { , }
            };
            quote! { #wc #sep #(#extra_bounds),* }
        }
        (None, true) => quote! {},
        (None, false) => quote! { where #(#extra_bounds),* },
    };

    quote! {
        impl #impl_generics ::acktor::StableId for #name #ty_generics #where_clause_tokens
        {
            const TYPE_ID: ::acktor::StableTypeId =
                ::acktor::StableTypeId::from_stable_type_name(
                    concat!(module_path!(), "::", stringify!(#name))
                )
                #(#combines)*;
        }
    }
}

/// Build a `.combine(&hash)` token stream for a const-generic param.
///
/// We need a `&[u8; 16]` to feed `combine`, so we hash the value's big-endian byte form with
/// `sha2_const` inline and take the first 16 bytes of the digest.
fn combine_const_param(ident: &syn::Ident, ty: &Type) -> syn::Result<TokenStream> {
    const SUPPORTED: &str = "StableId derive only supports legal const generic parameter types";
    let Type::Path(tp) = ty else {
        return Err(syn::Error::new_spanned(ty, SUPPORTED));
    };
    let seg = tp
        .path
        .segments
        .last()
        .expect("type syntax should guarantee at least one segment");
    let name = seg.ident.to_string();
    let value_bytes = match name.as_str() {
        "u8" | "u16" | "u32" | "u64" | "u128" | "i8" | "i16" | "i32" | "i64" | "i128" => quote! {
            &#ident.to_be_bytes()
        },
        "usize" => quote! { &(#ident as u64).to_be_bytes() },
        "isize" => quote! { &(#ident as i64).to_be_bytes() },
        "bool" => quote! { &[#ident as u8] },
        "char" => quote! { &(#ident as u32).to_be_bytes() },
        _ => {
            return Err(syn::Error::new_spanned(ty, SUPPORTED));
        }
    };

    Ok(quote! {
        .combine(&::acktor::stable_type_id::take16(
            ::acktor::sha2_const::Sha256::new()
                .update(#value_bytes)
                .finalize(),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(src: &str) -> syn::DeriveInput {
        syn::parse_str(src).unwrap()
    }

    #[test]
    fn test_no_generics() {
        let out = expand(&input("struct Ping;")).to_string();
        assert!(out.contains("impl :: acktor :: StableId for Ping"));
        assert!(out.contains(":: from_stable_type_name"));
        assert!(out.contains("concat ! (module_path ! ()"));
        assert!(out.contains("stringify ! (Ping)"));
        assert!(!out.contains(". combine ("));
    }

    #[test]
    fn test_type_generics() {
        let out = expand(&input("struct Wrap<T>(T);")).to_string();
        assert!(out.contains("T : :: acktor :: StableId"));
        assert!(out.contains(":: TYPE_ID . as_bytes ()"));
        assert_eq!(out.matches(". combine (").count(), 1);

        let out = expand(&input("struct Pair<A, B>(A, B);")).to_string();
        assert_eq!(out.matches(". combine (").count(), 2);
        assert!(out.contains("A : :: acktor :: StableId"));
        assert!(out.contains("B : :: acktor :: StableId"));

        let out = expand(&input("struct Wrap<T, U>(T, U) where T: Clone, U: Clone,;")).to_string();
        assert!(!out.contains(", ,"));
        assert!(out.contains("T : :: acktor :: StableId"));
        assert!(out.contains("U : :: acktor :: StableId"));

        let out = expand(&input("struct Wrap<T, U>(T, U) where T: Clone, U: Clone;")).to_string();
        assert!(!out.contains(", ,"));
        assert!(out.contains("T : :: acktor :: StableId"));
        assert!(out.contains("U : :: acktor :: StableId"));
    }

    #[test]
    fn test_const_generics() {
        let out = expand(&input("struct USize<const N: usize>;")).to_string();
        assert!(out.contains("(N as u64) . to_be_bytes ()"));
        assert!(out.contains(":: sha2_const :: Sha256 :: new"));
        assert_eq!(out.matches(". combine (").count(), 1);

        let out = expand(&input("struct ISize<const I: isize>;")).to_string();
        assert!(out.contains("(I as i64) . to_be_bytes ()"));

        let out = expand(&input("struct I32<const I: i32>;")).to_string();
        assert!(out.contains("I . to_be_bytes ()"));

        let out = expand(&input("struct Bool<const F: bool>;")).to_string();
        assert!(out.contains("[F as u8]"));

        let out = expand(&input("struct Char<const C: char>;")).to_string();
        assert!(out.contains("(C as u32) . to_be_bytes ()"));

        let out = expand(&input("struct Bad<const S: SomeUnknown>;")).to_string();
        assert!(out.contains("StableId derive only supports legal const generic parameter types"));
    }

    #[test]
    fn test_lifetime_only_generics() {
        let out = expand(&input("struct Borrow<'a>(&'a u8);")).to_string();
        assert!(out.contains("impl < 'a > :: acktor :: StableId for Borrow < 'a >"));
        assert!(!out.contains(". combine ("));
    }

    #[test]
    fn test_mixed_generics() {
        let out = expand(&input(
            "struct Mixed<T, const N: usize>(std::marker::PhantomData<T>);",
        ))
        .to_string();
        assert_eq!(out.matches(". combine (").count(), 2);
        assert!(out.contains("T : :: acktor :: StableId"));
        assert!(out.contains("(N as u64) . to_be_bytes ()"));
    }
}

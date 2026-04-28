use proc_macro2::TokenStream;
use quote::quote;
use syn::{GenericParam, LitInt};

use crate::has_stable_type_id;

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let custom_id = match get_custom_id(ast) {
        Ok(id) => id,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    if let Some(value) = custom_id {
        // user-supplied id: skip HasStableTypeId entirely
        return quote! {
            impl #impl_generics ::acktor::message::MessageId
                for #name #ty_generics #where_clause
            {
                const ID: u64 = #value;
            }
        };
    }

    // delegate to STABLE_TYPE_ID; mirror the bounds emitted by the HasStableTypeId derive so the
    // MessageId impl can name `Self::STABLE_TYPE_ID` for the same generic parameters
    let mut extra_bounds = Vec::<TokenStream>::new();
    for param in &ast.generics.params {
        if let GenericParam::Type(t) = param {
            let ident = &t.ident;
            extra_bounds.push(quote! {
                #ident: ::acktor::stable_type_id::HasStableTypeId
            });
        }
    }

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

    let has_stable_type_id_impl = has_stable_type_id::expand(ast);

    quote! {
        impl #impl_generics ::acktor::message::MessageId
            for #name #ty_generics #where_clause_tokens
        {
            const ID: u64 =
                <Self as ::acktor::stable_type_id::HasStableTypeId>::STABLE_TYPE_ID.as_u64();
        }

        #has_stable_type_id_impl
    }
}

fn get_custom_id(ast: &syn::DeriveInput) -> syn::Result<Option<LitInt>> {
    let Some(attr) = ast.attrs.iter().find(|a| a.path().is_ident("custom_id")) else {
        return Ok(None);
    };
    let syn::Meta::List(list) = &attr.meta else {
        return Err(syn::Error::new_spanned(
            attr,
            "the correct syntax is `#[custom_id(<u64 value>)]`",
        ));
    };
    let lit = list.parse_args::<LitInt>()?;
    // ensure it fits in u64
    lit.base10_parse::<u64>()?;
    Ok(Some(lit))
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
        assert!(out.contains("impl :: acktor :: stable_type_id :: HasStableTypeId for Ping"));
        assert!(out.contains("impl :: acktor :: message :: MessageId for Ping"));
        assert!(out.contains("STABLE_TYPE_ID . as_u64 ()"));
    }

    #[test]
    fn test_type_generics() {
        let out = expand(&input("struct Wrap<T>(T);")).to_string();
        assert!(out.contains("impl < T > :: acktor :: stable_type_id :: HasStableTypeId"));
        assert!(out.contains("impl < T > :: acktor :: message :: MessageId for Wrap < T >"));
        // T: HasStableTypeId appears in both impls' where-clauses
        assert_eq!(
            out.matches("T : :: acktor :: stable_type_id :: HasStableTypeId")
                .count(),
            2
        );
    }

    #[test]
    fn test_const_generics() {
        let out = expand(&input("struct Buf<const N: usize>;")).to_string();
        assert!(
            out.contains(
                "impl < const N : usize > :: acktor :: message :: MessageId for Buf < N >"
            )
        );
        // no `where` for either impl when only const generics are present
        assert!(!out.contains("where"));
    }

    #[test]
    fn test_lifetime_only_generics() {
        let out = expand(&input("struct Borrow<'a>(&'a u8);")).to_string();
        assert!(out.contains(
            "impl < 'a > :: acktor :: stable_type_id :: HasStableTypeId for Borrow < 'a >"
        ));
        assert!(out.contains("impl < 'a > :: acktor :: message :: MessageId for Borrow < 'a >"));
        assert!(!out.contains("where"));
    }

    #[test]
    fn test_mixed_generics() {
        let out = expand(&input(
            "struct Mixed<'a, T, const N: usize>(&'a std::marker::PhantomData<[T; N]>);",
        ))
        .to_string();
        assert_eq!(
            out.matches("T : :: acktor :: stable_type_id :: HasStableTypeId")
                .count(),
            2
        );
        assert!(out.contains("(N as u64) . to_le_bytes ()"));
    }

    #[test]
    fn test_custom_id() {
        let out = expand(&input("#[custom_id(42)] struct Ping;")).to_string();
        assert!(out.contains("impl :: acktor :: message :: MessageId for Ping"));
        assert!(out.contains("const ID : u64 = 42"));
        // no HasStableTypeId impl is emitted when a custom id is supplied
        assert!(!out.contains("HasStableTypeId for Ping"));
        assert!(!out.contains("STABLE_TYPE_ID"));

        let out = expand(&input("#[custom_id(0xdead_beef)] struct Ping;")).to_string();
        assert!(out.contains("const ID : u64 = 0xdead_beef"));

        let out = expand(&input("#[custom_id(7)] struct Wrap<T>(T);")).to_string();
        assert!(out.contains("impl < T > :: acktor :: message :: MessageId for Wrap < T >"));
        // no auto-added HasStableTypeId bound on T
        assert!(!out.contains("T : :: acktor :: stable_type_id :: HasStableTypeId"));
        assert!(!out.contains("HasStableTypeId for Wrap"));

        // u64::MAX + 1 — must not parse as u64
        let out = expand(&input("#[custom_id(18446744073709551616)] struct Ping;")).to_string();
        assert!(out.contains("compile_error"));

        let out = expand(&input("#[custom_id(\"oops\")] struct Ping;")).to_string();
        assert!(out.contains("compile_error"));
    }
}

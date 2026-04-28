use proc_macro2::TokenStream;
use quote::quote;
use syn::GenericParam;

use crate::has_stable_type_id;

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    // mirror the bounds emitted by the `HasStableTypeId` derive so a generic type ends up with
    // consistent where-clauses on both impls
    let mut extra_bounds = Vec::<TokenStream>::new();
    for param in &ast.generics.params {
        if let GenericParam::Type(t) = param {
            let ident = &t.ident;
            extra_bounds.push(quote! {
                #ident: ::acktor::stable_type_id::HasStableTypeId
            });
        }
    }

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let where_clause_tokens = match (where_clause, extra_bounds.is_empty()) {
        (Some(wc), true) => quote! { #wc },
        (Some(wc), false) => quote! { #wc #(#extra_bounds,)* },
        (None, true) => quote! {},
        (None, false) => quote! { where #(#extra_bounds,)* },
    };

    // also emit the `HasStableTypeId` impl so `#[derive(MessageId)]` alone is enough
    let has_stable_type_id_impl = has_stable_type_id::expand(ast);

    quote! {
        impl #impl_generics ::acktor::message::MessageId
            for #name #ty_generics #where_clause_tokens {}

        #has_stable_type_id_impl
    }
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
        // the MessageId impl body is empty (marker trait)
        assert!(out.contains("MessageId for Ping { }"));
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
        assert!(out.contains(
            "impl < const N : usize > :: acktor :: message :: MessageId for Buf < N > { }"
        ));
        // no `where` for either impl when only const generics are present
        assert!(!out.contains("where"));
    }

    #[test]
    fn test_lifetime_only_generics() {
        let out = expand(&input("struct Borrow<'a>(&'a u8);")).to_string();
        assert!(out.contains(
            "impl < 'a > :: acktor :: stable_type_id :: HasStableTypeId for Borrow < 'a >"
        ));
        assert!(
            out.contains("impl < 'a > :: acktor :: message :: MessageId for Borrow < 'a > { }")
        );
        assert!(!out.contains("where"));
    }

    #[test]
    fn test_mixed_generics() {
        let out = expand(&input(
            "struct Mixed<'a, T, const N: usize>(&'a std::marker::PhantomData<[T; N]>);",
        ))
        .to_string();
        // each impl bounds T exactly once
        assert_eq!(
            out.matches("T : :: acktor :: stable_type_id :: HasStableTypeId")
                .count(),
            2
        );
        // const N still folded into the HasStableTypeId hash
        assert!(out.contains("(N as u64) . to_le_bytes ()"));
    }
}

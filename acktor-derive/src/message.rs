use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};

pub fn expand(ast: &syn::DeriveInput) -> TokenStream {
    let result_type = match get_result_type(ast) {
        Ok(ty) => ty,
        Err(err) => return err.to_compile_error(),
    };

    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let result_type = result_type.into_token_stream();

    quote! {
        impl #impl_generics ::acktor::Message for #name #ty_generics #where_clause {
            type Result = #result_type;
        }
    }
}

pub fn get_result_type(ast: &syn::DeriveInput) -> syn::Result<syn::Type> {
    let attr = ast
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("result_type"))
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "missing required attribute `#[result_type(T)]`",
            )
        })?;

    match &attr.meta {
        syn::Meta::List(list) => list.parse_args::<syn::Type>(),
        _ => Err(syn::Error::new_spanned(
            attr,
            "the correct syntax is `#[result_type(T)]`",
        )),
    }
}

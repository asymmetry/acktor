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
        impl #impl_generics ::acktor::message::Message for #name #ty_generics #where_clause {
            type Result = #result_type;
        }
    }
}

pub fn get_result_type(ast: &syn::DeriveInput) -> syn::Result<syn::Type> {
    let attr = ast
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("result_type"))
        .ok_or_else(|| syn::Error::new(Span::call_site(), "Expect an attribute `result_type`"))?;

    match attr.meta {
        syn::Meta::NameValue(ref nv) => {
            if let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(lit_str),
                ..
            }) = nv.value.clone()
            {
                if let Ok(ty) = syn::parse_str::<syn::Type>(&lit_str.value()) {
                    return Ok(ty);
                }
            }
            Err(syn::Error::new_spanned(&nv.value, "Expect type"))
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "The correct syntax is #[result_type(type)]",
        )),
    }
}

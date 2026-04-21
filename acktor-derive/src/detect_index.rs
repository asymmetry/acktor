use proc_macro2::Span;

pub fn detect_index(ast: &syn::DeriveInput) -> syn::Result<u64> {
    let attr = ast
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("index"))
        .ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "Expect an attribute `#[index(N)]` with a `u64` literal",
            )
        })?;

    match &attr.meta {
        syn::Meta::List(list) => {
            let lit: syn::LitInt = list.parse_args()?;
            lit.base10_parse::<u64>()
        }
        _ => Err(syn::Error::new_spanned(
            attr,
            "The correct syntax is #[index(N)]",
        )),
    }
}

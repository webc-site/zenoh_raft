mod expand;
mod variant_name;

use proc_macro::TokenStream;
use quote::quote;
use syn::Item;
use syn::ReturnType;
use syn::TraitItem;
use syn::Type;
use syn::parse_macro_input;
use syn::parse_str;
use syn::parse2;
use syn::token::RArrow;

/// This proc macro attribute adds `Send` bounds to a trait.
#[proc_macro_attribute]
pub fn add_async_trait(_attr: TokenStream, item: TokenStream) -> TokenStream {
    add_send_bounds(item)
}

fn add_send_bounds(item: TokenStream) -> TokenStream {
    let default_return_type: Box<Type> =
        parse_str("impl std::future::Future<Output = ()> + Send").unwrap();

    match parse_macro_input!(item) {
        Item::Trait(mut input) => {
            for item in input.items.iter_mut() {
                // for each async function definition
                let TraitItem::Fn(function) = item else {
                    continue;
                };
                if function.sig.asyncness.is_none() {
                    continue;
                };

                // remove async from signature
                function.sig.asyncness = None;

                // wrap the return type in a `Future`
                function.sig.output = match &function.sig.output {
                    ReturnType::Default => {
                        ReturnType::Type(RArrow::default(), default_return_type.clone())
                    }
                    ReturnType::Type(arrow, t) => {
                        let tokens = quote!(impl std::future::Future<Output = #t> + Send);
                        ReturnType::Type(*arrow, parse2(tokens).unwrap())
                    }
                };

                // if a body is defined, wrap it in an async block
                let Some(body) = &function.default else {
                    continue;
                };
                let body = parse2(quote!({ async move #body })).unwrap();
                function.default = Some(body);
            }

            quote!(#input).into()
        }

        _ => panic!("add_async_trait can only be used with traits"),
    }
}

/// Render a template with arguments multiple times.
///
/// The template to expand is defined as `(K,V) => { ... }`, where `K` and `V` are template
/// variables.
///
/// - The template must contain at least 1 variable.
/// - If the first macro argument is `KEYED`, the first variable serve as the key for deduplication.
///   Otherwise, the first macro argument should be `!KEYED`, and no deduplication will be
///   performed.
///
/// # Example: `KEYED` for deduplication
///
/// The following code builds a series of let statements:
/// ```
/// # use openraft_macros::expand;
/// # fn foo () {
/// expand!(
///     KEYED,
///     // Template with variables K and V, and template body, excluding the braces.
///     (K, T, V) => {let K: T = V;},
///     // Arguments for rendering the template
///     (a, u64, 1),
///     (b, String, "foo".to_string()),
///     (a, u32, 2), // duplicate a will be ignored
///     (c, Vec<u8>, vec![1,2])
/// );
/// # }
/// ```
///
/// The above code will be transformed into:
///
/// ```
/// # fn foo () {
/// let a: u64 = 1;
/// let b: String = "foo".to_string();
/// let c: Vec<u8> = vec![1, 2];
/// # }
/// ```
///
/// # Example: `!KEYED` for no deduplication
///
/// ```
/// # use openraft_macros::expand;
/// # fn foo () {
/// expand!(!KEYED, (K, T, V) => {let K: T = V;},
///                 (c, u8, 8),
///                 (c, u16, 16),
/// );
/// # }
/// ```
///
/// The above code will be transformed into:
///
/// ```
/// # fn foo () {
/// let c: u8 = 8;
/// let c: u16 = 16;
/// # }
/// ```
#[proc_macro]
pub fn expand(item: TokenStream) -> TokenStream {
    let repeat = parse_macro_input!(item as expand::Expand);
    repeat.render().into()
}

/// Derive `COUNT`, `ALL`, `index()`, `as_str()` and `Display` for an enum that names the
/// variants of another type.
///
/// Such a "name" enum is used to count events into a fixed-size array, so each variant needs a
/// stable index and the set of indices must be dense. This derive keeps the index, the variant
/// list and the count in agreement by generating all three from the enum definition.
///
/// The enum must derive `Copy`, and every variant must either be a unit variant or wrap exactly
/// one other name enum. A wrapping variant is expanded in place: it occupies as many indices as
/// the inner enum has variants, and delegates `as_str()` to it.
///
/// `#[variant_name(prefix = "...")]` prepends a string to the rendering of every unit variant.
///
/// # Example
///
/// ```
/// use openraft_macros::VariantName;
///
/// #[derive(Clone, Copy, VariantName)]
/// #[variant_name(prefix = "SM::")]
/// enum SmName {
///     Build,
///     Apply,
/// }
///
/// #[derive(Clone, Copy, VariantName)]
/// enum Name {
///     Vote,
///     StateMachine(SmName),
///     Respond,
/// }
///
/// assert_eq!(Name::COUNT, 4);
/// assert_eq!(Name::StateMachine(SmName::Apply).index(), 2);
/// assert_eq!(Name::Respond.index(), 3);
/// assert_eq!(Name::StateMachine(SmName::Apply).as_str(), "SM::Apply");
/// assert_eq!(Name::Vote.as_str(), "Vote");
/// ```
#[proc_macro_derive(VariantName, attributes(variant_name))]
pub fn variant_name(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::DeriveInput);
    match variant_name::expand(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

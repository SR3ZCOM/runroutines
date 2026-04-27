use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ExprPath, Path, Expr,  visit_mut::{self, VisitMut}};

#[proc_macro_attribute]
pub fn rr_compliant(_attr: TokenStream, item: TokenStream) -> TokenStream {
  let mut input_fn = parse_macro_input!(item as ItemFn);

  struct CallRewriter;

  impl VisitMut for CallRewriter {
    fn visit_expr_call_mut(&mut self, i: &mut syn::ExprCall) {
      if let Expr::Path(ref p) = *i.func && let Some(last_segment) = p.path.segments.last() && last_segment.ident == "sleep" {
        println!("FOUND: {} IN:", last_segment.ident);

        let new_path: Path = syn::parse_str("sleep_yield").unwrap();
        // let new_path: Path = syn::parse_str("arbit_yield").unwrap();

        *i.func = Expr::Path(ExprPath { attrs: vec![], qself: None, path: new_path })
      }
      visit_mut::visit_expr_call_mut(self, i);
    }
  }

  let mut rewriter = CallRewriter;
  rewriter.visit_item_fn_mut(&mut input_fn);

  TokenStream::from(quote!(#input_fn))
}

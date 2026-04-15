fn main() {
  let version = rustc_version::version().expect("RUST_C_VERSION_UNAVAILABLE_IN BUILD_SCRIPT");
  println!("cargo:rustc-env=RUSTC_VERSION={}", version);
  cc::Build::new().file("asm/swap_stack.S").compile("swap_stack");
}

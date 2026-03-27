//! Re-exports the filter parser from the core `flapjack` crate.
//!
//! The parser was moved to core so that `Rule::matches()` can parse
//! condition filter strings without a cross-crate dependency.

pub use flapjack::filter_parser::*;

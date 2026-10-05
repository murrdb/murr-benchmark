//! Encoders from the backend-independent workload model to backend wire formats.
//!
//! Each encoder is built once from the workload `Schema` at backend init and
//! is cheap to clone, so backends can hold them by value.

pub mod arrow;
pub mod blob;
pub mod json;
pub mod key;
pub mod murr;

use crate::workload::{Key, Row, RowBatch, Value};

/// Appends the bytes of a single value to `buf`.
pub trait ValueEncoder {
    fn encode(&self, value: &Value, buf: &mut Vec<u8>);
}

/// Appends the bytes of a row's values to `buf`.
pub trait RowEncoder {
    fn encode(&self, row: &Row, buf: &mut Vec<u8>);
}

/// Converts a whole write batch at once.
pub trait BatchEncoder {
    type Output;

    fn encode(&self, batch: &RowBatch) -> Self::Output;
}

/// Converts one key into the form a backend addresses rows by.
pub trait KeyEncoder {
    type Output<'a>;

    fn encode<'a>(&self, key: &'a Key) -> Self::Output<'a>;
}

/// Converts all keys of a request at once.
pub trait KeySetEncoder {
    type Output;

    fn encode(&self, keys: &[Key]) -> Self::Output;
}

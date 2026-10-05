use crate::workload::{DType, Key, Schema, Value};

use super::KeyEncoder;

/// Encodes a key as a single string, for backends that address rows by one string key.
///
/// Only a single utf8 key field is supported for now.
#[derive(Debug, Clone, Copy)]
pub struct StringKey;

impl StringKey {
    pub fn new(schema: &Schema) -> Self {
        match schema.keys.as_slice() {
            [field] if field.dtype == DType::Utf8 => StringKey,
            other => panic!("expected a single utf8 key field, got {other:?}"),
        }
    }
}

impl KeyEncoder for StringKey {
    type Output<'a> = &'a str;

    fn encode<'a>(&self, key: &'a Key) -> &'a str {
        match key.0.as_slice() {
            [Value::Utf8(s)] => s,
            other => panic!("expected a single utf8 key, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::Field;

    fn key_field(name: &str) -> Field {
        Field {
            name: name.to_string(),
            dtype: DType::Utf8,
            nullable: false,
        }
    }

    #[test]
    fn single_utf8_key_is_borrowed() {
        let schema = Schema {
            keys: vec![key_field("key")],
            values: vec![],
        };
        let key = Key(vec![Value::Utf8("k1".to_string())]);

        assert_eq!(StringKey::new(&schema).encode(&key), "k1");
    }

    #[test]
    #[should_panic(expected = "single utf8 key field")]
    fn compound_key_schema_is_rejected() {
        let schema = Schema {
            keys: vec![key_field("a"), key_field("b")],
            values: vec![],
        };

        StringKey::new(&schema);
    }
}

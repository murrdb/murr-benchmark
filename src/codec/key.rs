use std::borrow::Cow;

use crate::workload::{DType, Key, Schema, Value};

use super::KeyEncoder;

/// Separates the fields of a compound key; a control character that does not
/// occur in key values.
const SEPARATOR: char = '\u{1f}';

/// Encodes a key as a single string, for backends that address rows by one string key.
///
/// A single-field key is borrowed as is; the fields of a compound key are
/// joined with `SEPARATOR`. Only utf8 key fields are supported.
#[derive(Debug, Clone, Copy)]
pub struct StringKey;

impl StringKey {
    pub fn new(schema: &Schema) -> Self {
        assert!(!schema.keys.is_empty(), "schema has no key fields");
        for field in &schema.keys {
            assert!(
                field.dtype == DType::Utf8,
                "expected utf8 key fields, got {field:?}"
            );
        }
        StringKey
    }
}

fn utf8(value: &Value) -> &str {
    match value {
        Value::Utf8(s) => s,
        other => panic!("expected a utf8 key value, got {other:?}"),
    }
}

impl KeyEncoder for StringKey {
    type Output<'a> = Cow<'a, str>;

    fn encode<'a>(&self, key: &'a Key) -> Cow<'a, str> {
        match key.0.as_slice() {
            [single] => Cow::Borrowed(utf8(single)),
            fields => {
                let len = fields.iter().map(|f| utf8(f).len() + 1).sum();
                let mut joined = String::with_capacity(len);
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        joined.push(SEPARATOR);
                    }
                    joined.push_str(utf8(field));
                }
                Cow::Owned(joined)
            }
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

        let encoded = StringKey::new(&schema).encode(&key);

        assert!(matches!(encoded, Cow::Borrowed("k1")));
    }

    #[test]
    fn compound_key_fields_are_joined() {
        let schema = Schema {
            keys: vec![key_field("a"), key_field("b")],
            values: vec![],
        };
        let key = Key(vec![Value::Utf8("x".to_string()), Value::Utf8("y".to_string())]);

        assert_eq!(StringKey::new(&schema).encode(&key), "x\u{1f}y");
    }

    #[test]
    #[should_panic(expected = "expected utf8 key fields")]
    fn non_utf8_key_field_is_rejected() {
        let schema = Schema {
            keys: vec![Field {
                name: "id".to_string(),
                dtype: DType::Int64,
                nullable: false,
            }],
            values: vec![],
        };

        StringKey::new(&schema);
    }
}

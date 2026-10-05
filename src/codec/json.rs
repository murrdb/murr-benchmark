use std::sync::Arc;

use crate::workload::{Key, Schema, Value};

use super::KeySetEncoder;

impl From<&Value> for serde_json::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::Utf8(s) => serde_json::Value::from(s.as_str()),
            Value::Float32(v) => serde_json::Value::from(*v),
            Value::Float64(v) => serde_json::Value::from(*v),
            Value::Int64(v) => serde_json::Value::from(*v),
        }
    }
}

/// Encodes request keys as a JSON object of parallel arrays, one per key field.
#[derive(Clone)]
pub struct JsonKeys {
    schema: Arc<Schema>,
}

impl JsonKeys {
    pub fn new(schema: Arc<Schema>) -> Self {
        JsonKeys { schema }
    }
}

impl KeySetEncoder for JsonKeys {
    type Output = serde_json::Map<String, serde_json::Value>;

    fn encode(&self, keys: &[Key]) -> Self::Output {
        self.schema
            .keys
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column: Vec<serde_json::Value> =
                    keys.iter().map(|key| (&key.0[i]).into()).collect();
                (field.name.clone(), serde_json::Value::Array(column))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::{DType, Field};
    use serde_json::json;

    #[test]
    fn keys_are_parallel_arrays_named_by_key_field() {
        let field = |name: &str| Field {
            name: name.to_string(),
            dtype: DType::Utf8,
            nullable: false,
        };
        let schema = Arc::new(Schema {
            keys: vec![field("geid"), field("vendor")],
            values: vec![],
        });
        let key = |a: &str, b: &str| Key(vec![Value::Utf8(a.to_string()), Value::Utf8(b.to_string())]);

        let encoded = JsonKeys::new(schema).encode(&[key("TB_AE", "a"), key("TB_AE", "b")]);

        assert_eq!(
            serde_json::Value::Object(encoded),
            json!({"geid": ["TB_AE", "TB_AE"], "vendor": ["a", "b"]})
        );
    }

    #[test]
    fn numbers_keep_their_json_kind() {
        assert_eq!(serde_json::Value::from(&Value::Float64(1.5)), json!(1.5));
        assert_eq!(serde_json::Value::from(&Value::Int64(-7)), json!(-7));
    }
}

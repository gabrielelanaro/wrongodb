use std::collections::{HashMap, HashSet};

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKey {
    Null,
    Bool(bool),
    Number(String),
    String(String),
}

impl ScalarKey {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(ScalarKey::Null),
            Value::Bool(b) => Some(ScalarKey::Bool(*b)),
            Value::Number(n) => Some(ScalarKey::Number(n.to_string())),
            Value::String(s) => Some(ScalarKey::String(s.clone())),
            Value::Array(_) | Value::Object(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryIndex {
    pub fields: HashSet<String>,
    index: HashMap<String, HashMap<ScalarKey, HashSet<u64>>>,
}

impl InMemoryIndex {
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let field_set: HashSet<String> = fields.into_iter().map(Into::into).collect();
        let index = field_set
            .iter()
            .map(|f| (f.clone(), HashMap::<ScalarKey, HashSet<u64>>::new()))
            .collect();
        Self {
            fields: field_set,
            index,
        }
    }

    pub fn add(&mut self, doc: &serde_json::Map<String, Value>, offset: u64) {
        for field in self.fields.iter() {
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = ScalarKey::from_value(value) else {
                continue;
            };
            self.index
                .entry(field.clone())
                .or_default()
                .entry(key)
                .or_default()
                .insert(offset);
        }
    }

    pub fn lookup(&self, field: &str, value: &Value) -> Vec<u64> {
        let Some(field_index) = self.index.get(field) else {
            return Vec::new();
        };
        let Some(key) = ScalarKey::from_value(value) else {
            return Vec::new();
        };
        field_index
            .get(&key)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn remove(&mut self, doc: &serde_json::Map<String, Value>, offset: u64) {
        for field in self.fields.iter() {
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = ScalarKey::from_value(value) else {
                continue;
            };
            if let Some(field_index) = self.index.get_mut(field) {
                if let Some(offsets) = field_index.get_mut(&key) {
                    offsets.remove(&offset);
                }
            }
        }
    }

    pub fn clear(&mut self) {
        for values in self.index.values_mut() {
            values.clear();
        }
    }
}

use serde_json::Value;

use crate::Document;
use crate::WrongoDBError;

/// Apply MongoDB-style update operators or replacement
pub fn apply_update(doc: &Document, update: &Value) -> Result<Document, WrongoDBError> {
    let update_obj = match update.as_object() {
        Some(obj) => obj,
        None => return Ok(doc.clone()),
    };

    let is_update_operators = update_obj.keys().any(|k| k.starts_with('$'));

    if !is_update_operators {
        let mut new_doc = update_obj.clone();
        if let Some(id) = doc.get("_id") {
            new_doc.insert("_id".to_string(), id.clone());
        }
        return Ok(new_doc);
    }

    let mut new_doc = doc.clone();

    // $set
    if let Some(Value::Object(set_fields)) = update_obj.get("$set") {
        for (k, v) in set_fields {
            new_doc.insert(k.clone(), v.clone());
        }
    }

    // $unset
    if let Some(Value::Object(unset_fields)) = update_obj.get("$unset") {
        for k in unset_fields.keys() {
            new_doc.remove(k);
        }
    }

    // $inc
    if let Some(Value::Object(inc_fields)) = update_obj.get("$inc") {
        for (k, v) in inc_fields {
            if let Some(inc_val) = v.as_f64() {
                let current = new_doc.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                new_doc.insert(
                    k.clone(),
                    Value::Number(
                        serde_json::Number::from_f64(current + inc_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
            }
        }
    }

    // $push
    if let Some(Value::Object(push_fields)) = update_obj.get("$push") {
        for (k, v) in push_fields {
            let arr = new_doc
                .entry(k.clone())
                .or_insert_with(|| Value::Array(vec![]));
            if let Value::Array(ref mut arr_vec) = arr {
                arr_vec.push(v.clone());
            }
        }
    }

    // $pull
    if let Some(Value::Object(pull_fields)) = update_obj.get("$pull") {
        for (k, v) in pull_fields {
            if let Some(Value::Array(arr)) = new_doc.get_mut(k) {
                arr.retain(|item| item != v);
            }
        }
    }

    Ok(new_doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_apply_update_set() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$set": {"age": 31}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("age").unwrap().as_i64().unwrap(), 31);
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "alice");
    }

    #[test]
    fn test_apply_update_unset() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$unset": {"age": ""}});
        let result = apply_update(&doc, &update).unwrap();
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("name"));
    }

    #[test]
    fn test_apply_update_inc() {
        let doc = serde_json::from_value(json!({"_id": 1, "counter": 10})).unwrap();
        let update = json!({"$inc": {"counter": 5}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("counter").unwrap().as_f64().unwrap(), 15.0);
    }

    #[test]
    fn test_apply_update_push() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b"]})).unwrap();
        let update = json!({"$push": {"tags": "c"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[2].as_str().unwrap(), "c");
    }

    #[test]
    fn test_apply_update_pull() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b", "c"]})).unwrap();
        let update = json!({"$pull": {"tags": "b"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(!tags.iter().any(|t| t.as_str() == Some("b")));
    }

    #[test]
    fn test_apply_update_replacement() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"name": "bob", "status": "active"});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "bob");
        assert_eq!(result.get("status").unwrap().as_str().unwrap(), "active");
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("_id")); // _id preserved
    }
}

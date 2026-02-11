//! Predicate types for filter queries.
//!
//! Mirrors the existing Python (`advanced_filters/schema.py`) and TypeScript
//! (`filter-api.ts`) Predicate types. Used as the output format for the
//! backward solver and SPARQL-to-SQL translator.
//!
//! Serialization matches the existing frontend format:
//! - Simple: `{"fieldId": "...", "predicateTypeId": "...", "value": ...}`
//! - Expression: `{"operator": "AND"|"OR", "predicates": [...]}`
//! - Negated: `{"operator": "NOT", "predicate": {...}}`

use serde::{Deserialize, Serialize};

/// Logical operators for combining predicates.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogicalOperator {
    And,
    Or,
}

/// A filter predicate — the universal interchange format between the
/// constraint solver, the frontend `FilterQuery` system, and the backend
/// `filter_query_interpreter`.
///
/// Uses `#[serde(untagged)]` so the JSON format matches the existing
/// frontend and Python conventions (no `"type"` discriminator field).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Predicate {
    /// Boolean combination of predicates.
    /// Must be before Simple so `operator`+`predicates` is tried first.
    Expression {
        operator: LogicalOperator,
        predicates: Vec<Predicate>,
    },
    /// Negation of a predicate.
    /// Has `operator: "NOT"` to match the frontend `NegatedPredicate` type.
    Negated {
        operator: NegateOperator,
        predicate: Box<Predicate>,
    },
    /// A simple field-level predicate (e.g., "zone equals Zone 4").
    /// Tried last because it's the most permissive structurally.
    Simple {
        #[serde(rename = "fieldId")]
        field_id: String,
        #[serde(rename = "predicateTypeId")]
        predicate_type_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<serde_json::Value>,
    },
}

/// The NOT operator, serialized as the string "NOT".
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NegateOperator {
    NOT,
}

impl Predicate {
    /// Create a simple predicate.
    pub fn simple(field_id: impl Into<String>, op: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        Predicate::Simple {
            field_id: field_id.into(),
            predicate_type_id: op.into(),
            value: Some(value.into()),
        }
    }

    /// Create a simple predicate with no value (e.g., "exists").
    pub fn simple_no_value(field_id: impl Into<String>, op: impl Into<String>) -> Self {
        Predicate::Simple {
            field_id: field_id.into(),
            predicate_type_id: op.into(),
            value: None,
        }
    }

    /// AND-combine multiple predicates. Flattens nested ANDs.
    pub fn and(predicates: Vec<Predicate>) -> Self {
        let mut flat = Vec::new();
        for p in predicates {
            match p {
                Predicate::Expression { operator: LogicalOperator::And, predicates: children } => {
                    flat.extend(children);
                }
                other => flat.push(other),
            }
        }
        if flat.len() == 1 {
            return flat.into_iter().next().unwrap();
        }
        Predicate::Expression {
            operator: LogicalOperator::And,
            predicates: flat,
        }
    }

    /// OR-combine multiple predicates. Flattens nested ORs.
    pub fn or(predicates: Vec<Predicate>) -> Self {
        let mut flat = Vec::new();
        for p in predicates {
            match p {
                Predicate::Expression { operator: LogicalOperator::Or, predicates: children } => {
                    flat.extend(children);
                }
                other => flat.push(other),
            }
        }
        if flat.len() == 1 {
            return flat.into_iter().next().unwrap();
        }
        Predicate::Expression {
            operator: LogicalOperator::Or,
            predicates: flat,
        }
    }

    /// Negate a predicate.
    pub fn not(predicate: Predicate) -> Self {
        Predicate::Negated {
            operator: NegateOperator::NOT,
            predicate: Box::new(predicate),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_predicate_json_roundtrip() {
        let pred = Predicate::simple("zone", "equals", "Zone 4");
        let json = serde_json::to_string(&pred).unwrap();
        let parsed: Predicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, parsed);

        // Verify JSON field names match frontend convention (no "type" field)
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("type").is_none(), "should not have a 'type' discriminator");
        assert_eq!(value["fieldId"], "zone");
        assert_eq!(value["predicateTypeId"], "equals");
        assert_eq!(value["value"], "Zone 4");
    }

    #[test]
    fn test_expression_json_roundtrip() {
        let pred = Predicate::and(vec![
            Predicate::simple("zone", "equals", "Zone 4"),
            Predicate::not(Predicate::simple("status", "equals", "deleted")),
        ]);
        let json = serde_json::to_string(&pred).unwrap();
        let parsed: Predicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, parsed);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["operator"], "AND");
        assert!(value.get("type").is_none());
    }

    #[test]
    fn test_negated_json_roundtrip() {
        let pred = Predicate::not(Predicate::simple("status", "equals", "Verkocht"));
        let json = serde_json::to_string(&pred).unwrap();
        let parsed: Predicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, parsed);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["operator"], "NOT");
        assert!(value["predicate"].is_object());
    }

    #[test]
    fn test_and_flattening() {
        let inner = Predicate::and(vec![
            Predicate::simple("a", "equals", "1"),
            Predicate::simple("b", "equals", "2"),
        ]);
        let outer = Predicate::and(vec![
            inner,
            Predicate::simple("c", "equals", "3"),
        ]);
        // Should flatten to a single AND with 3 children
        match &outer {
            Predicate::Expression { operator: LogicalOperator::And, predicates } => {
                assert_eq!(predicates.len(), 3);
            }
            _ => panic!("expected AND expression"),
        }
    }

    #[test]
    fn test_single_element_and_unwraps() {
        let pred = Predicate::and(vec![
            Predicate::simple("zone", "equals", "Zone 4"),
        ]);
        // Single-element AND should unwrap to the element itself
        match &pred {
            Predicate::Simple { field_id, .. } => assert_eq!(field_id, "zone"),
            _ => panic!("expected Simple predicate, got {:?}", pred),
        }
    }

    #[test]
    fn test_deserialize_frontend_format() {
        // Verify we can parse the exact format the frontend produces
        let frontend_json = r#"{
            "operator": "AND",
            "predicates": [
                {"fieldId": "asset_type", "predicateTypeId": "equals", "value": "TunnelComponent"},
                {"operator": "NOT", "predicate": {"fieldId": "status", "predicateTypeId": "equals", "value": "deleted"}}
            ]
        }"#;
        let pred: Predicate = serde_json::from_str(frontend_json).unwrap();
        match &pred {
            Predicate::Expression { operator: LogicalOperator::And, predicates } => {
                assert_eq!(predicates.len(), 2);
                match &predicates[1] {
                    Predicate::Negated { operator: NegateOperator::NOT, .. } => {}
                    other => panic!("expected Negated, got {:?}", other),
                }
            }
            other => panic!("expected Expression, got {:?}", other),
        }
    }
}

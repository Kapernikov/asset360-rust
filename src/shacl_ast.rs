//! SHACL AST types for introspectable business rules.
//!
//! Represents the restricted SHACL subset that can be both evaluated forward
//! (validation) and backward (UI dropdown filtering). See spec.md FR2.

use serde::{Deserialize, Serialize};

/// A SHACL property path expression.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type")]
pub enum PropertyPath {
    /// A simple IRI predicate path (e.g., `asset360:ceAssetPrimaryStatus`).
    Iri { iri: String },
    /// A sequence path: follow multiple predicates in order (e.g., `(ex:parent ex:parent)`).
    Sequence { steps: Vec<PropertyPath> },
    /// An inverse path: follow a predicate backward (e.g., `[ sh:inversePath ex:parent ]`).
    Inverse { path: Box<PropertyPath> },
}

impl PropertyPath {
    pub fn iri(iri: impl Into<String>) -> Self {
        PropertyPath::Iri { iri: iri.into() }
    }

    pub fn sequence(steps: Vec<PropertyPath>) -> Self {
        PropertyPath::Sequence { steps }
    }

    pub fn inverse(path: PropertyPath) -> Self {
        PropertyPath::Inverse {
            path: Box::new(path),
        }
    }

    /// Extract the local name from an IRI path (last segment after `/` or `#`).
    /// Returns None for non-IRI paths.
    pub fn local_name(&self) -> Option<&str> {
        match self {
            PropertyPath::Iri { iri } => {
                // Try fragment (#) first, then last path segment (/)
                iri.rsplit_once('#')
                    .or_else(|| iri.rsplit_once('/'))
                    .map(|(_, name)| name)
            }
            _ => None,
        }
    }
}

/// Abstract syntax tree for a SHACL constraint in the restricted subset.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type")]
pub enum ShaclAst {
    /// All children must hold.
    And { children: Vec<ShaclAst> },
    /// At least one child must hold.
    Or { children: Vec<ShaclAst> },
    /// Child must not hold.
    Not { child: Box<ShaclAst> },
    /// Field at `path` has exact value.
    PropEquals {
        path: PropertyPath,
        value: serde_json::Value,
    },
    /// Field value at `path` is in the given set.
    PropIn {
        path: PropertyPath,
        values: Vec<serde_json::Value>,
    },
    /// Field at `path` has cardinality between min and max.
    PropCount {
        path: PropertyPath,
        min: Option<u32>,
        max: Option<u32>,
    },
    /// Values at `path_a` must equal values at `path_b`.
    PathEquals {
        path_a: PropertyPath,
        path_b: PropertyPath,
    },
    /// Values at `path_a` must not overlap with values at `path_b`.
    PathDisjoint {
        path_a: PropertyPath,
        path_b: PropertyPath,
    },
}

/// Enforcement level for a constraint violation.
///
/// - `Critical` / `Serious`: block publication
/// - `Error` / `Unlikely`: informational only
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EnforcementLevel {
    Critical,
    #[default]
    Serious,
    Error,
    Unlikely,
}

impl EnforcementLevel {
    /// Whether this level blocks publication.
    pub fn is_blocking(&self) -> bool {
        matches!(self, EnforcementLevel::Critical | EnforcementLevel::Serious)
    }
}

/// Result of parsing a single SHACL shape.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ShapeResult {
    /// URI of the SHACL shape (e.g., `asset360:TunnelComponent_ForbiddenStatusComboShape`).
    pub shape_uri: String,
    /// Target class name (e.g., `TunnelComponent`).
    pub target_class: String,
    /// Enforcement level for violations from this shape.
    pub enforcement_level: EnforcementLevel,
    /// Human-readable violation message.
    pub message: String,
    /// Field names referenced by this constraint (for change-triggered re-evaluation).
    pub affected_fields: Vec<String>,
    /// Whether this shape is in the introspectable subset (no SPARQL).
    pub introspectable: bool,
    /// Parsed AST (only if `introspectable` is true).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast: Option<ShaclAst>,
    /// Raw SPARQL select string (only if `introspectable` is false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparql: Option<String>,
}

/// A violation produced by forward evaluation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Violation {
    /// Field path(s) involved in the violation.
    pub fields: Vec<String>,
    /// Human-readable explanation.
    pub message: String,
    /// Enforcement level.
    pub enforcement_level: EnforcementLevel,
    /// Optional suggested fix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_fix: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_path_local_name() {
        let path = PropertyPath::iri("https://data.infrabel.be/asset360/ceAssetPrimaryStatus");
        assert_eq!(path.local_name(), Some("ceAssetPrimaryStatus"));

        let path = PropertyPath::iri("http://example.org/schema#name");
        assert_eq!(path.local_name(), Some("name"));

        let path = PropertyPath::sequence(vec![]);
        assert_eq!(path.local_name(), None);
    }

    #[test]
    fn test_enforcement_level_blocking() {
        assert!(EnforcementLevel::Critical.is_blocking());
        assert!(EnforcementLevel::Serious.is_blocking());
        assert!(!EnforcementLevel::Error.is_blocking());
        assert!(!EnforcementLevel::Unlikely.is_blocking());
    }

    #[test]
    fn test_ast_json_roundtrip() {
        let ast = ShaclAst::Not {
            child: Box::new(ShaclAst::Or {
                children: vec![ShaclAst::And {
                    children: vec![
                        ShaclAst::PropEquals {
                            path: PropertyPath::iri("asset360:ceAssetPrimaryStatus"),
                            value: serde_json::Value::String("In_voorbereiding".into()),
                        },
                        ShaclAst::PropEquals {
                            path: PropertyPath::iri("asset360:ceAssetSecondaryStatus"),
                            value: serde_json::Value::String("Verkocht".into()),
                        },
                    ],
                }],
            }),
        };
        let json = serde_json::to_string(&ast).unwrap();
        let parsed: ShaclAst = serde_json::from_str(&json).unwrap();
        assert_eq!(ast, parsed);
    }

    #[test]
    fn test_shape_result_json_roundtrip() {
        let shape = ShapeResult {
            shape_uri: "asset360:TestShape".into(),
            target_class: "TunnelComponent".into(),
            enforcement_level: EnforcementLevel::Serious,
            message: "Test violation".into(),
            affected_fields: vec!["field1".into(), "field2".into()],
            introspectable: true,
            ast: Some(ShaclAst::PropEquals {
                path: PropertyPath::iri("asset360:field1"),
                value: serde_json::json!("value"),
            }),
            sparql: None,
        };
        let json = serde_json::to_string(&shape).unwrap();
        let parsed: ShapeResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.shape_uri, "asset360:TestShape");
        assert_eq!(parsed.enforcement_level, EnforcementLevel::Serious);
        assert!(parsed.ast.is_some());
        assert!(parsed.sparql.is_none());
    }

    #[test]
    fn test_violation_json() {
        let v = Violation {
            fields: vec![
                "ceAssetPrimaryStatus".into(),
                "ceAssetSecondaryStatus".into(),
            ],
            message: "Forbidden status combination".into(),
            enforcement_level: EnforcementLevel::Serious,
            suggested_fix: Some("Change secondary status".into()),
        };
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("\"enforcement_level\":\"serious\""));
    }
}

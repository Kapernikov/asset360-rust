//! SHACL Turtle parser → AST.
//!
//! Parses `.shacl.ttl` files into [`ShapeResult`] structs. Feature-gated
//! behind `shacl-parser` (uses `oxttl`/`oxrdf`).

use std::collections::HashMap;
use std::fmt;

use oxrdf::{Literal, NamedOrBlankNode, Term};
use oxttl::TurtleParser;

use crate::shacl_ast::*;

// ── Well-known IRIs ──────────────────────────────────────────────────

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

const ASSET360: &str = "https://data.infrabel.be/asset360/";

fn sh(local: &str) -> String {
    format!("{SH}{local}")
}

fn a360(local: &str) -> String {
    format!("{ASSET360}{local}")
}

// ── Error type ───────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ParseError {
    Turtle(String),
    UnsupportedConstruct(String),
    MissingField(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Turtle(e) => write!(f, "Turtle parse error: {e}"),
            ParseError::UnsupportedConstruct(msg) => {
                write!(f, "Unsupported SHACL construct: {msg}")
            }
            ParseError::MissingField(msg) => write!(f, "Missing required field: {msg}"),
        }
    }
}

impl std::error::Error for ParseError {}

// ── Triple store ─────────────────────────────────────────────────────

/// Simple in-memory triple store for walking parsed RDF.
struct TripleStore {
    /// All triples indexed by subject string.
    by_subject: HashMap<String, Vec<(String, Term)>>,
}

impl TripleStore {
    fn parse(ttl: &str) -> Result<Self, ParseError> {
        let mut by_subject: HashMap<String, Vec<(String, Term)>> = HashMap::new();
        let parser = TurtleParser::new().for_reader(ttl.as_bytes());
        for result in parser {
            let triple = result.map_err(|e| ParseError::Turtle(e.to_string()))?;
            let subj_key = subject_key(&triple.subject);
            by_subject
                .entry(subj_key)
                .or_default()
                .push((triple.predicate.as_str().to_owned(), triple.object));
        }
        Ok(Self { by_subject })
    }

    fn objects(&self, subject: &str, predicate: &str) -> Vec<&Term> {
        self.by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .filter(|(p, _)| p == predicate)
                    .map(|(_, o)| o)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn first_object(&self, subject: &str, predicate: &str) -> Option<&Term> {
        self.objects(subject, predicate).into_iter().next()
    }

    fn first_str(&self, subject: &str, predicate: &str) -> Option<String> {
        self.first_object(subject, predicate).and_then(term_str)
    }

    fn first_literal(&self, subject: &str, predicate: &str) -> Option<String> {
        self.first_object(subject, predicate).and_then(|t| match t {
            Term::Literal(lit) => Some(lit.value().to_owned()),
            _ => None,
        })
    }

    /// Pick the best literal for `predicate` given a preferred language.
    ///
    /// Priority: exact language match > untagged literal > first available.
    fn literal_for_language(
        &self,
        subject: &str,
        predicate: &str,
        language: &str,
    ) -> Option<String> {
        let literals: Vec<&Literal> = self
            .objects(subject, predicate)
            .into_iter()
            .filter_map(|t| match t {
                Term::Literal(lit) => Some(lit),
                _ => None,
            })
            .collect();
        if literals.is_empty() {
            return None;
        }
        // Exact language match
        if let Some(lit) = literals.iter().find(|l| l.language() == Some(language)) {
            return Some(lit.value().to_owned());
        }
        // Untagged fallback
        if let Some(lit) = literals.iter().find(|l| l.language().is_none()) {
            return Some(lit.value().to_owned());
        }
        // First available
        Some(literals[0].value().to_owned())
    }

    /// List the distinct predicates present on a given subject node (for diagnostics).
    fn list_predicates(&self, subject: &str) -> Vec<String> {
        let mut predicates: Vec<String> = self
            .by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|(p, _)| iri_local_name(p).to_owned())
                    .collect()
            })
            .unwrap_or_default();
        predicates.sort();
        predicates.dedup();
        predicates
    }

    /// Local names of the predicates on `subject` that live in the SHACL
    /// namespace. Used to detect SHACL constraints the introspectable engine
    /// does not support (so we can refuse to claim a shape we cannot fully
    /// evaluate, rather than silently dropping the unsupported part).
    fn shacl_predicates(&self, subject: &str) -> Vec<String> {
        let mut predicates: Vec<String> = self
            .by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .filter(|(p, _)| p.starts_with(SH))
                    .map(|(p, _)| iri_local_name(p).to_owned())
                    .collect()
            })
            .unwrap_or_default();
        predicates.sort();
        predicates.dedup();
        predicates
    }

    /// Collect an RDF list (rdf:first/rdf:rest chain) starting from a term.
    fn collect_rdf_list<'a>(&'a self, head: &'a Term) -> Vec<&'a Term> {
        let mut result = Vec::new();
        let mut current = head;
        loop {
            let key = term_key(current);
            if key == RDF_NIL {
                break;
            }
            if let Some(first) = self.first_object(&key, RDF_FIRST) {
                result.push(first);
            } else {
                break;
            }
            if let Some(rest) = self.first_object(&key, RDF_REST) {
                current = rest;
            } else {
                break;
            }
        }
        result
    }
}

fn subject_key(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => n.as_str().to_owned(),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
    }
}

#[allow(unreachable_patterns)]
fn term_key(t: &Term) -> String {
    match t {
        Term::NamedNode(n) => n.as_str().to_owned(),
        Term::BlankNode(b) => format!("_:{}", b.as_str()),
        Term::Literal(l) => l.value().to_owned(),
        _ => String::new(),
    }
}

fn term_str(t: &Term) -> Option<String> {
    match t {
        Term::NamedNode(n) => Some(n.as_str().to_owned()),
        Term::Literal(l) => Some(l.value().to_owned()),
        _ => None,
    }
}

fn iri_local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .map(|(_, name)| name)
        .unwrap_or(iri)
}

// ── Public API ───────────────────────────────────────────────────────

/// Parse a SHACL Turtle file and extract shapes targeting `target_class`.
///
/// If `target_class` is empty, all shapes are returned.
/// `language` selects the preferred language for `sh:message` (e.g. `"nl"`, `"en"`).
/// When empty, the first available literal is used.
pub fn parse_shacl(
    ttl: &str,
    target_class: &str,
    language: &str,
) -> Result<Vec<ShapeResult>, ParseError> {
    let store = TripleStore::parse(ttl)?;
    let mut results = Vec::new();

    // Find all sh:NodeShape subjects
    for (subj, pairs) in &store.by_subject {
        let is_node_shape = pairs
            .iter()
            .any(|(p, o)| p == RDF_TYPE && term_key(o) == sh("NodeShape"));
        if !is_node_shape {
            continue;
        }

        // Check target class
        let shape_target = store.first_str(subj, &sh("targetClass"));
        if !target_class.is_empty() {
            if let Some(ref tc) = shape_target {
                let tc_local = iri_local_name(tc);
                if tc_local != target_class && tc != target_class {
                    continue;
                }
            } else {
                continue;
            }
        }

        let target_class_name = shape_target
            .as_deref()
            .map(iri_local_name)
            .unwrap_or("")
            .to_owned();

        // Read annotations
        let enforcement_str = store
            .first_literal(subj, &a360("enforcementLevel"))
            .unwrap_or_else(|| "serious".to_owned());
        let enforcement_level = match enforcement_str.as_str() {
            "critical" => EnforcementLevel::Critical,
            "serious" => EnforcementLevel::Serious,
            "error" => EnforcementLevel::Error,
            "unlikely" => EnforcementLevel::Unlikely,
            _ => EnforcementLevel::default(),
        };

        let introspectable_ann = store
            .first_literal(subj, &a360("introspectable"))
            .map(|s| s == "true")
            .unwrap_or(true); // default: attempt introspection

        let message = store
            .literal_for_language(subj, &sh("message"), language)
            .unwrap_or_default();

        // Check if shape uses SPARQL
        let sparql_node = store.first_object(subj, &sh("sparql"));
        if let Some(sparql_term) = sparql_node {
            let sparql_key = term_key(sparql_term);
            let select = store
                .first_literal(&sparql_key, &sh("select"))
                .unwrap_or_default();
            let sparql_message = store
                .literal_for_language(&sparql_key, &sh("message"), language)
                .unwrap_or_else(|| message.clone());

            // Extract affected fields from SPARQL BIND patterns
            let affected_fields = extract_bind_fields_from_sparql(&select);

            results.push(ShapeResult {
                shape_uri: subj.clone(),
                target_class: target_class_name,
                enforcement_level,
                message: sparql_message,
                affected_fields,
                introspectable: false,
                ast: None,
                sparql: Some(select),
            });
            continue;
        }

        // A shape explicitly marked `introspectable false` is owned by pyshacl;
        // the Rust engine must NOT evaluate it, even when its body happens to
        // parse into a valid AST (e.g. a plain `sh:in` / `sh:minCount` shape).
        // Keeping an AST here let the forward evaluator and backward solver pick
        // it up by `ast.is_some()`, corrupting evaluation of the genuinely
        // introspectable shapes on the same class. Treat it as opaque.
        if !introspectable_ann {
            results.push(ShapeResult {
                shape_uri: subj.clone(),
                target_class: target_class_name,
                enforcement_level,
                message,
                affected_fields: vec![],
                introspectable: false,
                ast: None,
                sparql: None,
            });
            continue;
        }

        // Try to parse as introspectable AST
        match parse_shape_ast(&store, subj) {
            Ok(ast) => {
                let affected_fields = collect_affected_fields(&ast);
                results.push(ShapeResult {
                    shape_uri: subj.clone(),
                    target_class: target_class_name,
                    enforcement_level,
                    message,
                    affected_fields,
                    introspectable: true,
                    ast: Some(ast),
                    sparql: None,
                });
            }
            Err(e) => {
                // Default-introspectable shape whose body is outside the
                // supported subset and carries no SPARQL — surface the error.
                return Err(e);
            }
        }
    }
    Ok(results)
}

// ── AST building ─────────────────────────────────────────────────────

fn parse_shape_ast(store: &TripleStore, shape_key: &str) -> Result<ShaclAst, ParseError> {
    // Collect all constraint components on this shape node
    let mut constraints = Vec::new();

    // Recognize the standard-SHACL "unique by member field" idiom (a sequence
    // sh:in + a cluster of sh:qualifiedValueShape/sh:qualifiedMaxCount blocks on
    // one slot) and lower it to a single UniqueByMemberField AST. The consumed
    // sh:property nodes are skipped below so the fail-closed property parser
    // doesn't reject the (otherwise unsupported) qualified blocks.
    let mut consumed_props: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some((ast, consumed)) = recognize_unique_by_member(store, shape_key) {
        constraints.push(ast);
        consumed_props = consumed;
    }

    // sh:not
    for obj in store.objects(shape_key, &sh("not")) {
        let inner = parse_constraint_node(store, &term_key(obj))?;
        constraints.push(ShaclAst::Not {
            child: Box::new(inner),
        });
    }

    // sh:and (top-level)
    for obj in store.objects(shape_key, &sh("and")) {
        let items = store.collect_rdf_list(obj);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        constraints.push(ShaclAst::And { children });
    }

    // sh:or (top-level)
    for obj in store.objects(shape_key, &sh("or")) {
        let items = store.collect_rdf_list(obj);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        constraints.push(ShaclAst::Or { children });
    }

    // sh:property (top-level property shapes), skipping any consumed by the
    // unique-by-member recognizer above.
    for obj in store.objects(shape_key, &sh("property")) {
        let prop_key = term_key(obj);
        if consumed_props.contains(&prop_key) {
            continue;
        }
        let prop_ast = parse_property_shape(store, &prop_key)?;
        constraints.push(prop_ast);
    }

    // sh:equals (property pair, top-level)
    if let (Some(path_a_term), Some(path_b_term)) = (
        store.first_object(shape_key, &sh("path")),
        store.first_object(shape_key, &sh("equals")),
    ) {
        constraints.push(ShaclAst::PathEquals {
            path_a: parse_path(store, path_a_term)?,
            path_b: parse_path(store, path_b_term)?,
        });
    }

    // sh:disjoint (property pair, top-level)
    if let (Some(path_a_term), Some(path_b_term)) = (
        store.first_object(shape_key, &sh("path")),
        store.first_object(shape_key, &sh("disjoint")),
    ) {
        constraints.push(ShaclAst::PathDisjoint {
            path_a: parse_path(store, path_a_term)?,
            path_b: parse_path(store, path_b_term)?,
        });
    }

    match constraints.len() {
        0 => Err(ParseError::MissingField(format!(
            "no constraint components found on shape {shape_key}"
        ))),
        1 => Ok(constraints.into_iter().next().unwrap()),
        _ => Ok(ShaclAst::And {
            children: constraints,
        }),
    }
}

fn parse_constraint_node(store: &TripleStore, key: &str) -> Result<ShaclAst, ParseError> {
    // Check what constraint components exist on this blank node

    // sh:not
    if let Some(inner) = store.first_object(key, &sh("not")) {
        let child = parse_constraint_node(store, &term_key(inner))?;
        return Ok(ShaclAst::Not {
            child: Box::new(child),
        });
    }

    // sh:and
    if let Some(list_head) = store.first_object(key, &sh("and")) {
        let items = store.collect_rdf_list(list_head);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ShaclAst::And { children });
    }

    // sh:or
    if let Some(list_head) = store.first_object(key, &sh("or")) {
        let items = store.collect_rdf_list(list_head);
        let children = items
            .into_iter()
            .map(|item| parse_constraint_node(store, &term_key(item)))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(ShaclAst::Or { children });
    }

    // sh:property (nested property shape)
    if let Some(prop_node) = store.first_object(key, &sh("property")) {
        return parse_property_shape(store, &term_key(prop_node));
    }

    // This node might itself be a property shape (has sh:path)
    if store.first_object(key, &sh("path")).is_some() {
        return parse_property_shape(store, key);
    }

    let predicates = store.list_predicates(key);
    Err(ParseError::UnsupportedConstruct(format!(
        "Unsupported SHACL construct on node {key}.\n\
         Found predicates: [{}].\n\
         Supported: sh:not, sh:and, sh:or, sh:property (with sh:path + value constraint).\n\
         Hint: Set `asset360:introspectable false` and use `sh:sparql` instead.",
        predicates.join(", ")
    )))
}

/// SHACL constraint/annotation predicates (local names) the introspectable
/// engine fully understands on a property shape. `sh:path` is the path itself;
/// the value-constraint set matches what `parse_property_shape` can lower to an
/// AST; the trailing entries are non-constraint annotations safe to ignore.
const SUPPORTED_PROPERTY_SHACL: &[&str] = &[
    "path",
    // value constraints lowered to an AST below:
    "hasValue",
    "in",
    "minCount",
    "maxCount",
    "equals",
    "disjoint",
    // non-constraint annotations:
    "message",
    "name",
    "description",
    "order",
    "group",
    "severity",
    "deactivated",
];

fn parse_property_shape(store: &TripleStore, key: &str) -> Result<ShaclAst, ParseError> {
    let path_term = store.first_object(key, &sh("path")).ok_or_else(|| {
        ParseError::MissingField(format!("sh:path missing on property shape {key}"))
    })?;
    let path = parse_path(store, path_term)?;

    // Fail-closed: only introspect a property shape if EVERY SHACL constraint on
    // it is one we can fully lower to the AST. If it carries any other SHACL
    // constraint (e.g. sh:minLength, sh:pattern, sh:datatype, sh:nodeKind), we
    // must NOT emit a partial AST that silently ignores it — the forward/backward
    // evaluators would then report a verdict that omits a real constraint. We
    // refuse here; the shape falls back to pyshacl (the complete engine) via the
    // `introspectable false` path, or — if it was marked `introspectable true` —
    // surfaces as an authoring error in `parse_shacl`.
    if let Some(unsupported) = store
        .shacl_predicates(key)
        .into_iter()
        .find(|local| !SUPPORTED_PROPERTY_SHACL.contains(&local.as_str()))
    {
        let path_name = path.local_name().unwrap_or("(complex path)");
        return Err(ParseError::UnsupportedConstruct(format!(
            "Unsupported value constraint sh:{unsupported} on property \"{path_name}\" (node {key}).\n\
             Supported property constraints: sh:hasValue, sh:in, sh:minCount, sh:maxCount, sh:equals, sh:disjoint.\n\
             Set `asset360:introspectable false` so pyshacl evaluates this shape."
        )));
    }

    // sh:hasValue
    if let Some(val_term) = store.first_object(key, &sh("hasValue")) {
        let value = term_to_json_value(val_term);
        return Ok(ShaclAst::PropEquals { path, value });
    }

    // sh:in
    if let Some(list_head) = store.first_object(key, &sh("in")) {
        let items = store.collect_rdf_list(list_head);
        let values = items.into_iter().map(term_to_json_value).collect();
        return Ok(ShaclAst::PropIn { path, values });
    }

    // sh:minCount / sh:maxCount
    let min_count = store
        .first_literal(key, &sh("minCount"))
        .and_then(|s| s.parse::<u32>().ok());
    let max_count = store
        .first_literal(key, &sh("maxCount"))
        .and_then(|s| s.parse::<u32>().ok());
    if min_count.is_some() || max_count.is_some() {
        return Ok(ShaclAst::PropCount {
            path,
            min: min_count,
            max: max_count,
        });
    }

    // sh:equals (property pair constraint)
    if let Some(other_path_term) = store.first_object(key, &sh("equals")) {
        let other_path = parse_path(store, other_path_term)?;
        return Ok(ShaclAst::PathEquals {
            path_a: path,
            path_b: other_path,
        });
    }

    // sh:disjoint (property pair constraint)
    if let Some(other_path_term) = store.first_object(key, &sh("disjoint")) {
        let other_path = parse_path(store, other_path_term)?;
        return Ok(ShaclAst::PathDisjoint {
            path_a: path,
            path_b: other_path,
        });
    }

    let path_name = path.local_name().unwrap_or("(complex path)");
    let predicates = store.list_predicates(key);
    Err(ParseError::UnsupportedConstruct(format!(
        "Unsupported value constraint on property \"{path_name}\" (node {key}).\n\
         Found predicates: [{}].\n\
         Supported property constraints: sh:hasValue, sh:in, sh:minCount, sh:maxCount, sh:equals, sh:disjoint.\n\
         Common unsupported: sh:pattern, sh:class, sh:nodeKind, sh:datatype, sh:minInclusive/maxInclusive, sh:minLength/maxLength.\n\
         Hint: Set `asset360:introspectable false` and use `sh:sparql` for this constraint.",
        predicates.join(", ")
    )))
}

/// Recognize the standard-SHACL "members of `slot` are unique by `member`,
/// restricted to an allowed set" idiom and lower it to `UniqueByMemberField`.
///
/// Canonical form on a NodeShape (any validator enforces it directly):
///   - one `sh:property` with `sh:path ( slot member )` and `sh:in ( … )`
///     — the allowed value set;
///   - one or more `sh:property` with `sh:path slot`,
///     `sh:qualifiedValueShape [ sh:path member ; sh:hasValue X ]` and a uniform
///     `sh:qualifiedMaxCount` — at most that many members per value.
///
/// Returns the lowered AST plus the set of consumed `sh:property` node keys.
/// Fails safe: any deviation (no qualified blocks, inconsistent slot/member,
/// non-uniform max count, complex paths) yields `None`, leaving the shape to
/// pyshacl for enforcement (no introspectable AST, no dropdown solving).
fn recognize_unique_by_member(
    store: &TripleStore,
    shape_key: &str,
) -> Option<(ShaclAst, std::collections::HashSet<String>)> {
    struct Allowed {
        array: PropertyPath,
        member: PropertyPath,
        values: Vec<serde_json::Value>,
        prop_key: String,
    }
    struct Qualified {
        array: PropertyPath,
        member: PropertyPath,
        max: u32,
        has_value: serde_json::Value,
        prop_key: String,
    }
    let mut allowed: Option<Allowed> = None;
    let mut qualified: Vec<Qualified> = Vec::new();

    for obj in store.objects(shape_key, &sh("property")) {
        let prop_key = term_key(obj);
        let path = parse_path(store, store.first_object(&prop_key, &sh("path"))?).ok()?;

        // (1) allowed-set block: sequence path (array member) + sh:in.
        if let Some(in_head) = store.first_object(&prop_key, &sh("in")) {
            // Fully fail-closed: no SHACL predicate beyond sh:path + sh:in.
            if !shacl_predicates_subset(store, &prop_key, &["path", "in"]) {
                return None;
            }
            let PropertyPath::Sequence { steps } = &path else {
                return None;
            };
            if steps.len() != 2 {
                return None;
            }
            if !matches!(
                (&steps[0], &steps[1]),
                (PropertyPath::Iri { .. }, PropertyPath::Iri { .. })
            ) {
                return None;
            }
            if allowed.is_some() {
                return None; // more than one allowed-set block — not canonical
            }
            allowed = Some(Allowed {
                array: steps[0].clone(),
                member: steps[1].clone(),
                values: store
                    .collect_rdf_list(in_head)
                    .into_iter()
                    .map(term_to_json_value)
                    .collect(),
                prop_key,
            });
            continue;
        }

        // (2) qualified block: sh:path array + qualifiedValueShape[path member; hasValue X] + qualifiedMaxCount.
        if let Some(qvs_term) = store.first_object(&prop_key, &sh("qualifiedValueShape")) {
            if !shacl_predicates_subset(
                store,
                &prop_key,
                &["path", "qualifiedValueShape", "qualifiedMaxCount"],
            ) {
                return None;
            }
            let qvs_key = term_key(qvs_term);
            if !shacl_predicates_subset(store, &qvs_key, &["path", "hasValue"]) {
                return None;
            }
            let member = parse_path(store, store.first_object(&qvs_key, &sh("path"))?).ok()?;
            if !matches!(
                (&path, &member),
                (PropertyPath::Iri { .. }, PropertyPath::Iri { .. })
            ) {
                return None;
            }
            let has_value = term_to_json_value(store.first_object(&qvs_key, &sh("hasValue"))?);
            let max = store
                .first_literal(&prop_key, &sh("qualifiedMaxCount"))
                .and_then(|s| s.parse::<u32>().ok())?;
            // qualifiedMaxCount 0 ("never present") has no coherent dropdown
            // semantics — fail closed so pyshacl stays authoritative.
            if max == 0 {
                return None;
            }
            qualified.push(Qualified {
                array: path,
                member,
                max,
                has_value,
                prop_key,
            });
            continue;
        }
    }

    // Need ≥1 qualified block; all must agree on array path, member path, max —
    // compared on the *full* IRI (PropertyPath: PartialEq), never local names.
    let first = qualified.first()?;
    let (array, member, max) = (first.array.clone(), first.member.clone(), first.max);
    if qualified
        .iter()
        .any(|q| q.array != array || q.member != member || q.max != max)
    {
        return None;
    }

    // Require the allowed-set block on the same array+member, and require the
    // qualified blocks to cover *exactly* the sh:in set — otherwise lowering
    // would change semantics vs the standard shapes; leave it to pyshacl.
    let allowed = allowed?;
    if allowed.array != array || allowed.member != member {
        return None;
    }
    let in_keys: std::collections::HashSet<String> =
        allowed.values.iter().map(value_to_key).collect();
    let qualified_keys: std::collections::HashSet<String> = qualified
        .iter()
        .map(|q| value_to_key(&q.has_value))
        .collect();
    if in_keys != qualified_keys {
        return None;
    }

    let mut consumed: std::collections::HashSet<String> =
        qualified.into_iter().map(|q| q.prop_key).collect();
    consumed.insert(allowed.prop_key);

    Some((
        ShaclAst::UniqueByMemberField {
            array_path: array,
            member_field: member,
            allowed_values: Some(allowed.values),
            max_count_per_value: max,
        },
        consumed,
    ))
}

/// True when every SHACL-namespace predicate on `node` is in `allowed` (by
/// local name). Used to keep the recognizer fully fail-closed: an extra
/// constraint (e.g. sh:qualifiedMinCount, sh:datatype, sh:minCount) on a
/// consumed node would otherwise be silently dropped, under-enforcing vs
/// pyshacl.
fn shacl_predicates_subset(store: &TripleStore, node: &str, allowed: &[&str]) -> bool {
    store
        .shacl_predicates(node)
        .iter()
        .all(|p| allowed.contains(&p.as_str()))
}

/// Stable string key for a JSON value (strings as-is, others stringified) —
/// used to compare the sh:in set against the qualified-block hasValue set.
fn value_to_key(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn parse_path(store: &TripleStore, term: &Term) -> Result<PropertyPath, ParseError> {
    match term {
        Term::NamedNode(n) => Ok(PropertyPath::iri(n.as_str())),
        Term::BlankNode(b) => {
            let key = format!("_:{}", b.as_str());

            // sh:inversePath
            if let Some(inner) = store.first_object(&key, &sh("inversePath")) {
                let inner_path = parse_path(store, inner)?;
                return Ok(PropertyPath::inverse(inner_path));
            }

            // RDF list (sequence path)
            let items = store.collect_rdf_list(term);
            if !items.is_empty() {
                let steps = items
                    .into_iter()
                    .map(|item| parse_path(store, item))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(PropertyPath::sequence(steps));
            }

            let predicates = store.list_predicates(&key);
            Err(ParseError::UnsupportedConstruct(format!(
                "Unsupported property path at blank node {key}.\n\
                 Found predicates: [{}].\n\
                 Supported paths: simple IRI, sequence (RDF list), sh:inversePath.\n\
                 Hint: sh:alternativePath, sh:zeroOrMorePath etc. are not supported.",
                predicates.join(", ")
            )))
        }
        _ => Err(ParseError::UnsupportedConstruct(format!(
            "Unexpected term in path position: {term:?}.\n\
             Paths must be IRIs (e.g. asset360:fieldName) or structured (sh:inversePath, sequence)."
        ))),
    }
}

#[allow(unreachable_patterns)]
fn term_to_json_value(t: &Term) -> serde_json::Value {
    match t {
        Term::Literal(l) => {
            // Try numeric
            if let Ok(n) = l.value().parse::<i64>() {
                return serde_json::Value::Number(n.into());
            }
            if let Ok(n) = l.value().parse::<f64>()
                && let Some(num) = serde_json::Number::from_f64(n)
            {
                return serde_json::Value::Number(num);
            }
            // Boolean
            match l.value() {
                "true" => return serde_json::Value::Bool(true),
                "false" => return serde_json::Value::Bool(false),
                _ => {}
            }
            serde_json::Value::String(l.value().to_owned())
        }
        Term::NamedNode(n) => serde_json::Value::String(n.as_str().to_owned()),
        Term::BlankNode(b) => serde_json::Value::String(format!("_:{}", b.as_str())),
        _ => serde_json::Value::Null,
    }
}

/// Extract local names of IRIs bound to `?path`-style output variables via
/// `BIND(<iri> AS ?var)` in a SHACL `sh:select` query.
///
/// Parses the query via `spargebra` and walks the algebra for
/// `GraphPattern::Extend` nodes whose expression is a `NamedNode`. Anything
/// richer (variables, function calls, arithmetic) is intentionally skipped:
/// the goal here is the "affected fields" hint for change tracking, not
/// general SPARQL evaluation.
///
/// Returns an empty `Vec` if the query fails to parse.
fn extract_bind_fields_from_sparql(sparql: &str) -> Vec<String> {
    use spargebra::{Query, SparqlParser};

    let parser = SparqlParser::new()
        .with_prefix("asset360", "https://data.infrabel.be/asset360/")
        .expect("hardcoded prefix is valid");

    let query = match parser.parse_query(sparql) {
        Ok(q) => q,
        Err(_) => return Vec::new(),
    };

    let pattern = match query {
        Query::Select { pattern, .. } => pattern,
        _ => return Vec::new(),
    };

    let mut fields = Vec::new();
    collect_extend_iris(&pattern, &mut fields);
    fields.sort();
    fields.dedup();
    fields
}

/// Recursively walk the algebra collecting local names of `BIND(<iri> AS ?v)`
/// extensions. Non-IRI expressions are skipped.
fn collect_extend_iris(pattern: &spargebra::algebra::GraphPattern, fields: &mut Vec<String>) {
    use spargebra::algebra::{Expression, GraphPattern};

    match pattern {
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            if let Expression::NamedNode(nn) = expression {
                fields.push(iri_local_name(nn.as_str()).to_owned());
            }
            collect_extend_iris(inner, fields);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_extend_iris(left, fields);
            collect_extend_iris(right, fields);
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. } => {
            collect_extend_iris(inner, fields);
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => {}
    }
}

fn collect_affected_fields(ast: &ShaclAst) -> Vec<String> {
    let mut fields = Vec::new();
    collect_fields_recursive(ast, &mut fields);
    fields.sort();
    fields.dedup();
    fields
}

fn collect_fields_recursive(ast: &ShaclAst, fields: &mut Vec<String>) {
    match ast {
        ShaclAst::And { children } | ShaclAst::Or { children } => {
            for child in children {
                collect_fields_recursive(child, fields);
            }
        }
        ShaclAst::Not { child } => collect_fields_recursive(child, fields),
        ShaclAst::PropEquals { path, .. }
        | ShaclAst::PropIn { path, .. }
        | ShaclAst::PropCount { path, .. } => {
            if let Some(name) = path.local_name() {
                fields.push(name.to_owned());
            }
        }
        ShaclAst::PathEquals { path_a, path_b } | ShaclAst::PathDisjoint { path_a, path_b } => {
            if let Some(name) = path_a.local_name() {
                fields.push(name.to_owned());
            }
            if let Some(name) = path_b.local_name() {
                fields.push(name.to_owned());
            }
        }
        ShaclAst::UniqueByMemberField {
            array_path,
            member_field,
            ..
        } => {
            if let Some(name) = array_path.local_name() {
                fields.push(name.to_owned());
            }
            if let Some(name) = member_field.local_name() {
                fields.push(name.to_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS_COMBO_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

asset360:TunnelComponent_ForbiddenStatusComboShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:message "Forbidden: ceAssetPrimaryStatus incompatible with ceAssetSecondaryStatus." ;
  sh:not [
    sh:or (
      [
        sh:and (
          [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
          [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Verkocht" ] ]
        )
      ]
      [
        sh:and (
          [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
          [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Afgebroken" ] ]
        )
      ]
    )
  ] .
"#;

    const DELEGATE_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TunnelComponent_DelegateUniquenessShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable false ;
  sh:sparql [
    sh:message "Only one tunnel component per tunnel complex can be marked as delegate." ;
    sh:select """
      SELECT $this ?path
      WHERE {
        $this asset360:belongsToTunnelComplex ?complex ;
              asset360:isTunnelDelegate true .
        ?other asset360:belongsToTunnelComplex ?complex ;
               asset360:isTunnelDelegate true .
        FILTER(?other != $this)
        { BIND(asset360:isTunnelDelegate AS ?path) }
        UNION
        { BIND(asset360:belongsToTunnelComplex AS ?path) }
      }
    """ ;
  ] .
"#;

    #[test]
    fn test_parse_introspectable_shape() {
        let results = parse_shacl(STATUS_COMBO_TTL, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 1);
        let shape = &results[0];
        assert!(shape.introspectable);
        assert_eq!(shape.target_class, "TunnelComponent");
        assert_eq!(shape.enforcement_level, EnforcementLevel::Serious);
        assert!(shape.ast.is_some());
        assert!(shape.sparql.is_none());

        // AST should be Not(Or(And(...), And(...)))
        let ast = shape.ast.as_ref().unwrap();
        match ast {
            ShaclAst::Not { child } => match child.as_ref() {
                ShaclAst::Or { children } => {
                    assert_eq!(children.len(), 2);
                    // Each child should be And with 2 PropEquals
                    for child in children {
                        match child {
                            ShaclAst::And { children: inner } => {
                                assert_eq!(inner.len(), 2);
                            }
                            _ => panic!("expected And, got {child:?}"),
                        }
                    }
                }
                _ => panic!("expected Or, got {child:?}"),
            },
            _ => panic!("expected Not, got {ast:?}"),
        }

        // Affected fields
        assert!(
            shape
                .affected_fields
                .contains(&"ceAssetPrimaryStatus".to_owned())
        );
        assert!(
            shape
                .affected_fields
                .contains(&"ceAssetSecondaryStatus".to_owned())
        );
    }

    #[test]
    fn test_parse_sparql_shape() {
        let results = parse_shacl(DELEGATE_TTL, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 1);
        let shape = &results[0];
        assert!(!shape.introspectable);
        assert!(shape.ast.is_none());
        assert!(shape.sparql.is_some());
        assert_eq!(shape.enforcement_level, EnforcementLevel::Serious);
        assert!(shape.message.contains("Only one tunnel component"));

        // SPARQL-extracted fields
        assert!(
            shape
                .affected_fields
                .contains(&"isTunnelDelegate".to_owned())
        );
        assert!(
            shape
                .affected_fields
                .contains(&"belongsToTunnelComplex".to_owned())
        );
    }

    #[test]
    fn test_parse_combined_file() {
        let combined = format!("{STATUS_COMBO_TTL}\n{DELEGATE_TTL}");
        let results = parse_shacl(&combined, "TunnelComponent", "").unwrap();
        assert_eq!(results.len(), 2);

        let introspectable_count = results.iter().filter(|r| r.introspectable).count();
        let sparql_count = results.iter().filter(|r| r.sparql.is_some()).count();
        assert_eq!(introspectable_count, 1);
        assert_eq!(sparql_count, 1);
    }

    #[test]
    fn test_parse_empty_target_class_returns_all() {
        let results = parse_shacl(STATUS_COMBO_TTL, "", "").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_wrong_target_class_returns_empty() {
        let results = parse_shacl(STATUS_COMBO_TTL, "Signal", "").unwrap();
        assert_eq!(results.len(), 0);
    }

    // ── Error message quality tests ─────────────────────────────────

    fn assert_error_contains(result: Result<Vec<ShapeResult>, ParseError>, needles: &[&str]) {
        let err = result.expect_err("expected a parse error");
        let msg = err.to_string();
        for needle in needles {
            assert!(
                msg.contains(needle),
                "Error message missing expected substring \"{needle}\".\nFull message:\n{msg}"
            );
        }
    }

    #[test]
    fn test_unsupported_sh_pattern_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:name ;
    sh:pattern "^[A-Z]"
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "name",
                "sh:hasValue",
                "introspectable false",
                "pattern",
            ],
        );
    }

    #[test]
    fn test_unsupported_sh_class_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:belongsToTunnelComplex ;
    sh:class asset360:TunnelComplex
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "belongsToTunnelComplex",
                "sh:hasValue",
                "sh:class",
                "introspectable false",
            ],
        );
    }

    #[test]
    fn test_unsupported_sh_datatype_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:length ;
    sh:datatype xsd:decimal
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "length",
                "sh:datatype",
                "introspectable false",
            ],
        );
    }

    #[test]
    fn test_required_nonempty_marked_introspectable_is_authoring_error() {
        // "required AND non-empty" = sh:minCount 1 ; sh:minLength 1. minLength is
        // not introspectable, so a shape carrying it must NOT be lowered to a
        // partial AST (which would silently ignore minLength). Marked
        // introspectable=true, that mismatch surfaces as an error.
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:introspectable true ;
  sh:property [
    sh:path asset360:hasName ;
    sh:minCount 1 ;
    sh:minLength 1
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComplex", "");
        assert_error_contains(
            result,
            &[
                "Unsupported value constraint",
                "minLength",
                "hasName",
                "introspectable false",
            ],
        );
    }

    #[test]
    fn test_required_nonempty_non_introspectable_is_delegated() {
        // Same shape marked introspectable=false: parse must succeed but produce
        // NO AST (ast: None), leaving the whole shape to pyshacl — minCount and
        // minLength are then both enforced by the complete engine.
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:introspectable false ;
  sh:property [
    sh:path asset360:hasName ;
    sh:minCount 1 ;
    sh:minLength 1
  ] .
"#;
        let shapes = parse_shacl(ttl, "TunnelComplex", "").expect("should not error");
        assert_eq!(shapes.len(), 1);
        assert!(
            shapes[0].ast.is_none(),
            "a shape with an unsupported constraint must not be introspected"
        );
        assert!(!shapes[0].introspectable);
    }

    /// The canonical standard-SHACL idiom the recognizer must lower.
    const UNIQUE_MEMBER_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TunnelComplex_FileLinksTypedShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:message "msg"@en ;
  sh:property [
    sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "NetMapExcerpt" "RoadMapExcerpt" "NGIMapExcerpt" "Sketch" )
  ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "NetMapExcerpt" ] ;
    sh:qualifiedMaxCount 1 ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "RoadMapExcerpt" ] ;
    sh:qualifiedMaxCount 1 ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "NGIMapExcerpt" ] ;
    sh:qualifiedMaxCount 1 ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "Sketch" ] ;
    sh:qualifiedMaxCount 1 ] .
"#;

    #[test]
    fn test_recognize_unique_by_member_from_standard_shacl() {
        let shapes = parse_shacl(UNIQUE_MEMBER_TTL, "TunnelComplex", "").expect("parse");
        assert_eq!(shapes.len(), 1);
        assert!(shapes[0].introspectable);
        match shapes[0].ast.as_ref().expect("ast") {
            ShaclAst::UniqueByMemberField {
                array_path,
                member_field,
                allowed_values,
                max_count_per_value,
            } => {
                assert_eq!(array_path.local_name(), Some("fileLinksTyped"));
                assert_eq!(member_field.local_name(), Some("type"));
                assert_eq!(*max_count_per_value, 1);
                let allowed = allowed_values.as_ref().expect("sh:in present");
                assert_eq!(allowed.len(), 4);
                assert!(allowed.iter().any(|v| v == "Sketch"));
            }
            other => panic!("expected UniqueByMemberField, got {other:?}"),
        }
        assert!(
            shapes[0]
                .affected_fields
                .contains(&"fileLinksTyped".to_owned())
        );
        assert!(shapes[0].affected_fields.contains(&"type".to_owned()));
    }

    /// A shape whose qualified blocks won't be recognized: marked
    /// Drive `recognize_unique_by_member` directly (it is private to this
    /// module). Builds the shape `asset360:S` from the given property block(s)
    /// and returns whether the recognizer lowered it. This exercises the
    /// recognizer's own `return None` branches — independent of the #20
    /// introspectable-false short-circuit in `parse_shacl`.
    fn recognizes(properties: &str) -> bool {
        let ttl = format!(
            r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:S a sh:NodeShape ; sh:targetClass asset360:TunnelComplex ;
{properties} ."#
        );
        let store = TripleStore::parse(&ttl).expect("parse ttl");
        recognize_unique_by_member(&store, "https://data.infrabel.be/asset360/S").is_some()
    }

    const CANONICAL_PROPS: &str = r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" "B" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 1 ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "B" ] ;
    sh:qualifiedMaxCount 1 ]"#;

    #[test]
    fn test_recognizer_lowers_canonical() {
        assert!(recognizes(CANONICAL_PROPS));
    }

    #[test]
    fn test_not_lowered_without_allowed_set() {
        // No sh:in → no coverage anchor.
        assert!(!recognizes(
            r#"  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 1 ]"#
        ));
    }

    #[test]
    fn test_not_lowered_when_qualified_blocks_undercover_sh_in() {
        // sh:in lists A,B but only A is capped → coverage mismatch.
        assert!(!recognizes(
            r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" "B" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 1 ]"#
        ));
    }

    #[test]
    fn test_not_lowered_with_zero_max_count() {
        assert!(!recognizes(
            r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 0 ]"#
        ));
    }

    #[test]
    fn test_not_lowered_with_inconsistent_max_counts() {
        assert!(!recognizes(
            r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" "B" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 1 ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "B" ] ;
    sh:qualifiedMaxCount 2 ]"#
        ));
    }

    #[test]
    fn test_not_lowered_with_extra_constraint_on_qualified_block() {
        // An extra sh:qualifiedMinCount on a consumed block would be silently
        // dropped if lowered — the recognizer must refuse (fail closed).
        assert!(!recognizes(
            r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ] ;
    sh:qualifiedMaxCount 1 ; sh:qualifiedMinCount 1 ]"#
        ));
    }

    #[test]
    fn test_not_lowered_with_extra_constraint_on_inner_shape() {
        // Extra sh:datatype inside the qualifiedValueShape would be dropped.
        assert!(!recognizes(
            r#"  sh:property [ sh:path ( asset360:fileLinksTyped asset360:type ) ;
    sh:in ( "A" ) ] ;
  sh:property [ sh:path asset360:fileLinksTyped ;
    sh:qualifiedValueShape [ sh:path asset360:type ; sh:hasValue "A" ;
      sh:datatype <http://www.w3.org/2001/XMLSchema#string> ] ;
    sh:qualifiedMaxCount 1 ]"#
        ));
    }

    #[test]
    fn test_introspectable_false_shape_does_not_corrupt_sibling_eval() {
        // End-to-end regression: an introspectable status-combo shape and a
        // non-introspectable sh:in shape on the SAME class. Forward evaluation
        // of valid data must stay clean — before the fix the sh:in shape was
        // force-evaluated (its parseable AST was kept) and flagged valid rows.
        use crate::constraint_set::ConstraintSet;
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:StatusComboShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:introspectable true ;
  sh:not [ sh:or (
    [ sh:and (
      [ sh:property [ sh:path asset360:primary ; sh:hasValue "TSI" ] ]
      [ sh:property [ sh:path asset360:secondary ; sh:hasValue "SST" ] ]
    )]
  )] .

asset360:AllowedTypesShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:introspectable false ;
  sh:property [
    sh:path asset360:docType ;
    sh:in ( "A" "B" )
  ] .
"#;
        let shapes = parse_shacl(ttl, "TunnelComplex", "").expect("parse");
        assert_eq!(shapes.len(), 2);
        // Exactly one introspectable shape survives to the Rust evaluator.
        assert_eq!(shapes.iter().filter(|s| s.introspectable).count(), 1);
        let cs = ConstraintSet::from_json(&serde_json::to_string(&shapes).unwrap()).unwrap();
        // Valid status combo, and a docType OUTSIDE the sh:in set — must NOT be
        // flagged, because the sh:in shape is pyshacl's, not the Rust engine's.
        let data = serde_json::json!({"primary": "TSI", "secondary": "COM", "docType": "Z"});
        assert!(
            cs.evaluate(&data).is_empty(),
            "non-introspectable sh:in shape must not be evaluated by the Rust engine"
        );
    }

    #[test]
    fn test_introspectable_false_with_parseable_body_is_opaque() {
        // Regression: a shape marked `introspectable false` whose body IS in the
        // supported subset (here a plain sh:in) must STILL be left to pyshacl —
        // no AST kept. Previously the parser produced an AST regardless of the
        // annotation, so the forward evaluator / backward solver (which select
        // shapes by `ast.is_some()`) picked it up and corrupted evaluation of
        // the genuinely introspectable shapes on the same class.
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:AllowedTypesShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComplex ;
  asset360:introspectable false ;
  sh:property [
    sh:path asset360:someType ;
    sh:in ( "A" "B" "C" )
  ] .
"#;
        let shapes = parse_shacl(ttl, "TunnelComplex", "").expect("should not error");
        assert_eq!(shapes.len(), 1);
        assert!(!shapes[0].introspectable);
        assert!(
            shapes[0].ast.is_none(),
            "introspectable:false must be authoritative even when the body parses"
        );
        assert!(shapes[0].affected_fields.is_empty());
    }

    #[test]
    fn test_unsupported_alternative_path_error() {
        let ttl = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:introspectable true ;
  sh:property [
    sh:path [ sh:alternativePath ( asset360:name asset360:identification ) ] ;
    sh:minCount 1
  ] .
"#;
        let result = parse_shacl(ttl, "TunnelComponent", "");
        assert_error_contains(
            result,
            &[
                "Unsupported property path",
                "alternativePath",
                "sh:inversePath",
            ],
        );
    }

    // ── Language-tagged message tests ────────────────────────────────

    const MULTILANG_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .

asset360:TestShape
  a sh:NodeShape ;
  sh:targetClass asset360:TunnelComponent ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:message "Forbidden status combination"@en ;
  sh:message "Verboden statuscombinatie"@nl ;
  sh:message "Combinaison de statuts interdite"@fr ;
  sh:not [
    sh:and (
      [ sh:property [ sh:path asset360:ceAssetPrimaryStatus ; sh:hasValue "In_voorbereiding" ] ]
      [ sh:property [ sh:path asset360:ceAssetSecondaryStatus ; sh:hasValue "Verkocht" ] ]
    )
  ] .
"#;

    #[test]
    fn test_language_tagged_message_exact_match() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "nl").unwrap();
        assert_eq!(results[0].message, "Verboden statuscombinatie");
    }

    #[test]
    fn test_language_tagged_message_different_lang() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "fr").unwrap();
        assert_eq!(results[0].message, "Combinaison de statuts interdite");
    }

    #[test]
    fn test_language_tagged_message_fallback_to_first() {
        // No "de" tag available — should fall back to first available
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "de").unwrap();
        // No untagged literal, so picks first available
        assert!(!results[0].message.is_empty());
    }

    #[test]
    fn test_language_tagged_message_empty_lang_picks_any() {
        let results = parse_shacl(MULTILANG_TTL, "TunnelComponent", "").unwrap();
        assert!(!results[0].message.is_empty());
    }

    #[test]
    fn test_untagged_message_still_works() {
        // Original STATUS_COMBO_TTL has an untagged sh:message
        let results = parse_shacl(STATUS_COMBO_TTL, "TunnelComponent", "nl").unwrap();
        assert!(results[0].message.contains("Forbidden"));
    }

    // Prefixed-name BIND and UNION-of-BIND cases are already exercised end-to-end
    // by `test_parse_sparql_shape` via DELEGATE_TTL. These tests cover the cases
    // the previous substring-based parser got wrong.

    #[test]
    fn bind_extracts_full_iri_in_angle_brackets() {
        // The bug the old parser produced "isTunnelDelegate>" for: it didn't
        // strip the surrounding <>.
        let q = r#"
            SELECT $this ?path WHERE {
                $this ?p ?o .
                BIND(<https://data.infrabel.be/asset360/isTunnelDelegate> AS ?path)
            }
        "#;
        assert_eq!(
            extract_bind_fields_from_sparql(q),
            vec!["isTunnelDelegate".to_string()]
        );
    }

    #[test]
    fn bind_multiline_and_case_insensitive() {
        // Old parser was line-scoped and case-sensitive on "BIND(" and " AS".
        let q = r#"
            select $this ?path where {
                $this ?p ?o .
                bind(
                    asset360:isTunnelDelegate
                    as
                    ?path
                )
            }
        "#;
        assert_eq!(
            extract_bind_fields_from_sparql(q),
            vec!["isTunnelDelegate".to_string()]
        );
    }

    #[test]
    fn bind_non_iri_expression_is_skipped() {
        // Variables and function calls aren't NamedNodes — we skip rather than
        // emit garbage like "STR(?x)" or "?x" as a field name.
        let q = r#"
            SELECT $this ?path WHERE {
                $this ?p ?x .
                { BIND(STR(?x) AS ?path) }
                UNION
                { BIND(?x AS ?path) }
            }
        "#;
        assert!(extract_bind_fields_from_sparql(q).is_empty());
    }

    const COVERED_SECTION_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

asset360:CoveredSection_TrackLineConsistencyShape
  a sh:NodeShape ;
  sh:targetClass asset360:CoveredSection ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:property [
    sh:path ( asset360:belongsToTrack asset360:refersToLine ) ;
    sh:equals asset360:belongsToLine ;
  ] .
"#;

    // Same shape but the property carries an unsupported constraint (sh:pattern),
    // so the fail-closed property parser must refuse to lower it.
    const COVERED_SECTION_NEAR_MISS_TTL: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix asset360: <https://data.infrabel.be/asset360/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

asset360:CoveredSection_NearMissShape
  a sh:NodeShape ;
  sh:targetClass asset360:CoveredSection ;
  asset360:enforcementLevel "serious" ;
  asset360:introspectable true ;
  sh:property [
    sh:path ( asset360:belongsToTrack asset360:refersToLine ) ;
    sh:equals asset360:belongsToLine ;
    sh:pattern "^x" ;
  ] .
"#;

    #[test]
    fn test_parse_cross_ref_path_equals() {
        let results = parse_shacl(COVERED_SECTION_TTL, "CoveredSection", "").unwrap();
        assert_eq!(results.len(), 1);
        let shape = &results[0];
        assert!(shape.introspectable);
        assert_eq!(shape.target_class, "CoveredSection");

        match shape.ast.as_ref().unwrap() {
            ShaclAst::PathEquals { path_a, path_b } => {
                match path_a {
                    PropertyPath::Sequence { steps } => {
                        assert_eq!(steps.len(), 2);
                        assert_eq!(steps[0].local_name(), Some("belongsToTrack"));
                        assert_eq!(steps[1].local_name(), Some("refersToLine"));
                    }
                    other => panic!("expected sequence path_a, got {other:?}"),
                }
                assert_eq!(path_b.local_name(), Some("belongsToLine"));
            }
            other => panic!("expected PathEquals, got {other:?}"),
        }

        // The peer slot must be an affected field: it drives solve()'s
        // null-normalization and the consumer's re-solve trigger.
        assert!(shape.affected_fields.contains(&"belongsToLine".to_owned()));
    }

    #[test]
    fn test_parse_cross_ref_near_miss_fails_closed() {
        // An unsupported constraint on the property shape must prevent lowering;
        // because the shape is introspectable:true with no SPARQL, parse_shacl errors.
        let result = parse_shacl(COVERED_SECTION_NEAR_MISS_TTL, "CoveredSection", "");
        assert!(
            result.is_err(),
            "near-miss shape must fail closed, got {result:?}"
        );
    }
}

//! How a stored value becomes an RDF term, decided once per column.
//!
//! The SQL pushdown route reads *text* out of JSONB. To answer in SPARQL it
//! must turn that text into the same RDF term the oxigraph route would produce
//! — a plain literal, a typed literal, a language-tagged literal, or an IRI —
//! or the same question answers differently depending on which route served it.
//!
//! The decision depends only on the slot, never on the row, so it is resolved
//! once per column at plan time and applied per row by the renderer.
//!
//! # Relationship to the turtle writer
//!
//! `linkml_runtime::turtle` makes the same decision when serialising an
//! instance, through private helpers (`literal_and_type`, `is_range_iri`,
//! `scalar_literal_term`, `enum_meaning_iri`). Those are thin wrappers over
//! **public** schema API — `SlotView::get_range_info()` supplies the
//! `rdf_datatype_iri` and the `is_range_iri` flag, and `get_range_enum()`
//! exposes permissible values. What is not public is the *precedence*, which
//! this module restates:
//!
//! 1. an enum value → its concept IRI (`permissible_value_iri`, shared with the
//!    schema graph and the writer, so all three name a value identically);
//! 2. a range that is IRI-ish → a named node;
//! 3. a language tag on the slot (only when there is no datatype, since RDF
//!    allows one or the other) → a language-tagged literal;
//! 4. a custom RDF datatype → a typed literal;
//! 5. otherwise a plain literal.
//!
//! Restating three lines of ordering is the whole duplication, and the
//! differential oracle in consolidator-server pins it: every corpus question
//! runs through both routes and the resulting SPARQL-results JSON must match,
//! term for term. If that ordering ever grows, move this composition upstream
//! and delete the module — it is deliberately one function wide so that swap
//! stays cheap.

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::SlotContainerMode;

/// What kind of RDF term a column's values become.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermKind {
    /// A named node — the range is IRI-ish, or the value is an object's own id.
    Iri,
    /// A literal, possibly typed or language-tagged.
    Literal,
    /// An enum. Every permissible value becomes its concept IRI — the one the
    /// schema graph describes, whether the value declared a `meaning` or had
    /// its IRI minted. A stored value the enum does not permit falls back to a
    /// literal, which is what the turtle writer does.
    EnumIri,
}

/// How to render one column's stored text as an RDF term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermDescriptor {
    pub kind: TermKind,
    /// Datatype IRI for a typed literal. `None` means a plain literal.
    pub datatype: Option<String>,
    /// Language tag. Mutually exclusive with `datatype`, per RDF.
    pub lang: Option<String>,
    /// Stored value → IRI, for `EnumIri`. Empty for every other kind.
    pub enum_map: Vec<(String, String)>,
    /// Whether the values are numbers, so the renderer may cast rather than
    /// compare them as text.
    pub numeric: bool,
}

impl TermDescriptor {
    /// The shape, short enough to sit at the end of a plan line.
    ///
    /// This is where wrong answers come from -- whether a value compares as a
    /// number or as text, whether an enum renders as its code or as an IRI --
    /// and none of it is visible in the query, so a plan that omitted it would
    /// hide the thing a reader most needs.
    pub fn shape(&self) -> String {
        let base = match self.kind {
            TermKind::Iri => "iri".to_owned(),
            TermKind::EnumIri => format!("enum→iri ({} mapped)", self.enum_map.len()),
            TermKind::Literal => match (&self.datatype, &self.lang) {
                (_, Some(lang)) => format!("literal@{lang}"),
                (Some(datatype), None) => short_iri(datatype),
                (None, None) => "literal".to_owned(),
            },
        };
        if self.numeric {
            format!("{base}, numeric")
        } else {
            base
        }
    }

    /// An object's own identifier: always a named node, never a number.
    pub fn subject_iri() -> Self {
        Self {
            kind: TermKind::Iri,
            datatype: None,
            lang: None,
            enum_map: Vec::new(),
            numeric: false,
        }
    }
}

/// Resolve a column: how the value at `slot_path` on `class_uri` renders as an
/// RDF term, and how each step of the path is stored.
///
/// An empty `slot_path` describes the object's own identifier. Returns `None`
/// when the class or a step of the path is unknown to the schema.
///
/// One walk, because there is only one route. The containers come out of the
/// same `SlotView`s the descriptor is derived from, so the two cannot disagree
/// about a path — and no caller has to know that one must run before the other,
/// which is what the previous pair of functions silently required.
///
/// Pull-based on an explicit path on purpose: a schema may be cyclic (a class
/// with a slot of its own range), so there is no finite enumeration of "every
/// path from this class" to precompute. Each call resolves the path it is
/// given, which is always finite because it comes from a query.
pub fn resolve_column(
    schema_view: &SchemaView,
    class_uri: &str,
    slot_path: &[String],
) -> Option<(TermDescriptor, Vec<SlotContainerMode>)> {
    if slot_path.is_empty() {
        return Some((TermDescriptor::subject_iri(), Vec::new()));
    }

    let mut class_view = schema_view.get_class_by_uri(class_uri).ok()??;
    let mut containers = Vec::with_capacity(slot_path.len());

    for (i, slot_name) in slot_path.iter().enumerate() {
        // The class's own name index, rather than a scan of its slots.
        let slot = class_view.slot(&Identifier::Name(slot_name.clone()))?;
        containers.push(slot.determine_slot_container_mode());

        if i + 1 == slot_path.len() {
            return describe_slot(schema_view, &slot).map(|d| (d, containers));
        }

        // An intermediate step must be a class to walk into.
        class_view = slot.get_range_class()?;
    }

    None
}

/// Describe a slot's values, or `None` when they are not a term a consumer can
/// reproduce.
pub fn describe_slot(
    schema_view: &SchemaView,
    slot: &linkml_schemaview::slotview::SlotView,
) -> Option<TermDescriptor> {
    // A slot whose range is a class needs care, and the two cases differ:
    //
    // * a *reference* stores the target's URI, so the stored value is exactly
    //   the IRI oxigraph binds — grouping by it answers "how many per parent",
    //   which is a report people actually want;
    // * an *inlined* structure stores nested JSON, and oxigraph binds a blank
    //   node whose label nothing can reproduce — not SQL, not a second
    //   oxigraph run. Those are traversable (see the path walk in the scoper)
    //   but never a value.
    if slot.get_range_class().is_some() {
        return match slot.determine_slot_inline_mode() {
            linkml_schemaview::slotview::SlotInlineMode::Reference => {
                Some(TermDescriptor::subject_iri())
            }
            _ => None,
        };
    }

    let range_info = slot.get_range_info();
    let info = range_info.first();
    let datatype = info.and_then(|ri| ri.rdf_datatype_iri.clone());
    let is_iri = info.is_some_and(|ri| ri.is_range_iri);

    // Enum values serialise as concept IRIs; the map is finite (the permissible
    // values), unlike the schema graph, so materialising it here is safe. It is
    // empty only when the range is not an enum.
    let enum_map = enum_meanings(schema_view, slot);
    if !enum_map.is_empty() {
        return Some(TermDescriptor {
            kind: TermKind::EnumIri,
            datatype: None,
            lang: None,
            enum_map,
            numeric: false,
        });
    }

    if is_iri {
        return Some(TermDescriptor {
            kind: TermKind::Iri,
            datatype: None,
            lang: None,
            enum_map: Vec::new(),
            numeric: false,
        });
    }

    // A language tag and a datatype are mutually exclusive in RDF, and the
    // turtle writer lets the datatype win — match that, or the two routes
    // would disagree on any slot declaring both.
    let lang = if datatype.is_none() {
        slot.definition().in_language.clone()
    } else {
        None
    };

    // Numeric-ness comes from the schema's own resolution, not from a list of
    // datatype IRIs kept here: `is_integer`/`is_floating_point` prefer the
    // resolved IRI *and* fall back to the builtin LinkML type names, so they
    // still work against a schema whose `linkml:types` is not loaded. A local
    // list missed that fallback, which is why the scoper's test fixture had to
    // declare its own `types:` block.
    //
    // Gap to close upstream: neither helper covers xsd:int / long / short /
    // unsigned*, so a slot declared with one of those is treated as
    // non-numeric here — conservative (SUM/AVG are refused, never miscast),
    // and the fix is a `RangeInfo::is_numeric()` request rather than a second
    // list.
    let numeric = info.is_some_and(|ri| ri.is_integer() || ri.is_floating_point());

    Some(TermDescriptor {
        kind: TermKind::Literal,
        datatype,
        lang,
        enum_map: Vec::new(),
        numeric,
    })
}

/// Permissible value → the IRI that names it, for enum ranges. Empty only when
/// the range is not an enum.
///
/// *Every* value is mapped, not only those carrying a `meaning`: the range of
/// an enum-valued slot is a `skos:ConceptScheme`, so a value nobody mapped to
/// an ontology is still a concept, and rendering it as a bare string made this
/// endpoint answer two kinds of term for one slot. The spelling comes from
/// [`linkml_schemaview::enumview::permissible_value_iri`], which the schema
/// graph and the instance writer also use — pushdown SQL has to reproduce
/// exactly the term the oxigraph leg binds, or the two legs disagree.
fn enum_meanings(
    schema_view: &SchemaView,
    slot: &linkml_schemaview::slotview::SlotView,
) -> Vec<(String, String)> {
    let Some(enum_view) = slot.get_range_enum() else {
        return Vec::new();
    };
    let Some(values) = enum_view.definition().permissible_values.as_ref() else {
        return Vec::new();
    };

    let converter = schema_view.converter();
    let enum_uri = enum_view
        .canonical_uri()
        .to_uri(&converter)
        .map(|u| u.0)
        .unwrap_or_else(|_| enum_view.canonical_uri().to_string());
    let mut out: Vec<(String, String)> = values
        .iter()
        .map(|(text, pv)| {
            let iri =
                linkml_schemaview::enumview::permissible_value_iri(&enum_uri, text, pv, &converter)
                    // An unexpandable `meaning` keeps its raw spelling, as before.
                    .unwrap_or_else(|raw| raw);
            (text.clone(), iri)
        })
        .collect();
    // Deterministic order: the map crosses to Python and ends up in generated
    // SQL, and an unstable order would make queries and tests flap.
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `http://www.w3.org/2001/XMLSchema#integer` → `xsd:integer`, for plan output.
fn short_iri(iri: &str) -> String {
    match iri.rsplit_once(['#', '/']) {
        Some((namespace, local)) if namespace.contains("XMLSchema") => format!("xsd:{local}"),
        Some((_, local)) => local.to_owned(),
        None => iri.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    /// The descriptor half of [`resolve_column`], which is all these cases are
    /// about. The container half has its own assertions in `sparql_pushdown`.
    fn descriptor_of(
        schema_view: &SchemaView,
        class_uri: &str,
        slot_path: &[String],
    ) -> Option<TermDescriptor> {
        resolve_column(schema_view, class_uri, slot_path).map(|(descriptor, _)| descriptor)
    }

    use super::*;
    use linkml_meta::SchemaDefinition;
    use linkml_schemaview::schemaview::SchemaView;

    const SIGNAL: &str = "https://data.infrabel.be/asset360/Signal";

    fn schema() -> SchemaView {
        use serde_path_to_error as p2e;
        use serde_yml as yml;

        // `trackLength` is the case that motivated resolving numeric-ness from
        // the datatype rather than the range's name: a schema-defined type
        // whose `typeof` chain ends at integer is a number, though nothing
        // about the spelling "TrackLength" says so. Since upstream learned to
        // inherit a datatype through `typeof`, it is also the case that proves
        // the chain is followed rather than the range looked up by name.
        let schema_yaml = r#"
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  asset360:
    prefix_reference: https://data.infrabel.be/asset360/
  xsd:
    prefix_reference: http://www.w3.org/2001/XMLSchema#
default_prefix: asset360
default_range: string

types:
  string:
    uri: xsd:string
    base: str
  integer:
    uri: xsd:integer
    base: int
  TrackLength:
    typeof: integer
    base: int
  uriorcurie:
    uri: xsd:anyURI
    base: URIorCURIE
    repr: str

enums:
  SignalStatus:
    permissible_values:
      InService:
        meaning: asset360:InService
      Retired:
        meaning: asset360:Retired
  Uncontrolled:
    permissible_values:
      Yes: {}
      No: {}

classes:
  Location:
    attributes:
      longitude:
        range: integer
  Track:
    class_uri: asset360:Track
    attributes:
      asset360_uri:
        identifier: true
      hasName:
        range: string
  Signal:
    class_uri: asset360:Signal
    attributes:
      asset360_uri:
        identifier: true
      locatedOnTrack:
        range: Track
      name:
        range: string
      length:
        range: integer
      trackLength:
        range: TrackLength
      status:
        range: SignalStatus
      flag:
        range: Uncontrolled
      seeAlso:
        range: uriorcurie
      description:
        range: string
        in_language: en
      location:
        range: Location
        inlined: true
"#;
        let schema: SchemaDefinition =
            p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    fn describe(slot: &str) -> TermDescriptor {
        descriptor_of(&schema(), SIGNAL, &[slot.to_owned()])
            .unwrap_or_else(|| panic!("no descriptor for {slot}"))
    }

    #[test]
    fn plain_string_is_an_untyped_literal() {
        let d = describe("name");
        assert_eq!(d.kind, TermKind::Literal);
        assert_eq!(d.datatype, None);
        assert!(!d.numeric);
    }

    #[test]
    fn declared_numeric_type_carries_its_datatype() {
        let d = describe("length");
        assert_eq!(
            d.datatype.as_deref(),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
        assert!(d.numeric);
    }

    /// A type declared only via `typeof`, with no `uri` of its own, now
    /// inherits its datatype through the `typeof` chain — so `TrackLength`,
    /// which is an `integer` by way of nothing but its `typeof`, is a typed
    /// literal and a number here.
    ///
    /// This test used to assert the opposite, and said so: the datatype was
    /// lost, both routes agreed it was a plain literal, and the fix belonged
    /// upstream in `determine_rdf_type_info` because it changes serialisation.
    /// Upstream made exactly that fix. The assertion is inverted rather than
    /// deleted, because the property being guarded has not changed — the SQL
    /// route and the turtle writer must describe this slot the same way — and
    /// a summable number is the better answer of the two.
    #[test]
    fn typeof_only_type_inherits_its_datatype_and_is_numeric() {
        let d = describe("trackLength");
        assert_eq!(
            d.datatype.as_deref(),
            Some("http://www.w3.org/2001/XMLSchema#integer")
        );
        assert!(d.numeric);
        // The parity that matters: the same literal on both routes.
        assert_eq!(d.kind, TermKind::Literal);
    }

    #[test]
    fn enum_with_meanings_maps_values_to_iris() {
        let d = describe("status");
        assert_eq!(d.kind, TermKind::EnumIri);
        let map: std::collections::HashMap<_, _> = d.enum_map.into_iter().collect();
        assert_eq!(
            map.get("InService").map(String::as_str),
            Some("https://data.infrabel.be/asset360/InService")
        );
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn enum_without_meanings_gets_minted_iris() {
        // Nothing to map to, so gen-owl's `<enum_uri>#<code>` names each value
        // — which is what the schema graph describes and the turtle writer
        // emits, so the descriptor has to agree.
        let d = describe("flag");
        assert_eq!(d.kind, TermKind::EnumIri);
        assert_eq!(
            d.enum_map,
            vec![
                (
                    "No".to_owned(),
                    "https://data.infrabel.be/asset360/Uncontrolled#No".to_owned()
                ),
                (
                    "Yes".to_owned(),
                    "https://data.infrabel.be/asset360/Uncontrolled#Yes".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn iri_ranges_are_named_nodes() {
        assert_eq!(describe("seeAlso").kind, TermKind::Iri);
    }

    #[test]
    fn language_tag_is_carried_when_there_is_no_datatype() {
        let d = describe("description");
        assert_eq!(d.lang.as_deref(), Some("en"));
        assert_eq!(d.datatype, None);
    }

    #[test]
    fn a_reference_is_the_target_iri() {
        // A reference stores the target's URI, which is exactly the term
        // oxigraph binds — so "count per parent" is answerable.
        let d = descriptor_of(&schema(), SIGNAL, &["locatedOnTrack".to_owned()])
            .expect("a reference is a renderable term");
        assert_eq!(d.kind, TermKind::Iri);
    }

    #[test]
    fn an_inlined_structure_is_not_a_term() {
        // Nested JSON serialises as a blank node whose label nothing can
        // reproduce, so there is nothing to render. Refusing here is what makes
        // the analyser reject `GROUP BY ?loc`.
        assert!(descriptor_of(&schema(), SIGNAL, &["location".to_owned()]).is_none());
    }

    #[test]
    fn subject_binding_is_an_iri() {
        let d = descriptor_of(&schema(), SIGNAL, &[]).unwrap();
        assert_eq!(d.kind, TermKind::Iri);
        assert!(!d.numeric);
    }

    #[test]
    fn path_walks_into_an_inline_object() {
        let d = descriptor_of(
            &schema(),
            SIGNAL,
            &["location".to_owned(), "longitude".to_owned()],
        )
        .expect("nested path should resolve");
        assert!(d.numeric);
    }

    #[test]
    fn unknown_class_or_slot_yields_nothing() {
        assert!(descriptor_of(&schema(), SIGNAL, &["nope".to_owned()]).is_none());
        assert!(descriptor_of(&schema(), "urn:not:a:class", &["name".to_owned()]).is_none());
        // A scalar cannot be walked through.
        assert!(
            descriptor_of(&schema(), SIGNAL, &["name".to_owned(), "deeper".to_owned()]).is_none()
        );
    }
}

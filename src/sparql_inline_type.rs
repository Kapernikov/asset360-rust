//! What `?x a :C` asks of an element stored *inline* in a record (#514).
//!
//! An inlined element is not a record: it has no table to scan and no
//! identity of its own, so its `rdf:type` triple is a statement about the
//! structure, decided by the element's *designated class* -- the value of the
//! range class's `designates_type` slot (`BaseLocation.typeURI`), or the
//! range class itself when the slot is absent or the class has none. This
//! module is that decision as a pure function of the schema; the rule that
//! rewrites a plan with it is `sparql_rules::InlineTypeMatchBecomesFilter`.

use linkml_runtime::schema_rdf::{TypeRegime, type_family};
use linkml_schemaview::classview::ClassView;
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;

/// The verdict of `?x a C` for an element of a slot ranging over `R`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InlineTypeTest {
    /// True of every element the slot can hold: the triple adds nothing.
    Discharged,
    /// True of no element the slot can hold.
    NeverMatches,
    /// True of the elements whose designator slot holds one of `values`
    /// (spelled the way the writer stores them), and, when `includes_absent`,
    /// of the elements that carry no designator at all.
    DesignatorIn {
        slot: String,
        values: Vec<String>,
        /// Whether the stored values are IRIs (`uri` range) rather than
        /// text, which decides the term a condition compares against.
        iri_valued: bool,
        includes_absent: bool,
    },
}

/// Decide `?x a c` for an element of a slot whose range is `range`, under
/// `regime`. `None` when the schema cannot answer (a class family or a
/// designator value that does not resolve): the caller keeps the triple.
///
/// **Proof.** Let `D` be the element's designated class: the class its
/// designator names, or `range` when it has none. The writer states exactly
/// `rdf_types(D, regime)` about the element, so `?x a c` holds iff
/// `c ∈ rdf_types(D, regime)`, i.e. `D ∈ type_family(c, regime)` -- the two are
/// inverse by definition (`type_family`). `D` ranges over `U = type_family(range,
/// IsAAndMixins)`, the widest family a slot over `range` may store, so the test
/// is decided by `M = U ∩ type_family(c, regime)`:
///
/// * `M = ∅`: no stored element has `c` among its types -- `NeverMatches`.
/// * `M = U`: every one has -- `Discharged`. (`range` is in `U`, so this
///   covers the element with no designator too.)
/// * otherwise the elements that match are exactly those whose designated class
///   is in `M`: the designator holds the stored value of one of them, or is
///   absent and `range ∈ M`. A class with no designator slot cannot tell its
///   elements apart, so there `D = range` for all of them and the test is
///   `Discharged` or `NeverMatches` by whether `range ∈ M`.
pub(crate) fn inline_type_test(
    schema: &SchemaView,
    range: &ClassView,
    c: &ClassView,
    regime: TypeRegime,
) -> Option<InlineTypeTest> {
    let key = |class: &ClassView| class.canonical_uri().to_string();
    let universe = type_family(range, TypeRegime::IsAAndMixins).ok()?;
    let wanted: std::collections::HashSet<String> =
        type_family(c, regime).ok()?.iter().map(key).collect();
    let matching: Vec<&ClassView> = universe
        .iter()
        .filter(|class| wanted.contains(&key(class)))
        .collect();
    let range_key = key(range);
    let includes_absent = matching.iter().any(|class| key(class) == range_key);

    if matching.is_empty() {
        return Some(InlineTypeTest::NeverMatches);
    }
    let Some(designator) = range.get_type_designator_slot() else {
        return Some(if includes_absent {
            InlineTypeTest::Discharged
        } else {
            InlineTypeTest::NeverMatches
        });
    };
    if matching.len() == universe.len() {
        return Some(InlineTypeTest::Discharged);
    }

    let conv = schema.converter();
    let mut values = Vec::with_capacity(matching.len());
    for class in &matching {
        values.push(
            class
                .get_type_designator_value(designator, &conv)
                .ok()?
                .to_string(),
        );
    }
    values.sort();
    values.dedup();
    let iri_valued = designator.range.as_ref().is_some_and(|range| {
        schema
            .type_ancestors(&Identifier::new(range), &conv)
            .map(|types| types.iter().any(|t| t.to_string() == "uri"))
            .unwrap_or(false)
    });
    Some(InlineTypeTest::DesignatorIn {
        slot: designator.name.clone(),
        values,
        iri_valued,
        includes_absent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::tests::test_schema_view;

    const NS: &str = "https://data.infrabel.be/asset360/";

    fn class(sv: &SchemaView, name: &str) -> ClassView {
        sv.get_class_by_uri(&format!("{NS}{name}"))
            .unwrap()
            .unwrap()
    }

    fn test(range: &str, c: &str, regime: TypeRegime) -> InlineTypeTest {
        let sv = test_schema_view();
        inline_type_test(&sv, &class(&sv, range), &class(&sv, c), regime).unwrap()
    }

    fn designator(values: &[&str], includes_absent: bool) -> InlineTypeTest {
        InlineTypeTest::DesignatorIn {
            slot: "typeURI".to_owned(),
            values: values.iter().map(|v| format!("{NS}{v}")).collect(),
            iri_valued: true,
            includes_absent,
        }
    }

    /// `Place` has the subclasses `SpotPlace`, `RoadPlace`, `RailPlace` and a
    /// designator; `Fix` is unrelated to it. All three regimes, because the
    /// answer is where they differ.
    #[test]
    fn the_verdict_follows_the_regime() {
        use InlineTypeTest::*;
        for regime in TypeRegime::ALL {
            // A subclass of the range: the elements designated by it.
            assert_eq!(
                test("Place", "SpotPlace", regime),
                designator(&["SpotPlace"], false)
            );
            // Unrelated to the range: nothing matches, in every regime.
            assert_eq!(test("Place", "Fix", regime), NeverMatches);
            // A sibling subtree the range does not hold.
            assert_eq!(test("SpotPlace", "RoadPlace", regime), NeverMatches);
        }
        // The range itself: asserted, only the elements that are plain
        // `Place`s (designator absent, or naming `Place`); inferred, all.
        assert_eq!(
            test("Place", "Place", TypeRegime::Asserted),
            designator(&["Place"], true)
        );
        assert_eq!(test("Place", "Place", TypeRegime::IsA), Discharged);
        assert_eq!(test("Place", "Place", TypeRegime::IsAAndMixins), Discharged);
        // A leaf range is its own whole family.
        assert_eq!(
            test("SpotPlace", "SpotPlace", TypeRegime::Asserted),
            Discharged
        );
        // A superclass of the range: asserted, an element is only its own
        // class; inferred, it is the superclass too, always.
        assert_eq!(test("GeoFix", "Fix", TypeRegime::Asserted), NeverMatches);
        assert_eq!(test("GeoFix", "Fix", TypeRegime::IsA), Discharged);
        assert_eq!(test("GeoFix", "Fix", TypeRegime::IsAAndMixins), Discharged);
    }

    /// A class with no designator cannot tell its elements apart: they are all
    /// the range class.
    #[test]
    fn a_range_without_a_designator_is_all_one_class() {
        use InlineTypeTest::*;
        for regime in TypeRegime::ALL {
            assert_eq!(test("Fix", "GeoFix", regime), NeverMatches, "{regime:?}");
            assert_eq!(test("Fix", "Place", regime), NeverMatches, "{regime:?}");
        }
        assert_eq!(test("Fix", "Fix", TypeRegime::Asserted), Discharged);
    }
}

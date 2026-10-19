//! Resolving a path hop's slot over the family of the hop's range class.

/// The classes whose records may stand where a `class` record is
/// expected, itself excluded: its `is_a` and mixin descendants.
///
/// One function, so every walk of a path asks the same question.
// stub: replaced by rust-linkml-core's `type_family(cv, TypeRegime::IsAAndMixins)`
// (minus cv itself) once that WS-U API is frozen.
pub(crate) fn descendants_for_slot_resolution(
    class: &linkml_schemaview::classview::ClassView,
) -> Vec<linkml_schemaview::classview::ClassView> {
    // `get_descendants` guards its recursion with a visited list, so a class
    // that mixes itself in terminates.
    class.get_descendants(true, true).unwrap_or_default()
}

/// The slot `slot_name` as a path hop sees it on `class`: the class's own
/// slot, or else the one definition its descendants agree on.
///
/// A hop's declared range names a *family*: the stored element may be any
/// descendant, and a slot declared only on a subclass is still a hop of the
/// path. Returns `None` when no member carries the slot, or when the members
/// that do disagree (different slot, range, inline or container mode), which
/// is what a path walk did before for every slot it could not find.
///
/// Equivalence argument. Slot resolution reads the schema, not the triples,
/// so it does not depend on the type regime. The stored JSON keys a slot by
/// its name whatever subclass the element is; an element of another subclass
/// simply lacks the key. The consumers of a resolved hop (a presence test, a
/// path read, a fan-out) are already defined on a missing key -- absent,
/// unbound, no row -- which is also what the RDF reading gives: an element
/// without that slot has no such triple. When the descendants agree on one
/// definition, the key means the same thing wherever it is present, so the
/// SQL reading and the triple reading coincide. When they disagree the key
/// would mean different things per element, and the hop is declined. The next
/// hop is walked from the resolved slot's range, never from the subclass.
pub(crate) fn slot_on_class_or_descendant(
    class: &linkml_schemaview::classview::ClassView,
    slot_name: &str,
) -> Option<linkml_schemaview::slotview::SlotView> {
    use linkml_schemaview::identifier::Identifier;
    let id = Identifier::Name(slot_name.to_owned());
    if let Some(own) = class.slot(&id) {
        return Some(own);
    }
    let mut found: Option<linkml_schemaview::slotview::SlotView> = None;
    for descendant in descendants_for_slot_resolution(class) {
        let Some(slot) = descendant.slot(&id) else {
            continue;
        };
        match &found {
            None => found = Some(slot),
            Some(first) if same_hop(first, &slot) => {}
            Some(_) => return None,
        }
    }
    found
}

/// Two slot views that a path walk cannot tell apart: the same slot, ranging
/// over the same class, stored the same way.
fn same_hop(
    a: &linkml_schemaview::slotview::SlotView,
    b: &linkml_schemaview::slotview::SlotView,
) -> bool {
    a.canonical_uri() == b.canonical_uri()
        && a.definition().range == b.definition().range
        && a.get_range_class().map(|c| c.canonical_uri())
            == b.get_range_class().map(|c| c.canonical_uri())
        && a.determine_slot_inline_mode() == b.determine_slot_inline_mode()
        && a.determine_slot_container_mode() == b.determine_slot_container_mode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql_scoper::tests::test_schema_view;

    fn class(name: &str) -> linkml_schemaview::classview::ClassView {
        test_schema_view()
            .get_class_by_uri(&format!("https://data.infrabel.be/asset360/{name}"))
            .unwrap()
            .unwrap()
    }

    /// The family's one definition is found two levels down, the next hop is
    /// the slot's own range, and a slot the family disagrees on, or does not
    /// carry, is declined as before.
    #[test]
    fn a_subclass_only_slot_resolves_through_the_family() {
        let place = class("Place");
        // Own slot first.
        assert_eq!(
            slot_on_class_or_descendant(&place, "label").unwrap().name,
            "label"
        );
        // Subclass-only slot, and its range is what the next hop starts from.
        let coords = slot_on_class_or_descendant(&place, "coords").unwrap();
        let fix = coords.get_range_class().unwrap();
        assert_eq!(fix.name(), "Fix");
        let lat = slot_on_class_or_descendant(&fix, "lat").unwrap();
        assert_eq!(lat.name, "lat");
        // Two descendants, two definitions: no answer.
        assert!(slot_on_class_or_descendant(&place, "tag").is_none());
        // Nobody has it, and the descendants' slots do not leak upwards.
        assert!(slot_on_class_or_descendant(&place, "nope").is_none());
        assert!(slot_on_class_or_descendant(&class("Fix"), "label").is_none());
        // A leaf class has no family beyond itself.
        assert!(slot_on_class_or_descendant(&class("SpotPlace"), "tag").is_none());
    }
}

//! Resolving a path hop's slot over the family of the hop's range class.

/// The classes whose records may stand where a `class` record is
/// expected, itself excluded: its `is_a` and mixin descendants.
///
/// One function, so every walk of a path asks the same question. The family
/// is schema-level: every `is_a` and mixin descendant, recursively. Slot
/// resolution reads the schema, not the triples, so the family does not
/// depend on the type regime.
///
/// `get_descendants` terminates on a class that mixes itself in (a class is
/// recorded once and not recursed into again) and then lists it as its own
/// descendant; the caller asks the class first, so it is dropped here.
///
/// An `Err` means the schema could not be read; the caller propagates it or
/// declines on it, it is never an empty family.
pub(crate) fn descendants_for_slot_resolution(
    class: &linkml_schemaview::classview::ClassView,
) -> Result<
    Vec<linkml_schemaview::classview::ClassView>,
    linkml_schemaview::schemaview::SchemaViewError,
> {
    let own = class.canonical_uri();
    Ok(class
        .get_descendants(true, true)?
        .into_iter()
        .filter(|member| member.canonical_uri() != own)
        .collect())
}

/// The slot `slot_name` as a path hop sees it on `class`: the class's own
/// slot, or else the one definition its descendants agree on.
///
/// A hop's declared range names a *family*: the stored element may be any
/// descendant, and a slot declared only on a subclass is still a hop of the
/// path. Returns `Ok(None)` when no member carries the slot, or when the
/// members that do disagree (different slot, range, inline or container
/// mode), which is what a path walk did before for every slot it could not
/// find. `Err` is a schema that could not be read.
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
) -> Result<
    Option<linkml_schemaview::slotview::SlotView>,
    linkml_schemaview::schemaview::SchemaViewError,
> {
    use linkml_schemaview::identifier::Identifier;
    let id = Identifier::Name(slot_name.to_owned());
    resolve_in_family(class, |member| member.slot(&id))
}

/// The slot a triple's `predicate_iri` names on `class`, resolved as a path
/// hop sees it: the class's own slot, or else the one definition its
/// descendants agree on.
///
/// The twin of [`slot_on_class_or_descendant`] for a caller that holds a
/// *predicate* rather than a JSON key. A path segment is a key and a key is a
/// slot name, so a path walk asks by name. A triple's predicate is an IRI, and
/// two slots of one name may be written under different IRIs (a global slot
/// `foo` with one `slot_uri`, an attribute `foo` on a subclass with another):
/// resolving the predicate to a *name* and asking by name would fold a read of
/// one predicate onto the column of the other. So only a slot whose canonical
/// IRI is the predicate's counts.
///
/// The predicate is read as `sparql_alias` reads it. An IRI that some slot
/// carries as its canonical spelling is that predicate, whatever else it is
/// the native spelling of (the native `prefix:name` of one slot may be the
/// canonical IRI of another, and [`SchemaView::get_slot_by_uri`] may answer
/// with the first). Only an IRI no slot carries is a native spelling, and then
/// it stands for the canonical IRI of the slot it names.
///
/// Equivalence argument. As for [`slot_on_class_or_descendant`], with the
/// question narrowed: a slot that is not written under this predicate can
/// never produce a triple with it, so excluding it can only decline a hop,
/// never widen one.
pub(crate) fn slot_for_predicate_on_class_or_descendant(
    schema: &linkml_schemaview::schemaview::SchemaView,
    class: &linkml_schemaview::classview::ClassView,
    predicate_iri: &str,
) -> Result<
    Option<linkml_schemaview::slotview::SlotView>,
    linkml_schemaview::schemaview::SchemaViewError,
> {
    let conv = schema.converter();
    let canonical = |slot: &linkml_schemaview::slotview::SlotView| -> Option<String> {
        slot.canonical_uri().to_uri(&conv).ok().map(|iri| iri.0)
    };
    let in_family = |wanted: &str| {
        resolve_in_family(class, |member| {
            member
                .slots()
                .iter()
                .find(|slot| canonical(slot).as_deref() == Some(wanted))
                .cloned()
        })
    };
    if let Some(slot) = in_family(predicate_iri)? {
        return Ok(Some(slot));
    }
    // Not carried by the family under that spelling. If any slot of the schema
    // writes it, it is a real predicate the family does not have: declined.
    if schema
        .slot_views()?
        .iter()
        .any(|slot| canonical(slot).as_deref() == Some(predicate_iri))
    {
        return Ok(None);
    }
    // Otherwise it can only be a native spelling.
    let Some(named) = schema.get_slot_by_uri(predicate_iri)? else {
        return Ok(None);
    };
    match canonical(&named) {
        Some(wanted) => in_family(&wanted),
        None => Ok(None),
    }
}

/// The one definition of a slot over a hop's family: `find` asked of the
/// class itself first, then of its descendants, which must agree.
fn resolve_in_family(
    class: &linkml_schemaview::classview::ClassView,
    find: impl Fn(
        &linkml_schemaview::classview::ClassView,
    ) -> Option<linkml_schemaview::slotview::SlotView>,
) -> Result<
    Option<linkml_schemaview::slotview::SlotView>,
    linkml_schemaview::schemaview::SchemaViewError,
> {
    if let Some(own) = find(class) {
        return Ok(Some(own));
    }
    let mut found: Option<linkml_schemaview::slotview::SlotView> = None;
    for descendant in descendants_for_slot_resolution(class)? {
        let Some(slot) = find(&descendant) else {
            continue;
        };
        match &found {
            None => found = Some(slot),
            Some(first) if same_hop(first, &slot) => {}
            Some(_) => return Ok(None),
        }
    }
    Ok(found)
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
            slot_on_class_or_descendant(&place, "label")
                .unwrap()
                .unwrap()
                .name,
            "label"
        );
        // Subclass-only slot, and its range is what the next hop starts from.
        let coords = slot_on_class_or_descendant(&place, "coords")
            .unwrap()
            .unwrap();
        let fix = coords.get_range_class().unwrap();
        assert_eq!(fix.name(), "Fix");
        let lat = slot_on_class_or_descendant(&fix, "lat").unwrap().unwrap();
        assert_eq!(lat.name, "lat");
        // Two descendants, two definitions: no answer.
        assert!(
            slot_on_class_or_descendant(&place, "tag")
                .unwrap()
                .is_none()
        );
        // Nobody has it, and the descendants' slots do not leak upwards.
        assert!(
            slot_on_class_or_descendant(&place, "nope")
                .unwrap()
                .is_none()
        );
        assert!(
            slot_on_class_or_descendant(&class("Fix"), "label")
                .unwrap()
                .is_none()
        );
        // A leaf class has no family beyond itself.
        assert!(
            slot_on_class_or_descendant(&class("SpotPlace"), "tag")
                .unwrap()
                .is_none()
        );
    }

    /// A slot is asked for by predicate, not by name: a descendant's attribute
    /// of the same name under another `slot_uri` is another predicate and is
    /// not the slot a triple names (review probe P4).
    #[test]
    fn a_same_named_attribute_under_another_slot_uri_is_not_the_predicate_s_slot() {
        use crate::sparql_scoper::tests::test_schema_view_plus;
        let schema = test_schema_view_plus(
            r#"
  Anchor:
    class_uri: asset360:Anchor
    mixins: [Anchor]
    attributes:
      asset360_uri:
        identifier: true
  AnchorKid:
    class_uri: asset360:AnchorKid
    is_a: Anchor
    attributes:
      name:
        slot_uri: asset360:kidName
        range: string
      length:
        range: integer
"#,
        );
        let anchor = schema
            .get_class_by_uri("https://data.infrabel.be/asset360/Anchor")
            .unwrap()
            .unwrap();
        let iri = |local: &str| format!("https://data.infrabel.be/asset360/{local}");
        // `Anchor` mixes itself in: the family walk terminates and the class
        // is not its own family member.
        let family: Vec<String> = descendants_for_slot_resolution(&anchor)
            .unwrap()
            .iter()
            .map(|member| member.name().to_owned())
            .collect();
        assert_eq!(family, ["AnchorKid"]);
        // By name the descendant's `name` is "the" slot: the walk of a JSON key.
        assert!(
            slot_on_class_or_descendant(&anchor, "name")
                .unwrap()
                .is_some()
        );
        // By predicate it is not: `asset360:name` is written by other slots.
        assert!(
            slot_for_predicate_on_class_or_descendant(&schema, &anchor, &iri("name"))
                .unwrap()
                .is_none()
        );
        // Its own predicate finds it.
        let own = slot_for_predicate_on_class_or_descendant(&schema, &anchor, &iri("kidName"))
            .unwrap()
            .unwrap();
        assert_eq!(own.name, "name");
    }
}

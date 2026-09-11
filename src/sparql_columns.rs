//! A registry of schema slots that a downstream repo has broken out into an
//! indexed physical column.
//!
//! # What this answers, and what it deliberately does not
//!
//! [`broken_out_column`] answers exactly one question: does this
//! `(class_uri, slot_path)` have an indexed physical column behind it?
//! It does **not** answer whether a *matching row's* column was actually
//! derived from this slot. `asset.yaml` fills `geometry_point` from
//! `latitude`/`longitude` when `hasGeometry/asWKT` is absent, so a
//! lat/lon-only record has a populated `geometry_point` and no `asWKT`
//! triple at all. A filter that lifts on the strength of this registry alone
//! would therefore match rows whose column came from a different slot.
//! That correctness gap is closed later, in SQL, by requiring the WKT path
//! itself to be non-NULL before trusting the column -- not here. Do not
//! treat a `Some` from this function as "this row's column came from this
//! slot"; treat it only as "this slot has a column to consult".
//!
//! # No new SPARQL vocabulary
//!
//! Geometry filters need no new predicates. The geometry configs locate a
//! record's geometry through real schema slots -- GeoSPARQL's own shape,
//! `hasGeometry/asWKT`, or the scalar pair `latitude`/`longitude` -- and
//! `set_geometry_field_on_django_depending_on_type` writes the result into
//! `geometry_point`/`geometry_linestring`/`geometry_polygon` on the Django
//! side. Those PostGIS columns are therefore a derived index over schema
//! data, the same relationship a JSONB expression index has to a scalar
//! slot, not a second source of truth. So the pushdown does not need new
//! predicates; it needs to notice that a filter targets a slot which has
//! such an index behind it.
//!
//! # A column *family*, not a column
//!
//! [`BrokenOutColumn::Geometry`] names a family because which of the three
//! Django columns is populated depends on the geometry's own type -- a fact
//! `GeoLocationCriterium`, on the Python side, already owns (it ORs the
//! lookup across all three). Naming `geometry_point`/`geometry_linestring`/
//! `geometry_polygon` here would copy a physical schema from the
//! `consolidator-server` repo into this one. `as_str()` therefore returns
//! `"geometry"`, and the Python side maps that name to its three columns
//! itself.
//!
//! # Keyed on the path's tail, not on a class list
//!
//! `postalcode.yaml` and `municipality.yaml` spell the geometry path as
//! `[hasGeometry, asWKT]`; `asset.yaml` spells the same shape one hop down,
//! as `[refersToLocatedNetEntity, hasGeometry, asWKT]`. Matching the tail of
//! the path covers both without needing to know which classes carry the
//! slot. A class list would go stale the moment a config changed, and there
//! is no way for Rust to read those YAML configs anyway -- they live in the
//! other repo. `class_uri` is accepted, for future use, and today only
//! checked to be non-empty; the real correctness condition is not "which
//! class" but "did this row's column come from this slot", and that is
//! settled in SQL (see above), not here.
//!
//! # Why `latitude`/`longitude` is not an entry
//!
//! The spec lists a `latitude` + `longitude` entry alongside the GeoSPARQL
//! shape. It is deliberately not added. There is no SPARQL predicate that
//! spells a geometry test over two independent scalar slots, so nothing
//! could ever look this entry up -- it would be dead code. Worse, it would
//! be an invitation: a `geof:` filter (which names a *geometry* predicate)
//! against a lat/lon-only class's `hasGeometry/asWKT` path must **not**
//! lift, because that class's data has no such slot and the engine leg
//! correctly answers nothing for it. An entry keyed on `[latitude]` or
//! `[longitude]` would not by itself cause that wrong lift, but its presence
//! invites a future reader to "complete" the registry by wiring it into a
//! geometry predicate lift, which would.

/// The column family a slot's data has been broken out into, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokenOutColumn {
    Geometry,
}

impl BrokenOutColumn {
    /// The family name Python maps to its concrete column(s).
    pub fn as_str(self) -> &'static str {
        match self {
            BrokenOutColumn::Geometry => "geometry",
        }
    }
}

/// The GeoSPARQL shape's path tail, in every spelling the configs use.
const GEOMETRY_TAIL: [&str; 2] = ["hasGeometry", "asWKT"];

/// Does `(class_uri, slot_path)` have an indexed physical column behind it?
///
/// See the module doc for what this does and does not mean. In short: a
/// `Some` says a column exists to consult, not that any particular row's
/// column was populated from this slot -- that is a SQL-time concern.
pub fn broken_out_column(class_uri: &str, slot_path: &[String]) -> Option<BrokenOutColumn> {
    if class_uri.is_empty() {
        return None;
    }
    let tail_len = GEOMETRY_TAIL.len();
    if slot_path.len() < tail_len {
        return None;
    }
    let tail = &slot_path[slot_path.len() - tail_len..];
    if tail.iter().eq(GEOMETRY_TAIL.iter()) {
        Some(BrokenOutColumn::Geometry)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_owned()).collect()
    }

    const POSTAL: &str = "https://data.infrabel.be/asset360/PostalCode";

    #[test]
    fn the_geosparql_shape_has_a_broken_out_column() {
        assert_eq!(
            broken_out_column(POSTAL, &path(&["hasGeometry", "asWKT"])),
            Some(BrokenOutColumn::Geometry),
        );
        // The same shape one hop down, which is how `asset.yaml` spells it.
        assert_eq!(
            broken_out_column(
                "https://data.infrabel.be/asset360/Asset",
                &path(&["refersToLocatedNetEntity", "hasGeometry", "asWKT"]),
            ),
            Some(BrokenOutColumn::Geometry),
        );
    }

    #[test]
    fn nothing_else_has_one() {
        assert_eq!(broken_out_column(POSTAL, &path(&["hasGeometry"])), None);
        assert_eq!(broken_out_column(POSTAL, &path(&["asWKT"])), None);
        assert_eq!(broken_out_column(POSTAL, &path(&["hasName"])), None);
        // Latitude and longitude populate `geometry_point`, and are
        // deliberately absent: no geometry predicate can name two scalar
        // slots, so an entry would be dead code that invites a wrong lift.
        assert_eq!(broken_out_column(POSTAL, &path(&["latitude"])), None);
        assert_eq!(
            broken_out_column("", &path(&["hasGeometry", "asWKT"])),
            None
        );
    }

    /// A tail match is not a substring match: a slot name that merely
    /// contains "hasGeometry" or "asWKT" as a piece of a longer identifier
    /// must not be mistaken for the real slot. A naive `ends_with` on a
    /// joined string (e.g. `"...hasGeometryFoo/asWKT"`) would get this wrong.
    #[test]
    fn path_boundaries_are_respected_not_substrings() {
        assert_eq!(
            broken_out_column(POSTAL, &path(&["hasGeometryFoo", "asWKT"])),
            None,
        );
        assert_eq!(
            broken_out_column(POSTAL, &path(&["hasGeometry", "asWKTLike"])),
            None,
        );
        assert_eq!(
            broken_out_column(POSTAL, &path(&["fooHasGeometry", "asWKT"])),
            None,
        );
    }

    /// A path that carries the tail as an infix, not a suffix, must not
    /// match either -- the registry is keyed on the *tail* of the path
    /// (the last two segments), not on whether the pair occurs anywhere in
    /// it.
    #[test]
    fn the_pair_must_be_the_tail_not_merely_present() {
        assert_eq!(
            broken_out_column(POSTAL, &path(&["hasGeometry", "asWKT", "extra"])),
            None,
        );
    }

    /// The non-empty `class_uri` gate in isolation: an unrelated but
    /// non-empty `class_uri` still matches (the gate today is only
    /// "non-empty", not "which class"), while an empty string fails it.
    #[test]
    fn class_uri_gate_is_non_empty_only() {
        assert_eq!(
            broken_out_column(
                "https://data.infrabel.be/asset360/SomeUnrelatedClass",
                &path(&["hasGeometry", "asWKT"]),
            ),
            Some(BrokenOutColumn::Geometry),
        );
        assert_eq!(
            broken_out_column("", &path(&["hasGeometry", "asWKT"])),
            None
        );
    }

    #[test]
    fn as_str_names_the_column_family() {
        assert_eq!(BrokenOutColumn::Geometry.as_str(), "geometry");
    }

    /// A path shorter than the tail must return `None`, not panic. This
    /// pins the length gate directly: a `slot_path.len() - tail_len` done
    /// without first checking `slot_path.len() < tail_len` underflows (a
    /// `usize` subtraction) and panics for any path shorter than the tail,
    /// including the empty path a caller could pass for a bare identifier
    /// filter with no path at all.
    #[test]
    fn shorter_than_tail_does_not_panic() {
        assert_eq!(broken_out_column(POSTAL, &path(&[])), None);
        assert_eq!(broken_out_column(POSTAL, &path(&["hasGeometry"])), None);
    }
}

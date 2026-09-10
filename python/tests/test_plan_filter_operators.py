"""Every arm the planner can push across the PyO3 boundary has a name here.

A newer planner meeting an older renderer is the hazard the whole split
creates; the operator names (and the filter-tree shape) are the only thing a
renderer can dispatch on. These tests pin the vocabulary and the `FilterNode`
shape against the actual PyO3 surface, not against what a docstring claims.
"""

from __future__ import annotations

from asset360_rust import FilterCondition, PlanOp, SchemaView
from asset360_rust import plan_query_refined as _plan_query_refined

lr = __import__("asset360_rust")

PREFIX = (
    "PREFIX asset360: <https://data.infrabel.be/asset360/> "
    "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
    "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
)

# Mirrors `sparql_scoper::tests::test_schema_view` in the Rust crate (see
# `src/sparql_scoper.rs`) so the two sides plan the identical fixture.
_SCHEMA_YAML = """
id: https://data.infrabel.be/asset360
name: asset360
prefixes:
  asset360:
    prefix_reference: https://data.infrabel.be/asset360/
  linkml:
    prefix_reference: https://w3id.org/linkml/
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
classes:
  Signal:
    class_uri: asset360:Signal
    attributes:
      asset360_uri:
        identifier: true
      name:
        range: string
      length:
        range: integer
  PostalCode:
    class_uri: asset360:PostalCode
    attributes:
      asset360_uri:
        identifier: true
      hasGeometry:
        range: Geometry
        inlined: true
  Geometry:
    class_uri: asset360:Geometry
    attributes:
      asWKT:
        range: string
"""


def schema() -> SchemaView:
    sv = SchemaView()
    sv.add_schema_str(_SCHEMA_YAML)
    return sv


def _sql_filter_ops(plan) -> list[PlanOp]:
    return [
        op
        for p in plan.passes
        if p.kind == "sql"
        for op in p.ops
        if op.kind == "filter"
    ]


def _sql_filter_tree_ops(plan) -> list[PlanOp]:
    return [
        op
        for p in plan.passes
        if p.kind == "sql"
        for op in p.ops
        if op.kind == "filter_tree"
    ]


def test_the_new_operators_cross_the_boundary():
    """Every arm the planner can now push has a name on this side."""
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . '
        'FILTER(CONTAINS(LCASE(?nm), "bx")) }',
        schema(),
    )
    operators = [op.condition.operator for op in _sql_filter_ops(plan)]
    assert operators == ["icontains"]


def test_not_equal_crosses_as_ne():
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . '
        'FILTER(?nm != "BX517") }',
        schema(),
    )
    operators = [op.condition.operator for op in _sql_filter_ops(plan)]
    assert operators == ["ne"]


def test_unbound_crosses_as_not_bound():
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal . '
        'OPTIONAL { ?s asset360:name ?nm } FILTER(!bound(?nm)) }',
        schema(),
    )
    operators = [op.condition.operator for op in _sql_filter_ops(plan)]
    assert operators == ["not_bound"]


def test_sf_intersects_crosses_as_intersects():
    box_wkt = "POLYGON((4 50, 5 50, 5 51, 4 51, 4 50))"
    plan = _plan_query_refined(
        PREFIX
        + "SELECT ?s WHERE { ?s a asset360:PostalCode ; asset360:hasGeometry ?g . "
        "?g asset360:asWKT ?w . "
        f'FILTER(geof:sfIntersects(?w, "{box_wkt}"^^geo:wktLiteral)) }}',
        schema(),
    )
    operators = [op.condition.operator for op in _sql_filter_ops(plan)]
    assert operators == ["intersects"]


def test_filter_tree_exposes_an_any_root_with_two_leaf_children():
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm ; '
        'asset360:length ?len . FILTER(?nm = "BX517" || ?len > 10) }',
        schema(),
    )
    trees = _sql_filter_tree_ops(plan)
    assert len(trees) == 1
    root = trees[0].filter_tree
    assert root is not None
    assert root.kind == "any"
    assert len(root.children) == 2
    assert all(child.kind == "leaf" for child in root.children)
    slot_paths = sorted(".".join(child.slot_path) for child in root.children)
    assert slot_paths == ["length", "name"]
    for child in root.children:
        assert child.condition is not None
        assert child.reading in ("column", "any_element", "bound_element")
        assert isinstance(child.numeric, bool)


def test_filter_tree_all_kind_appears_for_a_nested_conjunction():
    """`(A && B) || C` nests an `All` inside the `Any` -- pins the ``"all"``
    branch of `FilterNode.kind`, which nothing else here exercises."""
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm ; '
        'asset360:length ?len . '
        'FILTER((?nm = "BX517" && ?len > 3) || ?len > 10) }',
        schema(),
    )
    trees = _sql_filter_tree_ops(plan)
    assert len(trees) == 1
    root = trees[0].filter_tree
    assert root is not None
    assert root.kind == "any"
    kinds = sorted(child.kind for child in root.children)
    assert kinds == ["all", "leaf"]
    all_node = next(child for child in root.children if child.kind == "all")
    assert len(all_node.children) == 2
    assert all(child.kind == "leaf" for child in all_node.children)


def test_a_plan_op_that_is_not_filter_tree_has_no_tree():
    plan = _plan_query_refined(
        PREFIX + 'SELECT ?s WHERE { ?s a asset360:Signal }',
        schema(),
    )
    scans = [op for p in plan.passes if p.kind == "sql" for op in p.ops if op.kind == "scan"]
    assert scans
    assert scans[0].filter_tree is None


def test_not_bound_condition_has_no_values_and_repr_does_not_raise():
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal . '
        'OPTIONAL { ?s asset360:name ?nm } FILTER(!bound(?nm)) }',
        schema(),
    )
    conditions = [op.condition for op in _sql_filter_ops(plan)]
    assert len(conditions) == 1
    cond = conditions[0]
    assert cond is not None
    assert cond.values == []
    try:
        cond.value
    except IndexError:
        pass
    else:
        raise AssertionError("value must raise IndexError on an empty condition")
    assert repr(cond) == "FilterCondition(not_bound)"


def test_broken_out_column_module_function_finds_the_geometry_family():
    assert lr.broken_out_column(
        "https://data.infrabel.be/asset360/PostalCode", ["hasGeometry", "asWKT"]
    ) == "geometry"
    assert (
        lr.broken_out_column("https://data.infrabel.be/asset360/Signal", ["name"])
        is None
    )


def test_plan_op_broken_out_column_on_the_geometry_leaf():
    box_wkt = "POLYGON((4 50, 5 50, 5 51, 4 51, 4 50))"
    plan = _plan_query_refined(
        PREFIX
        + "SELECT ?s WHERE { ?s a asset360:PostalCode ; asset360:hasGeometry ?g . "
        "?g asset360:asWKT ?w . "
        f'FILTER(geof:sfIntersects(?w, "{box_wkt}"^^geo:wktLiteral)) }}',
        schema(),
    )
    ops = _sql_filter_ops(plan)
    assert len(ops) == 1
    assert ops[0].broken_out_column == "geometry"


def test_plan_op_broken_out_column_is_none_for_an_ordinary_slot():
    plan = _plan_query_refined(
        PREFIX
        + 'SELECT ?s WHERE { ?s a asset360:Signal ; asset360:name ?nm . '
        'FILTER(?nm = "BX517") }',
        schema(),
    )
    ops = _sql_filter_ops(plan)
    assert len(ops) == 1
    assert ops[0].broken_out_column is None

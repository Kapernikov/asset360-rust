"""Every arm the planner can push across the PyO3 boundary has a name here.

A newer planner meeting an older renderer is the hazard the whole split
creates; the operator names (and the filter-tree shape) are the only thing a
renderer can dispatch on. These tests pin the vocabulary and the `FilterNode`
shape against the actual PyO3 surface, not against what a docstring claims.
"""

from __future__ import annotations

from asset360_rust import DatasetSpec, FilterCondition, PlanOp, SchemaView, TypeRegime
from asset360_rust import plan_query_refined as _plan_with_dataset

# `dataset` is required by the binding (no call can forget it); these tests plan
# against the endpoint every dataset was before graphs had regimes.
_DATASET = DatasetSpec(TypeRegime.ASSERTED)


def _plan_query_refined(query, schema_view, **options):
    return _plan_with_dataset(query, schema_view, _DATASET, **options)

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


def test_the_dataset_is_required_and_validated():
    """No call can forget the dataset, and one that contradicts itself is refused."""
    import pytest

    query = PREFIX + "SELECT ?s WHERE { ?s a asset360:Signal }"
    with pytest.raises(TypeError):
        _plan_with_dataset(query, schema())
    with pytest.raises(TypeError):
        DatasetSpec("urn:graph:nowhere")
    with pytest.raises(ValueError, match="both a data graph and a schema graph"):
        DatasetSpec(
            TypeRegime.ASSERTED,
            data_graphs={"urn:g": TypeRegime.IS_A},
            schema_graphs={"urn:g": TypeRegime.IS_A},
        )
    served = DatasetSpec(
        TypeRegime.ASSERTED,
        data_graphs={"urn:graph:inferred": TypeRegime.IS_A},
    )
    plan = _plan_with_dataset(
        PREFIX
        + "SELECT ?s WHERE { GRAPH <urn:graph:inferred> { ?s a asset360:Signal } }",
        schema(),
        served,
    )
    assert plan.is_accounted


_PARENT_YAML = _SCHEMA_YAML.replace(
    "classes:\n  Signal:\n    class_uri: asset360:Signal\n",
    "classes:\n  Asset:\n    class_uri: asset360:Asset\n    attributes:\n      asset360_uri:\n"
    "        identifier: true\n  Signal:\n    is_a: Asset\n    class_uri: asset360:Signal\n",
)
_SESAME = "PREFIX sesame: <http://www.openrdf.org/schema/sesame#> "


def _is_a_scan(query):
    sv = SchemaView()
    sv.add_schema_str(_PARENT_YAML)
    plan = _plan_with_dataset(PREFIX + _SESAME + query, sv, DatasetSpec(TypeRegime.IS_A))
    scans = [
        op for p in plan.passes if p.kind == "sql" for op in p.ops if op.kind == "scan"
    ]
    assert len(scans) == 1
    return plan, scans[0]


def test_a_direct_type_is_a_column_of_the_scan():
    """`sesame:directType ?t` is the scan's `asset_type`, bound as `?t`; a constant
    one narrows `asset_types` (what the renderer filters `asset_type` by)."""
    _, scan = _is_a_scan(
        "SELECT ?s ?t WHERE { ?s a asset360:Asset ; sesame:directType ?t }"
    )
    assert scan.direct_type == "t"
    assert len(scan.asset_types) == 2

    _, scan = _is_a_scan(
        "SELECT ?s WHERE { ?s a asset360:Asset ; sesame:directType asset360:Signal }"
    )
    assert scan.direct_type is None
    assert [c.rsplit("/", 1)[1] for c in scan.asset_types] == ["Signal"]

    plan, scan = _is_a_scan(
        "SELECT ?t (COUNT(?s) AS ?n) WHERE { ?s a asset360:Signal ; a ?t } GROUP BY ?t"
    )
    assert scan.direct_type is not None
    joins = [
        op for p in plan.passes if p.kind == "sql" for op in p.ops if op.kind == "join"
    ]
    assert any(
        column.direct_type
        for join in joins
        for column in (join.join_key_left, join.join_key_right)
        if column is not None
    )

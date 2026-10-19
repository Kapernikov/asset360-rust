"""The SPARQL endpoint's Python surface: `DatasetSpec` and the argument order
of the entry points (`(query, schema_view, dataset, *, limits)`)."""

from __future__ import annotations

import inspect

import pytest
from asset360_rust import (
    DatasetSpec,
    Finishing,
    SchemaView,
    TypePartition,
    TypeRegime,
    plan_query_refined,
    sparql_execute,
    sparql_execute_plan,
    sparql_finish,
    sparql_schema_graph_ntriples,
    sparql_schema_graph_skipped,
    sparql_scope,
)

A = "urn:graph:asserted"
S = "urn:graph:schema"


def test_a_dataset_that_contradicts_itself_raises_with_every_conflict():
    with pytest.raises(ValueError) as raised:
        DatasetSpec(
            TypeRegime.parse("{}"),
            data_graphs={A: TypeRegime.ASSERTED},
            schema_graphs={A: TypeRegime.IS_A, S: TypeRegime.IS_A},
        )
    message = str(raised.value)
    # Both mistakes are named, not the first one met.
    assert "default regime" in message and "lacks the own partition" in message
    assert A in message and "a graph has one meaning" in message


def test_a_coherent_dataset_builds():
    DatasetSpec(
        TypeRegime.ASSERTED,
        data_graphs={A: TypeRegime.ASSERTED},
        schema_graphs={S: TypeRegime.IS_A},
    )


def test_a_graph_of_records_needs_the_own_partition():
    only_is_a = TypeRegime([TypePartition.IS_A])
    with pytest.raises(ValueError, match="lacks the own partition"):
        DatasetSpec(TypeRegime.ASSERTED, data_graphs={A: only_is_a})
    with pytest.raises(ValueError, match="lacks the own partition"):
        DatasetSpec(TypeRegime.parse("{}"))


def test_limits_are_keyword_only_with_their_defaults():
    execute = inspect.signature(sparql_execute)
    assert list(execute.parameters)[:4] == ["query", "instances", "schema_view", "dataset"]
    finish = inspect.signature(sparql_finish)
    assert list(finish.parameters)[:3] == ["plan", "solutions_json", "schema_view"]
    assert "dataset" not in finish.parameters
    for signature in (execute, finish):
        for name in ("max_triples", "max_result_rows", "max_eval_millis"):
            assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY


_SCHEMA_YAML = """
id: https://example.org/s
name: s
prefixes:
  ex: https://example.org/s/
  linkml: https://w3id.org/linkml/
default_prefix: ex
default_range: string
classes:
  Asset:
    class_uri: ex:Asset
  Tunnel:
    is_a: Asset
    class_uri: ex:Tunnel
"""


def test_the_schema_graph_is_asked_for_by_regime_alone():
    sv = SchemaView()
    sv.add_schema_str(_SCHEMA_YAML)
    subclass_of = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
    asserted = sparql_schema_graph_ntriples(sv, TypeRegime.ASSERTED)
    assert "<https://example.org/s/Tunnel> " + subclass_of + " <https://example.org/s/Asset>" in asserted
    assert asserted != sparql_schema_graph_ntriples(sv, TypeRegime.IS_A)
    assert sparql_schema_graph_skipped(sv, TypeRegime.ASSERTED) == []


def test_a_dataset_can_be_read_back():
    dataset = DatasetSpec(
        TypeRegime.IS_A,
        data_graphs={A: TypeRegime.IS_A},
        schema_graphs={S: TypeRegime.IS_A_AND_MIXINS},
    )
    assert dataset.default_regime == TypeRegime.IS_A
    assert dataset.data_graphs == {A: TypeRegime.IS_A}
    assert dataset.schema_graphs == {S: TypeRegime.IS_A_AND_MIXINS}
    assert repr(dataset) == (
        "DatasetSpec(TypeRegime.IS_A, data_graphs={'urn:graph:asserted': TypeRegime.IS_A}"
        ", schema_graphs={'urn:graph:schema': TypeRegime.IS_A_AND_MIXINS})"
    )


def test_a_dataset_repr_round_trips_and_hashes_with_equality():
    own_and_isa = TypeRegime([TypePartition.OWN, TypePartition.IS_A])
    for dataset in (
        DatasetSpec(TypeRegime.ASSERTED),
        DatasetSpec(TypeRegime.IS_A, data_graphs={A: own_and_isa}),
        DatasetSpec(TypeRegime.IS_A, data_graphs={A: own_and_isa}, schema_graphs={S: TypeRegime.IS_A}),
        DatasetSpec(TypeRegime.ASSERTED, data_graphs={"urn:it's": TypeRegime.ASSERTED}),
    ):
        again = eval(repr(dataset))  # noqa: S307 -- the repr is the thing under test
        assert again == dataset
        assert hash(again) == hash(dataset)
    assert hash(DatasetSpec(TypeRegime.ASSERTED)) != hash(DatasetSpec(TypeRegime.IS_A))
    # A served graph is part of the dataset's identity.
    assert DatasetSpec(TypeRegime.ASSERTED) != DatasetSpec(
        TypeRegime.ASSERTED, data_graphs={A: TypeRegime.ASSERTED}
    )


def test_a_regime_that_is_no_named_one_reads_back_as_itself():
    own_and_isa = TypeRegime([TypePartition.OWN, TypePartition.IS_A])
    dataset = DatasetSpec(TypeRegime.ASSERTED, data_graphs={A: own_and_isa})
    assert dataset.data_graphs == {A: own_and_isa}
    assert repr(dataset) == (
        "DatasetSpec(TypeRegime.ASSERTED, "
        "data_graphs={'urn:graph:asserted': TypeRegime.parse('{own,is_a}')})"
    )


def test_a_bare_dataset_reads_back_empty():
    dataset = DatasetSpec(TypeRegime.ASSERTED)
    assert dataset.default_regime == TypeRegime.ASSERTED
    assert dataset.data_graphs == {} and dataset.schema_graphs == {}
    assert repr(dataset) == "DatasetSpec(TypeRegime.ASSERTED)"


def test_a_query_reading_two_data_graphs_is_refused_by_name_on_every_route():
    sv = SchemaView()
    sv.add_schema_str(_SCHEMA_YAML)
    inferred = "urn:graph:inferred"
    dataset = DatasetSpec(
        TypeRegime.ASSERTED,
        data_graphs={A: TypeRegime.ASSERTED, inferred: TypeRegime.IS_A},
    )
    query = f"SELECT ?s FROM <{A}> FROM <{inferred}> WHERE {{ ?s a <https://example.org/s/Asset> }}"
    refusal = "cross_data_graph: a query can read one data graph; rewrite it per graph"
    with pytest.raises(ValueError, match=refusal):
        plan_query_refined(query, sv, dataset)
    with pytest.raises(ValueError, match=refusal):
        sparql_scope(query, sv, dataset)
    with pytest.raises(RuntimeError, match=refusal):
        sparql_execute(query, [], sv, dataset)
    # One graph is a query like any other.
    one = f"SELECT ?s FROM <{inferred}> WHERE {{ ?s a <https://example.org/s/Asset> }}"
    plan = plan_query_refined(one, sv, dataset)
    assert plan.is_accounted
    records_only = plan_query_refined(one, sv, dataset, finishing=Finishing.RECORDS_ONLY)
    assert records_only.is_accounted and records_only.refinement != "used_rows"
    # The default is `Finishing.ROWS`.
    rows = plan_query_refined(one, sv, dataset, finishing=Finishing.ROWS)
    assert plan.refinement == rows.refinement


def test_the_finishing_switch_is_keyword_only():
    signature = inspect.signature(plan_query_refined)
    assert list(signature.parameters)[:3] == ["query", "schema_view", "dataset"]
    assert signature.parameters["finishing"].kind is inspect.Parameter.KEYWORD_ONLY
    assert Finishing.ROWS != Finishing.RECORDS_ONLY


def test_the_records_route_takes_the_plan_and_its_dataset():
    sv = SchemaView()
    sv.add_schema_str(_SCHEMA_YAML)
    inferred = "urn:graph:inferred"
    dataset = DatasetSpec(
        TypeRegime.ASSERTED,
        data_graphs={A: TypeRegime.ASSERTED, inferred: TypeRegime.IS_A},
    )
    query = f"SELECT ?s FROM <{inferred}> WHERE {{ ?s a <https://example.org/s/Asset> }}"
    plan = plan_query_refined(query, sv, dataset)
    assert plan.dataset == dataset
    signature = inspect.signature(sparql_execute_plan)
    assert list(signature.parameters)[:3] == ["plan", "instances", "schema_view"]
    assert "dataset" not in signature.parameters
    for name in ("max_triples", "max_result_rows", "max_eval_millis"):
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
    content_type, body = sparql_execute_plan(plan, [], sv)
    assert "json" in content_type and '"bindings":[]' in body

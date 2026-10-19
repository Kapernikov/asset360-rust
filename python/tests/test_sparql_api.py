"""The SPARQL endpoint's Python surface: `DatasetSpec` and the argument order
of the entry points (`(query, schema_view, dataset, *, limits)`)."""

from __future__ import annotations

import inspect

import pytest
from asset360_rust import (
    DatasetSpec,
    SchemaView,
    TypePartition,
    TypeRegime,
    sparql_execute,
    sparql_finish,
    sparql_schema_graph_ntriples,
    sparql_schema_graph_skipped,
)

A = "urn:graph:asserted"
S = "urn:graph:schema"


def test_a_dataset_that_contradicts_itself_raises_with_every_conflict():
    with pytest.raises(ValueError) as raised:
        DatasetSpec(
            TypeRegime.ASSERTED,
            data_graphs={A: TypeRegime.ASSERTED},
            default_graph="urn:graph:nowhere",
            schema_graphs={A: TypeRegime.IS_A, S: TypeRegime.IS_A},
        )
    message = str(raised.value)
    # Both mistakes are named, not the first one met.
    assert "urn:graph:nowhere" in message and "not a registered data graph" in message
    assert A in message and "a graph has one meaning" in message


def test_a_coherent_dataset_builds():
    DatasetSpec(
        TypeRegime.ASSERTED,
        data_graphs={A: TypeRegime.ASSERTED},
        default_graph=A,
        schema_graphs={S: TypeRegime.IS_A},
    )


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
        TypeRegime.ASSERTED,
        data_graphs={A: TypeRegime.IS_A},
        default_graph=A,
        partition_graphs={TypePartition.Own: "urn:graph:own"},
        schema_graphs={S: TypeRegime.IS_A_AND_MIXINS},
    )
    # The default graph is a data graph, so its regime is the default regime.
    assert dataset.default_regime == TypeRegime.IS_A
    assert dataset.data_graphs == {A: TypeRegime.IS_A}
    assert dataset.default_graph == A
    assert dataset.partition_graphs == {TypePartition.Own: "urn:graph:own"}
    assert dataset.schema_graphs == {S: TypeRegime.IS_A_AND_MIXINS}
    assert repr(dataset) == (
        "DatasetSpec(TypeRegime.IS_A, data_graphs={'urn:graph:asserted': TypeRegime.IS_A}"
        ", default_graph='urn:graph:asserted'"
        ", partition_graphs={TypePartition.Own: 'urn:graph:own'}"
        ", schema_graphs={'urn:graph:schema': TypeRegime.IS_A_AND_MIXINS})"
    )


def test_a_regime_that_is_no_named_one_reads_back_as_itself():
    own_and_isa = TypeRegime([TypePartition.Own, TypePartition.IsA])
    dataset = DatasetSpec(TypeRegime.ASSERTED, data_graphs={A: own_and_isa})
    assert dataset.data_graphs == {A: own_and_isa}
    assert repr(dataset) == (
        "DatasetSpec(TypeRegime.ASSERTED, "
        "data_graphs={'urn:graph:asserted': TypeRegime.parse('own+isa')})"
    )


def test_a_bare_dataset_reads_back_empty():
    dataset = DatasetSpec(TypeRegime.ASSERTED)
    assert dataset.default_regime == TypeRegime.ASSERTED
    assert dataset.data_graphs == {} and dataset.schema_graphs == {}
    assert dataset.default_graph is None and dataset.partition_graphs == {}
    assert repr(dataset) == "DatasetSpec(TypeRegime.ASSERTED)"

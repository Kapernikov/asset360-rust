"""The SPARQL endpoint's Python surface: `DatasetSpec` and the argument order
of the entry points (`(query, schema_view, dataset, *, limits)`)."""

from __future__ import annotations

import inspect

import pytest
from asset360_rust import (
    DatasetSpec,
    SchemaView,
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
            TypeRegime.Asserted,
            data_graphs={A: TypeRegime.Asserted},
            default_graph="urn:graph:nowhere",
            schema_graphs={A: TypeRegime.IsA, S: TypeRegime.IsA},
        )
    message = str(raised.value)
    # Both mistakes are named, not the first one met.
    assert "urn:graph:nowhere" in message and "not a registered data graph" in message
    assert A in message and "a graph has one meaning" in message


def test_a_coherent_dataset_builds():
    DatasetSpec(
        TypeRegime.Asserted,
        data_graphs={A: TypeRegime.Asserted},
        default_graph=A,
        schema_graphs={S: TypeRegime.IsA},
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
    asserted = sparql_schema_graph_ntriples(sv, TypeRegime.Asserted)
    assert "<https://example.org/s/Tunnel> " + subclass_of + " <https://example.org/s/Asset>" in asserted
    assert asserted != sparql_schema_graph_ntriples(sv, TypeRegime.IsA)
    assert sparql_schema_graph_skipped(sv, TypeRegime.Asserted) == []

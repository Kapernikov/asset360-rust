"""`check_predicates`: the non-refusing report of which predicates a query's
subjects can carry, as the SHACL linter reads it (consolidator-server #306)."""

from __future__ import annotations

import pytest
from asset360_rust import SchemaView, check_predicates

_SCHEMA_YAML = """
id: https://example.org/s
name: s
prefixes:
  ex: https://example.org/s/
  rsm: http://rsm.example.org/RSM#
  linkml: https://w3id.org/linkml/
default_prefix: ex
default_range: string
classes:
  Asset:
    class_uri: ex:Asset
    attributes:
      name:
        slot_uri: rsm:EAID_NAME
  Tunnel:
    is_a: Asset
    class_uri: ex:Tunnel
    attributes:
      length:
        range: string
"""

ASSET = "https://example.org/s/Asset"
NAME = "http://rsm.example.org/RSM#EAID_NAME"
WRONG_NAME = "https://example.org/s/name"
LENGTH = "https://example.org/s/length"


def _schema() -> SchemaView:
    sv = SchemaView()
    sv.add_schema_str(_SCHEMA_YAML)
    return sv


def _by_predicate(checks):
    return {c.predicate: c for c in checks}


def test_a_seeded_this_is_judged_by_its_class_family_as_written():
    checks = _by_predicate(
        check_predicates(
            f"SELECT $this WHERE {{ $this <{WRONG_NAME}> ?a ; <{NAME}> ?b ; <{LENGTH}> ?c }}",
            _schema(),
            {"this": ASSET},
        )
    )
    wrong = checks[WRONG_NAME]
    assert wrong.verdict == "not_carried"
    assert wrong.suggestion == NAME
    assert wrong.class_iri == ASSET
    assert "can never match" in wrong.message
    assert checks[NAME].verdict == "carried"
    # a slot only a subclass carries is carried by the family
    assert checks[LENGTH].verdict == "carried"
    assert checks[LENGTH].message is None


def test_a_subject_without_an_inferable_class_is_unchecked():
    (check,) = check_predicates(f"SELECT ?s WHERE {{ ?s <{NAME}> ?n }}", _schema())
    assert (check.subject, check.verdict, check.class_iri) == ("?s", "unchecked", None)


def test_a_query_that_does_not_parse_raises_value_error():
    with pytest.raises(ValueError):
        check_predicates("SELECT WHERE {", _schema())

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

from asset360_rust import (
    Asset360ChangeMeta,
    ChangeStage,
    apply_deltas,
    blame_map_to_path_stage_map,
    compute_history,
    format_blame_map,
)

_test_blame = import_module("python.tests.test_blame")
_load_schema_view = _test_blame._load_schema_view  # type: ignore[attr-defined]


def _load_stage_fixture(sv, path: Path) -> list[ChangeStage]:
    payload = json.loads(path.read_text())
    return [ChangeStage.from_json(sv, entry) for entry in payload]


def _normalize_stage_dict(data: dict) -> dict:
    # Normalize through JSON to remove ordering differences and sort deltas/rejected paths
    normalized = json.loads(json.dumps(data))
    if "deltas" in normalized:
        normalized["deltas"] = sorted(
            normalized["deltas"],
            key=lambda d: (tuple(d.get("path", [])), d.get("op")),
        )
    if "rejected_paths" in normalized and normalized["rejected_paths"] is not None:
        normalized["rejected_paths"] = sorted(
            normalized["rejected_paths"], key=lambda path: list(path)
        )
    return normalized


def test_compute_history_matches_recomputed_fixture(tmp_path: Path) -> None:
    sv = _load_schema_view()
    base_dir = Path(__file__).resolve().parents[2] / "tests" / "data"

    stages = _load_stage_fixture(sv, base_dir / "stages.json")
    expected = json.loads((base_dir / "recomputed_stages.json").read_text())

    assert stages, "stages fixture should not be empty"
    assert len(stages) == len(expected)

    _, recomputed = compute_history(stages)

    assert len(recomputed) == len(expected)

    computed_payloads = [_normalize_stage_dict(stage.to_json()) for stage in recomputed]
    expected_payloads = [_normalize_stage_dict(item) for item in expected]

    assert computed_payloads == expected_payloads

    # Also exercise blame-map utilities on the recomputed stages
    base_stage, *history_tail = recomputed
    object_instance, blame_map = apply_deltas(base_stage.value, history_tail)
    path_map = blame_map_to_path_stage_map(object_instance, blame_map)
    blame_text = format_blame_map(object_instance, blame_map)

    output = tmp_path / "blame_dump.txt"
    output.write_text(blame_text)

    assert path_map, "expected non-empty path map"
    assert all(isinstance(meta, Asset360ChangeMeta) for _, meta in path_map)
    assert output.read_text() == blame_text

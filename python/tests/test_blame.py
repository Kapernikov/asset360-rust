from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
PYTHON_DIR = ROOT / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from asset360_rust import (  # noqa: E402  (import after path tweak)
    Asset360ChangeMeta,
    ChangeStage,
    Delta,
    SchemaView,
    load_json,
)


def _load_instance(json_payload: str, sv: SchemaView, cv):
    assert isinstance(json_payload, str), f"expected serialized JSON text, got {type(json_payload)}"
    value, errors = load_json(json_payload, sv, cv)
    for issue in errors:
        warnings.warn(f"linkml validation issue: {issue}")
    assert value is not None, "expected LinkMLInstance result"
    return value


def _load_schema_view() -> SchemaView:
    sv = SchemaView()
    data_dir = ROOT / "tests" / "data"
    for name in ["types.yaml", "rsm.yaml", "eulynx.yaml", "asset360.yaml"]:
        sv.add_schema_from_path(str(data_dir / name))
    return sv


def _build_change_stages(sv: SchemaView, class_id: str) -> list[ChangeStage]:
    cv = sv.get_class_view(class_id)
    assert cv is not None, f"expected class '{class_id}' in schema"

    stages_path = ROOT / "tests" / "data" / "asset360_stages.json"
    stages_payload = json.loads(stages_path.read_text())

    stages: list[ChangeStage] = []
    for entry in stages_payload:
        meta_dict = entry["meta"]
        meta = Asset360ChangeMeta(
            meta_dict["author"],
            meta_dict["timestamp"],
            meta_dict["source"],
            meta_dict["change_id"],
            meta_dict["ics_id"],
        )
        value = _load_instance(json.dumps(entry["value"]), sv, cv)
        deltas = [
            Delta(delta["path"], delta["op"], delta.get("old"), delta.get("new"))
            for delta in entry.get("deltas", [])
        ]
        rejected_paths = entry.get("rejected_paths")
        stages.append(ChangeStage(meta, value, deltas, rejected_paths))
    return stages


def test_change_stage_json_roundtrip() -> None:
    sv = _load_schema_view()
    class_id = "https://data.infrabel.be/asset360/Signal"
    stages = _build_change_stages(sv, class_id)
    assert stages, "change stages fixture should not be empty"

    original = stages[0]
    payload = original.to_json()
    assert payload["class_id"] == class_id
    # Ensure payload is JSON-serializable and normalize it through JSON encode/decode.
    roundtrip_payload = json.loads(json.dumps(payload))

    reconstructed = ChangeStage.from_json(sv, roundtrip_payload)

    assert reconstructed.meta.change_id == original.meta.change_id
    assert reconstructed.rejected_paths == original.rejected_paths
    assert reconstructed.to_json() == roundtrip_payload
    assert reconstructed.value.equals(original.value)


def test_asset360_meta_json_error_message() -> None:
    meta = Asset360ChangeMeta("author", "ts", "source", 1, 2)

    with pytest.raises(TypeError) as exc:
        json.dumps(meta)
    message = str(exc.value)
    assert "Asset360ChangeMeta" in message
    assert "to_dict" in message

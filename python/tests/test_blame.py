from __future__ import annotations

import json
import sys
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
    apply_deltas,
    blame_map_to_path_stage_map,
    format_blame_map,
    load_json,
)


def _format_path(segments: list[str]) -> str:
    if not segments:
        return "<root>"
    out: list[str] = []
    for segment in segments:
        if segment.isdigit():
            out.append(f"[{segment}]")
        else:
            if out:
                out.append(".")
            out.append(segment)
    return "".join(out)


def _format_stage_entries(entries: list[tuple[list[str], dict]]) -> str:
    if not entries:
        return "<empty stage map>"
    lines = []
    for path, meta in entries:
        if isinstance(meta, dict):
            meta_dict = meta
        elif hasattr(meta, "to_dict"):
            meta_dict = meta.to_dict()
            if not isinstance(meta_dict, dict):
                meta_dict = dict(meta_dict)
        else:
            meta_dict = dict(meta)
        lines.append(
            f"{_format_path(path)} => change_id={meta_dict['change_id']} "
            f"author={meta_dict['author']} timestamp={meta_dict['timestamp']} "
            f"source={meta_dict['source']} ics_id={meta_dict['ics_id']}"
        )
    return "\n".join(lines)


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
        value = load_json(json.dumps(entry["value"]), sv, cv)
        deltas = [
            Delta(delta["path"], delta["op"], delta.get("old"), delta.get("new"))
            for delta in entry.get("deltas", [])
        ]
        rejected_paths = entry.get("rejected_paths")
        stages.append(ChangeStage(meta, value, deltas, rejected_paths))
    return stages


@pytest.mark.usefixtures("ensure_pythonpath")
def test_apply_deltas_with_asset360_stages_python() -> None:
    sv = _load_schema_view()
    class_id = "https://data.infrabel.be/asset360/Signal"
    stages = _build_change_stages(sv, class_id)

    assert len(stages) >= 2, "fixture should contain base stage and updates"
    base_stage, *rest_stages = stages

    final_value, blame_map = apply_deltas(base_stage.value, rest_stages)

    blame_map_payload = {node_id: meta for node_id, meta in blame_map.items()}

    stage_entries = blame_map_to_path_stage_map(final_value, blame_map_payload)
    stage_dump = _format_stage_entries(stage_entries)
    print("Asset360 stage map entries:\n" + stage_dump)

    def _extract_change_id(meta: object) -> int | None:
        if isinstance(meta, dict):
            return meta.get("change_id")
        return getattr(meta, "change_id", None)

    seen_changes = {
        change_id
        for change_id in (_extract_change_id(meta) for meta in blame_map_payload.values())
        if change_id is not None
    }
    expected_changes = {stage.meta.change_id for stage in rest_stages}
    assert expected_changes.issubset(seen_changes)


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


@pytest.fixture
def ensure_pythonpath():
    # No-op fixture so @pytest.mark.usefixtures hooks into pytest collection even
    # when the importing environment already handled PYTHONPATH.
    yield

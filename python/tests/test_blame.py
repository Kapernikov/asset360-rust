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
        if not isinstance(meta, dict):
            meta = dict(meta)
        lines.append(
            f"{_format_path(path)} => change_id={meta['change_id']} "
            f"author={meta['author']} timestamp={meta['timestamp']} "
            f"source={meta['source']} ics_id={meta['ics_id']}"
        )
    return "\n".join(lines)


def _load_schema_view() -> SchemaView:
    sv = SchemaView()
    data_dir = ROOT / "tests" / "data"
    for name in ["types.yaml", "rsm.yaml", "eulynx.yaml", "asset360.yaml"]:
        sv.add_schema_from_path(str(data_dir / name))
    return sv


@pytest.mark.usefixtures("ensure_pythonpath")
def test_apply_deltas_with_asset360_stages_python() -> None:
    sv = _load_schema_view()
    class_id = "https://data.infrabel.be/asset360/Signal"
    cv = sv.get_class_view(class_id)
    assert cv is not None, "expected Signal class in schema"

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

    assert len(stages) >= 2, "fixture should contain base stage and updates"
    base_stage, *rest_stages = stages

    final_value, blame_map = apply_deltas(base_stage.value, rest_stages)

    blame_map_payload = {
        node_id: dict(meta.to_dict()) if hasattr(meta, "to_dict") else meta
        for node_id, meta in blame_map.items()
    }

    blame_dump = format_blame_map(final_value, blame_map_payload)
    print("Asset360 stages blame map:\n" + blame_dump)

    stage_entries = blame_map_to_path_stage_map(final_value, blame_map_payload)
    stage_dump = _format_stage_entries(stage_entries)
    print("Asset360 stage map entries:\n" + stage_dump)

    seen_changes = {meta["change_id"] for meta in blame_map_payload.values() if isinstance(meta, dict)}
    expected_changes = {stage.meta.change_id for stage in rest_stages}
    assert expected_changes.issubset(seen_changes)


@pytest.fixture
def ensure_pythonpath():
    # No-op fixture so @pytest.mark.usefixtures hooks into pytest collection even
    # when the importing environment already handled PYTHONPATH.
    yield

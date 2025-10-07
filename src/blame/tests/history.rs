use super::super::{
    Asset360ChangeMeta, ChangeStage, apply_deltas, blame_map_to_path_stage_map, compute_history,
    format_blame_map,
};
use linkml_runtime::diff::Delta;
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use std::fs;

fn load_asset360_schema() -> SchemaView {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    let schema_sources = [
        include_str!("../../../tests/data/types.yaml"),
        include_str!("../../../tests/data/rsm.yaml"),
        include_str!("../../../tests/data/eulynx.yaml"),
        include_str!("../../../tests/data/asset360.yaml"),
    ];

    let mut sv = SchemaView::new();
    for raw in schema_sources {
        let schema: SchemaDefinition = p2e::deserialize(yml::Deserializer::from_str(raw)).unwrap();
        sv.add_schema(schema).unwrap();
    }
    sv
}

fn load_change_stages(sv: &SchemaView, stages_json: &str) -> Vec<ChangeStage<Asset360ChangeMeta>> {
    let mut stages: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::new();
    let mut entries: Vec<serde_json::Value> = serde_json::from_str(stages_json).unwrap();
    let conv = sv.converter_for_primary_schema().unwrap();

    for entry in entries.drain(..) {
        let class_id = entry
            .get("class_id")
            .and_then(|v| v.as_str())
            .expect("class_id present");
        let class_view = sv
            .get_class(&Identifier::new(class_id), conv)
            .unwrap()
            .expect("class view present");

        let meta: Asset360ChangeMeta =
            serde_json::from_value(entry.get("meta").cloned().expect("meta present")).unwrap();

        let value_json = entry.get("value").cloned().expect("value present");
        let value_str = serde_json::to_string(&value_json).unwrap();
        let value = linkml_runtime::load_json_str(&value_str, sv, &class_view, conv)
            .expect("stage value conversion");

        let deltas: Vec<Delta> = entry
            .get("deltas")
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();

        let rejected_paths: Vec<Vec<String>> = entry
            .get("rejected_paths")
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();

        stages.push(ChangeStage {
            meta,
            value,
            deltas,
            rejected_paths,
        });
    }
    stages
}

fn sorted_delta_values(deltas: &[Delta]) -> Vec<serde_json::Value> {
    let mut json_deltas: Vec<_> = deltas
        .iter()
        .map(|delta| serde_json::to_value(delta).expect("delta serializable"))
        .collect();
    json_deltas.sort_by_key(|value| value.to_string());
    json_deltas
}

#[test]
fn test_compute_history_matches_fixture_and_blame_dump() {
    let sv = load_asset360_schema();
    let stages_fixture = include_str!("../../../tests/data/stages.json");
    let recomputed_fixture = include_str!("../../../tests/data/recomputed_stages.json");

    let stages = load_change_stages(&sv, stages_fixture);
    let expected_entries: Vec<serde_json::Value> =
        serde_json::from_str(recomputed_fixture).unwrap();
    assert_eq!(stages.len(), expected_entries.len());

    let (final_value, history) = compute_history(stages.clone());
    assert_eq!(history.len(), expected_entries.len());

    for (stage, expected) in history.iter().zip(expected_entries.iter()) {
        let expected_meta: Asset360ChangeMeta =
            serde_json::from_value(expected.get("meta").cloned().expect("meta"))
                .expect("valid meta");
        assert_eq!(stage.meta, expected_meta);

        let expected_value = expected.get("value").cloned().expect("value");
        assert_eq!(stage.value.to_json(), expected_value);

        let expected_deltas: Vec<Delta> = expected
            .get("deltas")
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();
        assert_eq!(
            sorted_delta_values(&stage.deltas),
            sorted_delta_values(&expected_deltas)
        );

        let expected_rejected: Vec<Vec<String>> = expected
            .get("rejected_paths")
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .unwrap_or_default();
        assert_eq!(stage.rejected_paths, expected_rejected);
    }

    let base_value = history
        .first()
        .expect("at least one stage in history")
        .value
        .clone();
    let rest_stages: Vec<_> = history.iter().skip(1).cloned().collect();
    let (applied_value, blame_map) = apply_deltas(Some(base_value), rest_stages);

    assert_eq!(applied_value.to_json(), final_value.to_json());

    let entries = blame_map_to_path_stage_map(&applied_value, &blame_map);
    assert!(!entries.is_empty());

    let blame_dump = format_blame_map(&applied_value, &blame_map);
    let output_path = std::env::temp_dir().join("asset360_recomputed_blame.txt");
    fs::write(&output_path, blame_dump.as_bytes()).expect("write blame map");

    let written = fs::read_to_string(&output_path).expect("read blame map output");
    assert!(written.contains("cid="));
    assert!(written.contains("author="));
}

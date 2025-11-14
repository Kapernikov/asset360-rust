use super::super::*;
use super::common::format_stage_entries;
use linkml_schemaview::schemaview::SchemaView;

#[test]
fn test_apply_deltas_with_asset360_stages() {
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
    let conv = sv.converter_for_primary_schema().unwrap();
    let signal_class = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new(
                "https://data.infrabel.be/asset360/Signal",
            ),
            &conv,
        )
        .unwrap()
        .unwrap();

    let stages_json = include_str!("../../../tests/data/asset360_stages.json");
    let mut parsed: Vec<serde_json::Value> = serde_json::from_str(stages_json).unwrap();

    let mut stages: Vec<ChangeStage<Asset360ChangeMeta>> = Vec::new();
    for entry in parsed.drain(..) {
        let meta: Asset360ChangeMeta =
            serde_json::from_value(entry.get("meta").cloned().expect("meta present")).unwrap();
        let value_json = entry.get("value").cloned().expect("value present");
        let value_str = serde_json::to_string(&value_json).unwrap();
        let value = linkml_runtime::load_json_str(&value_str, &sv, &signal_class, &conv)
            .expect("stage value conversion")
            .into_instance_tolerate_errors()
            .unwrap();
        let deltas: Vec<Delta> =
            serde_json::from_value(entry.get("deltas").cloned().expect("deltas present")).unwrap();

        stages.push(ChangeStage {
            meta,
            value,
            deltas,
            rejected_paths: Vec::new(),
        });
    }

    assert!(stages.len() >= 2);
    let base_stage = stages.remove(0);
    let (final_value, blame_map) = apply_deltas(Some(base_stage.value), stages.clone());

    let blame_dump = format_blame_map(&final_value, &blame_map);
    println!(
        "Asset360 stages blame map:
{}",
        blame_dump
    );
    let stage_entries = blame_map_to_path_stage_map(&final_value, &blame_map);
    let stage_dump = format_stage_entries(&stage_entries);
    println!(
        "Asset360 stage map entries:
{}",
        stage_dump
    );

    assert!(!stages.is_empty());
}

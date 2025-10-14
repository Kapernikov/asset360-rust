use super::super::*;
use super::common::format_stage_entries;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn test_apply_multiple_stages_preserves_blame_history() {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    const SCHEMA: &str = include_str!("../../../tests/data/blame_series/schema.yaml");
    const GENERATIONS: [&str; 4] = [
        include_str!("../../../tests/data/blame_series/project_v0.yaml"),
        include_str!("../../../tests/data/blame_series/project_v1.yaml"),
        include_str!("../../../tests/data/blame_series/project_v2.yaml"),
        include_str!("../../../tests/data/blame_series/project_v3.yaml"),
    ];

    let schema: SchemaDefinition = p2e::deserialize(yml::Deserializer::from_str(SCHEMA)).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    let conv = sv.converter_for_primary_schema().unwrap();
    let project_class = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Project"),
            &conv,
        )
        .unwrap()
        .unwrap();

    let generations: Vec<LinkMLInstance> = GENERATIONS
        .iter()
        .map(|data| linkml_runtime::load_yaml_str(data, &sv, &project_class, &conv).unwrap())
        .collect();

    let base = generations
        .first()
        .cloned()
        .expect("base generation present");
    let expected_final = generations
        .last()
        .cloned()
        .expect("final generation present");

    let stage_metadata = vec![
        Asset360ChangeMeta {
            author: "planner.one".into(),
            timestamp: "2024-01-01T09:00:00Z".into(),
            source: "ingest".into(),
            change_id: 1,
            ics_id: 1001,
        },
        Asset360ChangeMeta {
            author: "planner.two".into(),
            timestamp: "2024-01-03T14:00:00Z".into(),
            source: "ingest".into(),
            change_id: 2,
            ics_id: 1002,
        },
        Asset360ChangeMeta {
            author: "planner.three".into(),
            timestamp: "2024-01-05T08:30:00Z".into(),
            source: "ingest".into(),
            change_id: 3,
            ics_id: 1003,
        },
    ];

    let stages: Vec<ChangeStage<Asset360ChangeMeta>> = stage_metadata
        .into_iter()
        .enumerate()
        .map(|(idx, meta)| ChangeStage {
            rejected_paths: vec![],
            meta,
            value: generations[idx + 1].clone(),
            deltas: linkml_runtime::diff::diff(
                &generations[idx],
                &generations[idx + 1],
                DiffOptions {
                    treat_missing_as_null: true,
                    ..DiffOptions::default()
                },
            ),
        })
        .collect();

    let (updated, blame_map) = apply_deltas(Some(base), stages);

    assert_eq!(updated.to_json(), expected_final.to_json());

    let blame_dump = format_blame_map(&updated, &blame_map);
    println!(
        "Multi-stage blame map:
{}",
        blame_dump
    );
    let entries = blame_map_to_path_stage_map(&updated, &blame_map);
    let stage_dump = format_stage_entries(&entries);
    println!(
        "Stage map entries:
{}",
        stage_dump
    );
    let mut path_meta: BTreeMap<Vec<String>, Asset360ChangeMeta> = entries.into_iter().collect();

    let root_meta = path_meta.remove(&Vec::new()).unwrap_or_else(|| {
        panic!(
            "root blame present
{blame_dump}"
        )
    });
    assert_eq!(root_meta.change_id, 1);

    let role_meta = path_meta
        .remove(&vec!["owner".to_string(), "role".to_string()])
        .unwrap_or_else(|| {
            panic!(
                "owner.role blame present
{blame_dump}"
            )
        });
    assert_eq!(role_meta.change_id, 2);

    let task_title_meta = path_meta
        .remove(&vec![
            "tasks".to_string(),
            "1".to_string(),
            "title".to_string(),
        ])
        .unwrap_or_else(|| {
            panic!(
                "tasks[1].title blame present
{blame_dump}"
            )
        });
    assert_eq!(task_title_meta.change_id, 2);

    let description_meta = path_meta
        .remove(&vec!["description".to_string()])
        .unwrap_or_else(|| {
            panic!(
                "description blame present
{blame_dump}"
            )
        });
    assert_eq!(description_meta.change_id, 3);

    let task_status_meta = path_meta
        .remove(&vec![
            "tasks".to_string(),
            "0".to_string(),
            "status".to_string(),
        ])
        .unwrap_or_else(|| {
            panic!(
                "tasks[0].status blame present
{blame_dump}"
            )
        });
    assert_eq!(task_status_meta.change_id, 3);

    let mut seen_changes: BTreeSet<u64> = path_meta.values().map(|meta| meta.change_id).collect();
    seen_changes.insert(root_meta.change_id);
    seen_changes.insert(role_meta.change_id);
    seen_changes.insert(task_title_meta.change_id);
    seen_changes.insert(description_meta.change_id);
    seen_changes.insert(task_status_meta.change_id);
    assert!(seen_changes.contains(&1));
    assert!(seen_changes.contains(&2));
    assert!(seen_changes.contains(&3));
}

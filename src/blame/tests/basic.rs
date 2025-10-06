use super::super::*;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::HashMap;

#[test]
fn test_get_blame_info_with_manual_map() {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    let mut blame = HashMap::new();
    let meta1 = Asset360ChangeMeta {
        author: "a".into(),
        timestamp: "t1".into(),
        source: "manual".into(),
        change_id: 1,
        ics_id: 101,
    };
    let meta2 = Asset360ChangeMeta {
        author: "b".into(),
        timestamp: "t2".into(),
        source: "manual".into(),
        change_id: 2,
        ics_id: 102,
    };

    let schema_yaml = r#"id: https://example.org/testname: testdefault_prefix: exprefixes:  ex:    prefix_reference: http://example.org/classes:  Root: {}"#;
    let schema: SchemaDefinition =
        p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    let conv = sv.converter_for_primary_schema().unwrap();
    let class = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Root"),
            conv,
        )
        .unwrap()
        .unwrap();
    let value = linkml_runtime::load_yaml_str("{}", &sv, &class, conv).unwrap();

    let id = value.node_id();
    blame.insert(id, meta1.clone());
    blame.insert(id, meta2.clone());

    assert_eq!(get_blame_info(&value, &blame), Some(&meta2));
}

#[test]
fn test_apply_deltas_no_stages() {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    let schema_yaml = r#"id: https://example.org/testname: testdefault_prefix: exprefixes:  ex:    prefix_reference: http://example.org/classes:  Root: {}"#;
    let schema: SchemaDefinition =
        p2e::deserialize(yml::Deserializer::from_str(schema_yaml)).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    let conv = sv.converter_for_primary_schema().unwrap();
    let class = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Root"),
            conv,
        )
        .unwrap()
        .unwrap();

    let base = linkml_runtime::load_yaml_str("{}", &sv, &class, conv).unwrap();
    let (out, blame) = apply_deltas(Some(base.clone()), vec![]);

    assert!(blame.is_empty());
    assert_eq!(out.node_id(), base.node_id());
}

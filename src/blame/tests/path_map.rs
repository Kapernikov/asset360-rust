use super::super::*;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::HashMap;

#[test]
fn test_blame_map_to_path_stage_map() {
    use linkml_meta::SchemaDefinition;
    use serde_path_to_error as p2e;
    use serde_yml as yml;

    let schema_yaml = r#"
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
slots:
  name:
    range: string
  child:
    range: Child
  items:
    range: Child
    multivalued: true
  title:
    range: string
classes:
  Root:
    slots:
      - name
      - child
      - items
  Child:
    slots:
      - title
"#;
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

    let data = r#"
name: Rooty
child:
  title: Kid
items:
  - title: First
  - title: Second
"#;
    let value = linkml_runtime::load_yaml_str(data, &sv, &class, conv).unwrap();

    let mut blame = HashMap::new();
    let root_meta = Asset360ChangeMeta {
        author: "root-author".into(),
        timestamp: "t0".into(),
        source: "import".into(),
        change_id: 1,
        ics_id: 10,
    };
    blame.insert(value.node_id(), root_meta.clone());

    let child_title_node = match &value {
        LinkMLInstance::Object { values, .. } => values
            .get("child")
            .and_then(|child| match child {
                LinkMLInstance::Object { values, .. } => values.get("title"),
                _ => None,
            })
            .expect("child.title node present"),
        _ => panic!("expected root object"),
    };
    let child_meta = Asset360ChangeMeta {
        author: "child-author".into(),
        timestamp: "t1".into(),
        source: "import".into(),
        change_id: 2,
        ics_id: 20,
    };
    blame.insert(child_title_node.node_id(), child_meta.clone());

    let items_title_nodes = match &value {
        LinkMLInstance::Object { values, .. } => values
            .get("items")
            .and_then(|items| match items {
                LinkMLInstance::List { values, .. } => {
                    if values.len() == 2 {
                        let first = match &values[0] {
                            LinkMLInstance::Object { values, .. } => {
                                values.get("title").expect("items[0].title")
                            }
                            _ => panic!("expected object for items[0]"),
                        };
                        let second = match &values[1] {
                            LinkMLInstance::Object { values, .. } => {
                                values.get("title").expect("items[1].title")
                            }
                            _ => panic!("expected object for items[1]"),
                        };
                        Some((first, second))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .expect("items list present"),
        _ => panic!("expected root object"),
    };

    let (item0_title_node, item1_title_node) = items_title_nodes;

    let item0_meta = Asset360ChangeMeta {
        author: "item0-author".into(),
        timestamp: "t2".into(),
        source: "import".into(),
        change_id: 3,
        ics_id: 30,
    };
    blame.insert(item0_title_node.node_id(), item0_meta.clone());

    let item1_meta = Asset360ChangeMeta {
        author: "item1-author".into(),
        timestamp: "t3".into(),
        source: "import".into(),
        change_id: 4,
        ics_id: 40,
    };
    blame.insert(item1_title_node.node_id(), item1_meta.clone());

    let entries = blame_map_to_path_stage_map(&value, &blame);

    let expected = vec![
        (vec![], root_meta),
        (vec!["child".to_string(), "title".to_string()], child_meta),
        (
            vec!["items".to_string(), "0".to_string(), "title".to_string()],
            item0_meta,
        ),
        (
            vec!["items".to_string(), "1".to_string(), "title".to_string()],
            item1_meta,
        ),
    ];

    assert_eq!(entries, expected);
}

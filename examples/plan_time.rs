//! Time the refined planner on a query: `cargo run --release --example plan_time -- <schema-dir> <query.rq> [schema-graph-iri]`.
use linkml_meta::SchemaDefinition;
use linkml_schemaview::schemaview::SchemaView;
use std::path::Path;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let query = std::fs::read_to_string(&args[2]).unwrap();
    let graph = args.get(3).map(|s| s.as_str());
    let mut sv = SchemaView::new();
    for name in [
        "types.yaml",
        "rsm.yaml",
        "eulynx.yaml",
        "geosparql.yaml",
        "simple_ce_assets.yaml",
        "tunnels.yaml",
        "asset360.yaml",
    ] {
        let path = dir.join(name);
        if !path.exists() {
            continue;
        }
        let yaml = std::fs::read_to_string(&path).unwrap();
        let deser = serde_yml::Deserializer::from_str(&yaml);
        let schema: SchemaDefinition = serde_path_to_error::deserialize(deser).unwrap();
        sv.add_schema(schema).unwrap();
    }
    let t = Instant::now();
    let plan = asset360_rust::sparql_plan::plan_query_refined_with_schema_graph(&query, &sv, graph);
    let elapsed = t.elapsed();
    match plan {
        Ok(_) => println!("planned in {elapsed:?}"),
        Err(e) => println!("refused in {elapsed:?}: {e}"),
    }
}

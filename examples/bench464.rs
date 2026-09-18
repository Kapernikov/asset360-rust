//! #464 shape: OPTIONAL through an inline array to a record, synthetic.
use oxigraph::io::RdfFormat;
use oxigraph::model::Dataset;
use oxigraph::sparql::{QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use std::fmt::Write as _;
use std::time::Instant;

fn turtle(n_assets: usize, n_tracks: usize, cs_per_asset: usize, other_zone_assets: usize) -> String {
    let mut t = String::new();
    t.push_str("@prefix a: <https://data.infrabel.be/asset360/> .\n@prefix irsm: <https://data.infrabel.be/asset360-rsm-subset/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
    for k in 0..n_tracks {
        let _ = writeln!(t, "<urn:track/{k}> a a:Track ; irsm:name \"T{k}\" ; irsm:longname \"TC{k}\" ; a:hasTrackType \"main\" .");
    }
    let total = n_assets + other_zone_assets;
    for i in 0..total {
        let zone = if i < n_assets { "urn:zone/Z" } else { "urn:zone/other" };
        let _ = writeln!(t, "<urn:asset/{i}> a a:CivilEngineeringAsset ; a:belongsToSubZone <{zone}> ; a:identification \"A{i}\" .");
        for j in 0..cs_per_asset {
            let is_ref = if j == 0 { "true" } else { "false" };
            let _ = writeln!(t, "<urn:asset/{i}> a:hasCoveredSection <urn:asset/{i}/cs/{j}> .\n<urn:asset/{i}/cs/{j}> a a:CoveredSection ; a:isReference {is_ref} ; a:belongsToTrack <urn:track/{}> ; a:hasEntryPointM \"{}\"^^xsd:decimal .", (i + j) % n_tracks, i * 10 + j);
        }
    }
    t
}

const Q: &str = r#"
PREFIX asset360: <https://data.infrabel.be/asset360/>
PREFIX irsm: <https://data.infrabel.be/asset360-rsm-subset/>
SELECT * WHERE {
  ?a a asset360:CivilEngineeringAsset ; asset360:belongsToSubZone <urn:zone/Z> ; asset360:identification ?name .
  OPTIONAL {
    ?a asset360:hasCoveredSection ?cs .
    ?cs asset360:isReference true ; asset360:belongsToTrack ?track ; asset360:hasEntryPointM ?kpFromMeter .
    ?track a asset360:Track ; irsm:name ?trackName ; irsm:longname ?trackCode .
    OPTIONAL { ?track asset360:hasTrackType ?trackDiscr }
  }
}"#;

fn count(r: QueryResults<'_>) -> usize {
    match r { QueryResults::Solutions(s) => s.map(|x| x.unwrap()).count(), _ => unreachable!() }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_assets: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1167);
    let cs: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let explain = args.iter().any(|a| a == "--explain");
    let ttl = turtle(n_assets, 200, cs, 0);
    let parsed: oxigraph::sparql::Query = spargebra::SparqlParser::new().parse_query(Q).unwrap().into();

    let store = Store::new().unwrap();
    let t0 = Instant::now();
    store.load_from_reader(RdfFormat::Turtle, ttl.as_bytes()).unwrap();
    eprintln!("store load: {} quads in {:?}", store.len().unwrap(), t0.elapsed());
    let t0 = Instant::now();
    let bound = SparqlEvaluator::new().for_query(parsed.clone()).on_store(&store);
    if explain {
        let (r, ex) = bound.compute_statistics().explain();
        let n = count(r.unwrap());
        let mut s = Vec::new(); ex.write_in_json(&mut s).unwrap();
        println!("{}", String::from_utf8(s).unwrap());
        eprintln!("Store   (explain): {n} rows in {:?}", t0.elapsed());
    } else {
        let n = count(bound.execute().unwrap());
        eprintln!("Store            : {n} rows in {:?}", t0.elapsed());
    }

    let t0 = Instant::now();
    let dataset: Dataset = oxigraph::io::RdfParser::from_format(RdfFormat::Turtle)
        .for_slice(ttl.as_bytes()).map(|q| q.unwrap()).collect();
    eprintln!("dataset load: {} quads in {:?}", dataset.len(), t0.elapsed());
    let t0 = Instant::now();
    let n = count(SparqlEvaluator::new().for_query(parsed).on_queryable_dataset(&dataset).execute().unwrap());
    eprintln!("Dataset          : {n} rows in {:?}", t0.elapsed());
}

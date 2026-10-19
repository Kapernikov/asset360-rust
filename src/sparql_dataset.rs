//! The dataset a SPARQL endpoint serves: which named graphs exist, what each
//! one holds, and what the default graph means.
//!
//! One value, passed to every entry point that plans or evaluates a query, so
//! no call can forget it. The endpoint serves
//!
//! * **data graphs** -- named graphs over the golden records, each under a
//!   [`TypeRegime`] (which `rdf:type` triples its records carry),
//! * a **default graph**, the records under a regime, and
//! * **schema graphs** -- the datamodel itself, one per regime its subclass
//!   edges are published under.
//!
//! The planner reads it to decide which regime a triple pattern is matched
//! under (and so which classes a scan stands for); the executor reads it to
//! decide which schema graphs a query can observe. Nothing here knows SPARQL.

use std::collections::BTreeMap;
use std::fmt;

use linkml_runtime::schema_rdf::{TypePartition, TypeRegime};

/// Which named graphs the endpoint serves and what the default graph means.
///
/// Always valid: the only way to make one is [`DatasetSpec::new`], which
/// refuses a dataset that contradicts itself, so an entry point that takes a
/// `DatasetSpec` has nothing to check. There is deliberately no [`Default`]: a
/// dataset is something a deployment decides, and one that is defaulted by
/// accident plans a different endpoint's query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetSpec {
    default: TypeRegime,
    data_graphs: BTreeMap<String, TypeRegime>,
    schema_graphs: BTreeMap<String, TypeRegime>,
}

/// One way a dataset contradicts itself.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DatasetConflict {
    /// One IRI is both a data graph and a schema graph.
    GraphIsDataAndSchema { iri: String },
    /// A data graph's regime lacks the [`TypePartition::Own`] partition, which
    /// holds every non-type triple of a record: such a graph holds no record's
    /// slot values, and a scan over it would claim records the writer never
    /// typed that way.
    DataGraphWithoutOwn { iri: String, regime: TypeRegime },
    /// The default regime lacks [`TypePartition::Own`]: the records loaded
    /// under it would carry no slot values (see [`Self::DataGraphWithoutOwn`]).
    DefaultRegimeWithoutOwn { regime: TypeRegime },
}

impl fmt::Display for DatasetConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GraphIsDataAndSchema { iri } => write!(
                f,
                "<{iri}> is both a data graph and a schema graph; a graph has one meaning"
            ),
            Self::DataGraphWithoutOwn { iri, regime } => write!(
                f,
                "the data graph <{iri}> has regime {regime}, which lacks the own partition; \
                 a graph of records holds their slot values in it"
            ),
            Self::DefaultRegimeWithoutOwn { regime } => write!(
                f,
                "the default regime {regime} lacks the own partition; \
                 records are written with their slot values in it"
            ),
        }
    }
}

/// A dataset that does not describe one coherent endpoint: **every** conflict
/// found, in the order they were met, never fewer than one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatasetSpecError {
    conflicts: Vec<DatasetConflict>,
}

impl DatasetSpecError {
    /// Every conflict, in the order they were met. Never empty.
    pub fn conflicts(&self) -> &[DatasetConflict] {
        &self.conflicts
    }
}

impl fmt::Display for DatasetSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (n, conflict) in self.conflicts.iter().enumerate() {
            if n > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{conflict}")?;
        }
        Ok(())
    }
}

impl std::error::Error for DatasetSpecError {}

impl DatasetSpec {
    /// The dataset whose default graph holds the records under `default_regime`,
    /// that serves the records as the named `data_graphs` (each under its own
    /// regime) and the datamodel as the `schema_graphs` (each publishing its
    /// subclass edges under its regime). The one place a dataset is stated.
    ///
    /// Refused, with **every** conflict found: an IRI in both maps, and a
    /// regime without [`TypePartition::Own`] (for the default and for a data
    /// graph).
    pub fn new(
        default_regime: TypeRegime,
        data_graphs: BTreeMap<String, TypeRegime>,
        schema_graphs: BTreeMap<String, TypeRegime>,
    ) -> Result<Self, DatasetSpecError> {
        let mut conflicts = Vec::new();
        if !default_regime.contains(TypePartition::Own) {
            conflicts.push(DatasetConflict::DefaultRegimeWithoutOwn {
                regime: default_regime,
            });
        }
        for (iri, regime) in &data_graphs {
            if schema_graphs.contains_key(iri) {
                conflicts.push(DatasetConflict::GraphIsDataAndSchema { iri: iri.clone() });
            }
            if !regime.contains(TypePartition::Own) {
                conflicts.push(DatasetConflict::DataGraphWithoutOwn {
                    iri: iri.clone(),
                    regime: *regime,
                });
            }
        }
        if conflicts.is_empty() {
            Ok(Self {
                default: default_regime,
                data_graphs,
                schema_graphs,
            })
        } else {
            Err(DatasetSpecError { conflicts })
        }
    }

    /// The regime a triple pattern outside any `GRAPH` is matched under when
    /// the query names no `FROM`: the default graph's.
    pub fn default_regime(&self) -> TypeRegime {
        self.default
    }

    /// The regime of the data graph `iri`, if it is one.
    pub fn data_regime(&self, iri: &str) -> Option<TypeRegime> {
        self.data_graphs.get(iri).copied()
    }

    /// The regime the schema graph `iri` publishes its subclass edges under,
    /// if it is one.
    pub fn schema_regime(&self, iri: &str) -> Option<TypeRegime> {
        self.schema_graphs.get(iri).copied()
    }

    /// Every data graph, IRI order.
    pub fn data_graphs(&self) -> impl Iterator<Item = (&str, TypeRegime)> {
        self.data_graphs
            .iter()
            .map(|(iri, regime)| (iri.as_str(), *regime))
    }

    /// Every schema graph, IRI order.
    pub fn schema_graphs(&self) -> impl Iterator<Item = (&str, TypeRegime)> {
        self.schema_graphs
            .iter()
            .map(|(iri, regime)| (iri.as_str(), *regime))
    }

    /// Whether `iri` is a schema graph.
    pub(crate) fn is_schema_graph(&self, iri: &str) -> bool {
        self.schema_graphs.contains_key(iri)
    }

    /// Whether `GRAPH ?g` can bind nothing but a schema graph.
    ///
    /// True while no named graph holds records: the records are in the default
    /// graph, which a `GRAPH` clause never reaches, so the named graphs are the
    /// schema graphs and only them. Once the dataset serves a data graph a
    /// graph variable ranges over it too, and it is no longer a question about
    /// the datamodel.
    pub(crate) fn graph_variable_is_schema_only(&self) -> bool {
        self.data_graphs.is_empty()
    }

    /// The dataset as one query's `FROM NAMED` leaves it: under such a clause
    /// the named graphs are exactly the ones it lists, so a schema graph
    /// outside the list is not there for the query to read. What a rule that
    /// evaluates schema-only subplans consults, so it never answers from a
    /// graph the engine would not show the query.
    pub(crate) fn seen_through(&self, clause: Option<&spargebra::algebra::QueryDataset>) -> Self {
        let mut seen = self.clone();
        // A `FROM` alone parses as an empty `FROM NAMED` list, and that is the
        // truth of it: no named graph is visible (SPARQL 1.1 13.2).
        if let Some(named) = clause.and_then(|clause| clause.named.as_ref()) {
            seen.schema_graphs
                .retain(|iri, _| named.iter().any(|graph| graph.as_str() == iri));
        }
        seen
    }

    /// Whether the dataset serves any schema graph at all.
    pub(crate) fn has_schema_graph(&self) -> bool {
        !self.schema_graphs.is_empty()
    }
}

/// What one graph holds, as far as the planner can tell.
///
/// What a [`crate::sparql_refine::PlanOp::Match`] says it reads, so the cases
/// a rule must tell apart stay apart: the records (under a regime), the
/// datamodel, nothing at all, and a graph only the engine can speak for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GraphReads {
    /// The golden records, under the plan's one regime
    /// ([`crate::sparql_refine::Plan::data_regime`]). The SQL route reads them.
    Data,
    /// The datamodel. Only the engine, or a schema-only region, reads it.
    Schema,
    /// Nothing: the empty graph. The default graph of a query that writes
    /// `FROM NAMED` and no `FROM` (SPARQL 1.1 13.2), which no triple pattern
    /// matches in.
    Empty,
    /// Anything else: a graph the endpoint does not serve, a `GRAPH ?g`, or a
    /// default graph that mixes data and schema graphs. Only the engine can say
    /// what it holds.
    Other,
}

impl GraphReads {
    /// Whether this is the data graph, the only one that holds records a scan
    /// could read: what a rule about type triples asks.
    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data)
    }
}

/// A query's view of the dataset: [`DatasetSpec`] and the `FROM` / `FROM
/// NAMED` clause the query put over it.
///
/// The one place that decides which graph a triple pattern reads, so the
/// planner's naive plan and the scoper's star decomposition cannot disagree
/// about it.
#[derive(Debug, Clone)]
pub(crate) struct QueryGraphs<'a> {
    dataset: &'a DatasetSpec,
    /// The one regime the query reads its records under
    /// ([`crate::sparql_graph_clauses::single_data_graph`]).
    regime: TypeRegime,
    from: Vec<String>,
    /// Whether the query wrote a `FROM` / `FROM NAMED` clause at all.
    clause_written: bool,
    /// The graphs a `GRAPH` clause can reach, when the query wrote a dataset
    /// clause: exactly its `FROM NAMED` list, empty for a `FROM` alone.
    from_named: Option<Vec<String>>,
}

impl<'a> QueryGraphs<'a> {
    /// The view of `query` over `dataset`, or the refusal that it reads more
    /// than one data graph (see
    /// [`crate::sparql_graph_clauses::single_data_graph`]).
    pub(crate) fn new(
        dataset: &'a DatasetSpec,
        query: &spargebra::Query,
    ) -> Result<Self, crate::sparql_graph_clauses::CrossDataGraph> {
        let regime = crate::sparql_graph_clauses::single_data_graph(query, dataset)?.regime;
        let clause = crate::sparql_scoper::dataset_of(query);
        Ok(Self {
            regime,
            dataset,
            clause_written: clause.is_some(),
            from: clause
                .map(|clause| {
                    clause
                        .default
                        .iter()
                        .map(|g| g.as_str().to_owned())
                        .collect()
                })
                .unwrap_or_default(),
            // `FROM` alone parses as an empty `FROM NAMED` list: no named graph
            // at all, as the engine evaluates it.
            from_named: clause.and_then(|clause| {
                clause
                    .named
                    .as_ref()
                    .map(|named| named.iter().map(|g| g.as_str().to_owned()).collect())
            }),
        })
    }

    pub(crate) fn dataset(&self) -> &'a DatasetSpec {
        self.dataset
    }

    /// What a triple pattern outside any `GRAPH` reads, per SPARQL 1.1 13.2:
    /// the dataset's default graph when the query writes no dataset clause,
    /// the union of the `FROM` graphs when it writes any, and the empty graph
    /// for `FROM NAMED` alone.
    pub(crate) fn default_reads(&self) -> GraphReads {
        if self.from.is_empty() && self.clause_written {
            return GraphReads::Empty;
        }
        if self.from.is_empty() {
            return GraphReads::Data;
        }
        // The query reads one data graph, so a `FROM` over data graphs is one
        // regime: the plan's.
        if self
            .from
            .iter()
            .all(|iri| self.dataset.data_regime(iri).is_some())
        {
            GraphReads::Data
        } else if self
            .from
            .iter()
            .all(|iri| self.dataset.is_schema_graph(iri))
        {
            GraphReads::Schema
        } else {
            GraphReads::Other
        }
    }

    /// What a triple pattern inside `GRAPH <iri>` reads.
    pub(crate) fn graph_reads(&self, iri: &str) -> GraphReads {
        // Under `FROM NAMED` a graph outside the set matches nothing. Left to
        // the engine, which says so, rather than read as the graph it names.
        if let Some(named) = &self.from_named
            && !named.iter().any(|graph| graph == iri)
        {
            return GraphReads::Other;
        }
        if self.dataset.data_regime(iri).is_some() {
            GraphReads::Data
        } else if self.dataset.is_schema_graph(iri) {
            GraphReads::Schema
        } else {
            GraphReads::Other
        }
    }

    /// The one regime the query reads its records under.
    pub(crate) fn regime(&self) -> TypeRegime {
        self.regime
    }
}

/// A dataset for a test, from slices instead of maps.
#[cfg(test)]
pub(crate) fn dataset_with(
    default_regime: TypeRegime,
    data_graphs: &[(&str, TypeRegime)],
    schema_graphs: &[(&str, TypeRegime)],
) -> DatasetSpec {
    let owned = |graphs: &[(&str, TypeRegime)]| {
        graphs
            .iter()
            .map(|(iri, regime)| ((*iri).to_owned(), *regime))
            .collect()
    };
    DatasetSpec::new(default_regime, owned(data_graphs), owned(schema_graphs))
        .expect("a valid dataset")
}

/// The dataset a test that is not about datasets plans under, stated
/// explicitly rather than defaulted: an asserted default graph and no named
/// graph. Borrowed for as long as a test's rules need it.
#[cfg(test)]
pub(crate) fn default_dataset() -> &'static DatasetSpec {
    static DEFAULT: std::sync::OnceLock<DatasetSpec> = std::sync::OnceLock::new();
    DEFAULT.get_or_init(|| dataset_with(TypeRegime::ASSERTED, &[], &[]))
}

/// An asserted dataset that serves one schema graph, `iri`.
#[cfg(test)]
pub(crate) fn schema_dataset(iri: &str) -> DatasetSpec {
    dataset_with(TypeRegime::ASSERTED, &[], &[(iri, TypeRegime::ASSERTED)])
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "urn:graph:asserted";
    const I: &str = "urn:graph:inferred";
    const M: &str = "urn:graph:mixins";
    const S: &str = "urn:graph:schema";

    fn served() -> DatasetSpec {
        dataset_with(
            TypeRegime::IS_A,
            &[
                (A, TypeRegime::ASSERTED),
                (I, TypeRegime::IS_A),
                (M, TypeRegime::IS_A_AND_MIXINS),
            ],
            &[(S, TypeRegime::IS_A)],
        )
    }

    fn conflicts_of(
        default: TypeRegime,
        data: &[(&str, TypeRegime)],
        schema: &[(&str, TypeRegime)],
    ) -> Vec<DatasetConflict> {
        let owned = |graphs: &[(&str, TypeRegime)]| {
            graphs
                .iter()
                .map(|(iri, regime)| ((*iri).to_owned(), *regime))
                .collect()
        };
        DatasetSpec::new(default, owned(data), owned(schema))
            .expect_err("the dataset contradicts itself")
            .conflicts()
            .to_vec()
    }

    #[test]
    fn a_dataset_answers_for_each_graph_it_serves() {
        let dataset = served();
        assert_eq!(dataset.default_regime(), TypeRegime::IS_A);
        assert_eq!(dataset.data_regime(M), Some(TypeRegime::IS_A_AND_MIXINS));
        assert_eq!(dataset.schema_regime(S), Some(TypeRegime::IS_A));
        assert_eq!(dataset.data_regime(S), None);
        assert_eq!(dataset.schema_regime(A), None);
        assert!(dataset.has_schema_graph());
        assert!(!default_dataset().has_schema_graph());
    }

    #[test]
    fn a_graph_is_a_data_graph_or_a_schema_graph() {
        assert_eq!(
            conflicts_of(
                TypeRegime::ASSERTED,
                &[(A, TypeRegime::ASSERTED)],
                &[(A, TypeRegime::IS_A)]
            ),
            vec![DatasetConflict::GraphIsDataAndSchema { iri: A.into() }]
        );
    }

    /// `Own` holds every non-type triple of a record, so a graph of records
    /// without it holds no slot values: refused, for a data graph and for the
    /// default regime, and the empty set is one of them.
    #[test]
    fn a_regime_without_own_is_no_graph_of_records() {
        let is_a_only = TypeRegime::from(TypePartition::IsA);
        assert!(!is_a_only.contains(TypePartition::Own));
        assert!(TypeRegime::IS_A_AND_MIXINS.contains(TypePartition::Own));
        for regime in [is_a_only, TypeRegime::empty()] {
            assert_eq!(
                conflicts_of(TypeRegime::ASSERTED, &[("urn:graph:odd", regime)], &[]),
                vec![DatasetConflict::DataGraphWithoutOwn {
                    iri: "urn:graph:odd".into(),
                    regime
                }]
            );
            assert_eq!(
                conflicts_of(regime, &[], &[]),
                vec![DatasetConflict::DefaultRegimeWithoutOwn { regime }]
            );
        }
        let text = DatasetConflict::DataGraphWithoutOwn {
            iri: "urn:graph:odd".into(),
            regime: is_a_only,
        }
        .to_string();
        assert!(
            text.contains("urn:graph:odd") && text.contains("own"),
            "{text}"
        );
    }

    /// Every conflict is reported, not the first one met: a deployment fixing
    /// its configuration should not have to rebuild once per mistake.
    #[test]
    fn every_conflict_is_reported_at_once() {
        let conflicts = conflicts_of(
            TypeRegime::empty(),
            &[(A, TypeRegime::ASSERTED), (S, TypeRegime::empty())],
            &[(S, TypeRegime::IS_A)],
        );
        assert_eq!(conflicts.len(), 3, "{conflicts:?}");
        let text = conflicts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        assert!(text.contains("default regime"), "{text}");
        assert!(text.contains("urn:graph:schema"), "{text}");
    }
}

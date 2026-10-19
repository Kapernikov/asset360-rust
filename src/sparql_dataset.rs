//! The dataset a SPARQL endpoint serves: which named graphs exist, what each
//! one holds, and what the default graph means.
//!
//! One value, passed to every entry point that plans or evaluates a query, so
//! no call can forget it. It replaces the bare `schema_graph_iri` the planner
//! used to be handed: a single optional IRI could say "there is a schema
//! graph" and nothing else, where the endpoint now serves
//!
//! * **data graphs** -- named graphs over the golden records, each under a
//!   [`TypeRegime`] (which `rdf:type` triples its records carry),
//! * a **default graph**, whose regime is the dataset's `default_regime` or
//!   that of the data graph it is declared to be,
//! * **partition graphs** -- the graph that holds one [`TypePartition`] of the
//!   type triples, for an engine that materialises them apart, and
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
/// Built with the `with_*` methods, checked with [`Self::validate`] at every
/// entry point that takes one. [`Self::asserted`] is the dataset every endpoint
/// was before graphs had regimes: an asserted default graph, no named graphs,
/// no schema graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetSpec {
    default_regime: TypeRegime,
    data_graphs: BTreeMap<String, TypeRegime>,
    default_graph: Option<String>,
    partition_graphs: BTreeMap<TypePartition, String>,
    schema_graphs: BTreeMap<String, TypeRegime>,
    /// Registrations that contradicted an earlier one. Kept rather than
    /// refused on the spot, because a builder cannot fail: [`Self::validate`]
    /// names them.
    conflicts: Vec<DatasetSpecError>,
}

/// A dataset that does not describe one coherent endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetSpecError {
    /// The default graph names an IRI that is not a registered data graph.
    DefaultGraphNotADataGraph { iri: String },
    /// One IRI was registered with two different meanings (a data graph and a
    /// schema graph, or two regimes, or two partitions).
    IriRegisteredTwice {
        iri: String,
        first: String,
        second: String,
    },
}

impl fmt::Display for DatasetSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefaultGraphNotADataGraph { iri } => write!(
                f,
                "the default graph <{iri}> is not a registered data graph"
            ),
            Self::IriRegisteredTwice { iri, first, second } => write!(
                f,
                "<{iri}> is registered as {first} and as {second}; a graph has one meaning"
            ),
        }
    }
}

impl std::error::Error for DatasetSpecError {}

fn describe(regime: TypeRegime) -> &'static str {
    match regime {
        TypeRegime::Asserted => "asserted",
        TypeRegime::IsA => "is_a",
        TypeRegime::IsAAndMixins => "is_a+mixins",
    }
}

impl DatasetSpec {
    /// A dataset whose default graph carries `default_regime` and which
    /// serves no named graph.
    pub fn new(default_regime: TypeRegime) -> Self {
        Self {
            default_regime,
            data_graphs: BTreeMap::new(),
            default_graph: None,
            partition_graphs: BTreeMap::new(),
            schema_graphs: BTreeMap::new(),
            conflicts: Vec::new(),
        }
    }

    /// The dataset every endpoint was before graphs had regimes: an asserted
    /// default graph and no named graph. Stated by name where a caller wants
    /// it, because there is deliberately no [`Default`]: a dataset is
    /// something a deployment decides, and one that is defaulted by accident
    /// plans a different endpoint's query.
    pub fn asserted() -> Self {
        Self::new(TypeRegime::Asserted)
    }

    /// Serve the golden records as a named graph under `regime`.
    pub fn with_data_graph(mut self, iri: impl Into<String>, regime: TypeRegime) -> Self {
        let iri = iri.into();
        if let Some(seen) = self.schema_graphs.get(&iri) {
            self.conflicts.push(DatasetSpecError::IriRegisteredTwice {
                first: format!("a schema graph ({})", describe(*seen)),
                second: format!("a data graph ({})", describe(regime)),
                iri,
            });
            return self;
        }
        match self.data_graphs.get(&iri) {
            Some(seen) if *seen != regime => {
                self.conflicts.push(DatasetSpecError::IriRegisteredTwice {
                    first: format!("a data graph ({})", describe(*seen)),
                    second: format!("a data graph ({})", describe(regime)),
                    iri,
                });
            }
            _ => {
                self.data_graphs.insert(iri, regime);
            }
        }
        self
    }

    /// Make the default graph one of the data graphs: its regime becomes the
    /// default regime. The IRI must be registered with [`Self::with_data_graph`]
    /// (checked by [`Self::validate`], so the order of the calls is free).
    pub fn with_default_graph(mut self, iri: impl Into<String>) -> Self {
        self.default_graph = Some(iri.into());
        self
    }

    /// The graph holding one partition of the type triples.
    pub fn with_partition_graph(
        mut self,
        partition: TypePartition,
        iri: impl Into<String>,
    ) -> Self {
        let iri = iri.into();
        match self.partition_graphs.get(&partition) {
            Some(seen) if *seen != iri => {
                self.conflicts.push(DatasetSpecError::IriRegisteredTwice {
                    first: format!("the {partition:?} partition graph <{seen}>"),
                    second: format!("the {partition:?} partition graph <{iri}>"),
                    iri,
                });
            }
            _ => {
                self.partition_graphs.insert(partition, iri);
            }
        }
        self
    }

    /// Serve the datamodel as a named graph whose subclass edges are published
    /// under `regime`.
    pub fn with_schema_graph(mut self, iri: impl Into<String>, regime: TypeRegime) -> Self {
        let iri = iri.into();
        if let Some(seen) = self.data_graphs.get(&iri) {
            self.conflicts.push(DatasetSpecError::IriRegisteredTwice {
                first: format!("a data graph ({})", describe(*seen)),
                second: format!("a schema graph ({})", describe(regime)),
                iri,
            });
            return self;
        }
        match self.schema_graphs.get(&iri) {
            Some(seen) if *seen != regime => {
                self.conflicts.push(DatasetSpecError::IriRegisteredTwice {
                    first: format!("a schema graph ({})", describe(*seen)),
                    second: format!("a schema graph ({})", describe(regime)),
                    iri,
                });
            }
            _ => {
                self.schema_graphs.insert(iri, regime);
            }
        }
        self
    }

    /// Whether the dataset describes one coherent endpoint. Called at every
    /// entry point that takes a dataset.
    pub fn validate(&self) -> Result<(), DatasetSpecError> {
        if let Some(conflict) = self.conflicts.first() {
            return Err(conflict.clone());
        }
        if let Some(iri) = &self.default_graph
            && !self.data_graphs.contains_key(iri)
        {
            return Err(DatasetSpecError::DefaultGraphNotADataGraph { iri: iri.clone() });
        }
        let mut partitions: BTreeMap<&str, TypePartition> = BTreeMap::new();
        for (partition, iri) in &self.partition_graphs {
            if let Some(regime) = self.schema_graphs.get(iri) {
                return Err(DatasetSpecError::IriRegisteredTwice {
                    iri: iri.clone(),
                    first: format!("a schema graph ({})", describe(*regime)),
                    second: format!("the {partition:?} partition graph"),
                });
            }
            if let Some(seen) = partitions.insert(iri, *partition) {
                return Err(DatasetSpecError::IriRegisteredTwice {
                    iri: iri.clone(),
                    first: format!("the {seen:?} partition graph"),
                    second: format!("the {partition:?} partition graph"),
                });
            }
        }
        Ok(())
    }

    /// The regime a triple pattern outside any `GRAPH` is matched under when
    /// the query names no `FROM`: the default graph's.
    pub fn default_regime(&self) -> TypeRegime {
        self.default_graph
            .as_ref()
            .and_then(|iri| self.data_graphs.get(iri))
            .copied()
            .unwrap_or(self.default_regime)
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

    /// The graph that holds partition `p`, if the dataset names one.
    pub fn partition_graph(&self, p: TypePartition) -> Option<&str> {
        self.partition_graphs.get(&p).map(String::as_str)
    }

    /// The IRI the default graph is declared to be, if it is a named graph.
    pub fn default_graph(&self) -> Option<&str> {
        self.default_graph.as_deref()
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
    /// schema graphs and only them. Once the dataset serves a data graph (or a
    /// partition graph) a graph variable ranges over those too, and it is no
    /// longer a question about the datamodel.
    pub(crate) fn graph_variable_is_schema_only(&self) -> bool {
        self.data_graphs.is_empty() && self.partition_graphs.is_empty()
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

    /// The regime a pattern matched against exactly the graphs `iris` is
    /// matched under: `Some` when every one is a data graph and the union of
    /// their regimes' partitions is exactly one regime's partitions
    /// ([`TypeRegime::ALL`]), `None` for anything else -- a schema graph, an
    /// unknown IRI, an empty set, or a union no regime stands for.
    ///
    /// `Asserted` together with `IsA` is `IsA` (the second contains the
    /// first); `IsA` with `IsAAndMixins` is `IsAAndMixins`.
    pub(crate) fn regime_of_graph_set(&self, iris: &[&str]) -> Option<TypeRegime> {
        let mut union: Vec<TypePartition> = Vec::new();
        for iri in iris {
            for partition in self.data_regime(iri)?.partitions() {
                if !union.contains(partition) {
                    union.push(*partition);
                }
            }
        }
        if union.is_empty() {
            return None;
        }
        union.sort();
        TypeRegime::ALL.into_iter().find(|regime| {
            let mut own: Vec<TypePartition> = regime.partitions().to_vec();
            own.sort();
            own == union
        })
    }

    /// The widest regime any graph a `GRAPH ?g` can bind to carries: what a
    /// scan must read so that no graph's records are missed.
    pub(crate) fn widest_data_regime(&self) -> TypeRegime {
        let mut widest = self.default_regime();
        for (_, regime) in self.data_graphs() {
            if regime.partitions().len() > widest.partitions().len() {
                widest = regime;
            }
        }
        widest
    }
}

/// What one graph holds, as far as the planner can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphReads {
    /// The golden records, under this regime. The SQL route reads them.
    Data(TypeRegime),
    /// The datamodel. Only the engine, or a schema-only region, reads it.
    Schema,
    /// Nothing: the empty graph. The default graph of a query that writes
    /// `FROM NAMED` and no `FROM` (SPARQL 1.1 13.2), which no triple pattern
    /// matches in.
    Empty,
    /// Anything else: a graph the endpoint does not serve, or a set of graphs
    /// no single regime stands for. Only the engine can say what it holds.
    Other,
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
    from: Vec<String>,
    /// Whether the query wrote a `FROM` / `FROM NAMED` clause at all.
    clause_written: bool,
    /// The graphs a `GRAPH` clause can reach, when the query wrote a dataset
    /// clause: exactly its `FROM NAMED` list, empty for a `FROM` alone.
    from_named: Option<Vec<String>>,
}

impl<'a> QueryGraphs<'a> {
    /// The view of a query that writes `clause` (its `FROM` / `FROM NAMED`),
    /// if it writes one.
    pub(crate) fn new(
        dataset: &'a DatasetSpec,
        clause: Option<&spargebra::algebra::QueryDataset>,
    ) -> Self {
        Self {
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
        }
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
            return GraphReads::Data(self.dataset.default_regime());
        }
        let iris: Vec<&str> = self.from.iter().map(String::as_str).collect();
        if let Some(regime) = self.dataset.regime_of_graph_set(&iris) {
            GraphReads::Data(regime)
        } else if iris.iter().all(|iri| self.dataset.is_schema_graph(iri)) {
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
        if let Some(regime) = self.dataset.data_regime(iri) {
            GraphReads::Data(regime)
        } else if self.dataset.is_schema_graph(iri) {
            GraphReads::Schema
        } else {
            GraphReads::Other
        }
    }

    /// The regime to fetch under for a `GRAPH ?g`, which can bind any data
    /// graph: the widest regime among them, so no graph's records are missed.
    pub(crate) fn variable_graph_regime(&self) -> TypeRegime {
        self.dataset.widest_data_regime()
    }
}

/// The dataset a test that is not about datasets plans under, stated
/// explicitly rather than defaulted: an asserted default graph and no named
/// graph. Borrowed for as long as a test's rules need it.
#[cfg(test)]
pub(crate) fn default_dataset() -> &'static DatasetSpec {
    static DEFAULT: std::sync::OnceLock<DatasetSpec> = std::sync::OnceLock::new();
    DEFAULT.get_or_init(|| DatasetSpec::new(TypeRegime::Asserted))
}

/// An asserted dataset that serves one schema graph, `iri`.
#[cfg(test)]
pub(crate) fn schema_dataset(iri: &str) -> DatasetSpec {
    DatasetSpec::new(TypeRegime::Asserted).with_schema_graph(iri, TypeRegime::Asserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "urn:graph:asserted";
    const I: &str = "urn:graph:inferred";
    const M: &str = "urn:graph:mixins";
    const S: &str = "urn:graph:schema";

    fn served() -> DatasetSpec {
        DatasetSpec::new(TypeRegime::Asserted)
            .with_data_graph(A, TypeRegime::Asserted)
            .with_data_graph(I, TypeRegime::IsA)
            .with_data_graph(M, TypeRegime::IsAAndMixins)
            .with_schema_graph(S, TypeRegime::IsA)
    }

    #[test]
    fn the_default_dataset_is_the_asserted_one_with_no_graphs() {
        let dataset = DatasetSpec::asserted();
        assert_eq!(dataset.validate(), Ok(()));
        assert_eq!(dataset.default_regime(), TypeRegime::Asserted);
        assert!(!dataset.has_schema_graph());
        assert_eq!(dataset.data_regime(A), None);
    }

    #[test]
    fn a_dataset_answers_for_each_graph_it_serves() {
        let dataset = served()
            .with_default_graph(I)
            .with_partition_graph(TypePartition::Own, A);
        assert_eq!(dataset.validate(), Ok(()));
        assert_eq!(dataset.default_regime(), TypeRegime::IsA);
        assert_eq!(dataset.data_regime(M), Some(TypeRegime::IsAAndMixins));
        assert_eq!(dataset.schema_regime(S), Some(TypeRegime::IsA));
        assert_eq!(dataset.data_regime(S), None);
        assert_eq!(dataset.schema_regime(A), None);
        assert_eq!(dataset.partition_graph(TypePartition::Own), Some(A));
        assert_eq!(dataset.partition_graph(TypePartition::Mixin), None);
    }

    #[test]
    fn a_dataset_that_contradicts_itself_is_refused_by_name() {
        let orphan = DatasetSpec::new(TypeRegime::Asserted).with_default_graph(A);
        assert_eq!(
            orphan.validate(),
            Err(DatasetSpecError::DefaultGraphNotADataGraph { iri: A.into() })
        );
        // The order of the calls does not matter.
        let ordered = DatasetSpec::new(TypeRegime::Asserted)
            .with_default_graph(A)
            .with_data_graph(A, TypeRegime::IsA);
        assert_eq!(ordered.validate(), Ok(()));
        assert_eq!(ordered.default_regime(), TypeRegime::IsA);

        for twice in [
            served().with_data_graph(A, TypeRegime::IsA),
            served().with_schema_graph(A, TypeRegime::Asserted),
            served().with_data_graph(S, TypeRegime::IsA),
            served().with_schema_graph(S, TypeRegime::Asserted),
            served()
                .with_partition_graph(TypePartition::Own, "urn:x")
                .with_partition_graph(TypePartition::Own, "urn:y"),
            served()
                .with_partition_graph(TypePartition::Own, "urn:x")
                .with_partition_graph(TypePartition::IsA, "urn:x"),
            served().with_partition_graph(TypePartition::Own, S),
        ] {
            assert!(
                matches!(
                    twice.validate(),
                    Err(DatasetSpecError::IriRegisteredTwice { .. })
                ),
                "{twice:?}"
            );
        }
        // The same registration twice is no contradiction.
        assert_eq!(
            served().with_data_graph(A, TypeRegime::Asserted).validate(),
            Ok(())
        );
    }

    #[test]
    fn a_set_of_graphs_is_the_regime_whose_partitions_it_unions_to() {
        let dataset = served();
        let regime = |iris: &[&str]| dataset.regime_of_graph_set(iris);
        assert_eq!(regime(&[A]), Some(TypeRegime::Asserted));
        assert_eq!(regime(&[A, I]), Some(TypeRegime::IsA));
        assert_eq!(regime(&[I, A]), Some(TypeRegime::IsA));
        assert_eq!(regime(&[I, M]), Some(TypeRegime::IsAAndMixins));
        assert_eq!(regime(&[A, I, M]), Some(TypeRegime::IsAAndMixins));
        // A schema graph or an unknown graph is no data: no regime.
        assert_eq!(regime(&[A, S]), None);
        assert_eq!(regime(&[S]), None);
        assert_eq!(regime(&["urn:unknown"]), None);
        assert_eq!(regime(&[]), None);
    }

    #[test]
    fn a_graph_variable_reads_the_widest_regime_served() {
        assert_eq!(served().widest_data_regime(), TypeRegime::IsAAndMixins);
        assert_eq!(
            DatasetSpec::asserted().widest_data_regime(),
            TypeRegime::Asserted
        );
    }
}

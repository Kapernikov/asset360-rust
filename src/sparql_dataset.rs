//! The dataset a SPARQL endpoint serves: which named graphs exist, what each
//! one holds, and what the default graph means.
//!
//! One value, passed to every entry point that plans or evaluates a query, so
//! no call can forget it. The endpoint serves
//!
//! * **data graphs** -- named graphs over the golden records, each under a
//!   [`TypeRegime`] (which `rdf:type` triples its records carry),
//! * a **default graph**, which is either a regime or one of the data graphs
//!   ([`DefaultGraph`]), and
//! * **schema graphs** -- the datamodel itself, one per regime its subclass
//!   edges are published under.
//!
//! The planner reads it to decide which regime a triple pattern is matched
//! under (and so which classes a scan stands for); the executor reads it to
//! decide which schema graphs a query can observe. Nothing here knows SPARQL.

use std::collections::BTreeMap;
use std::fmt;

use linkml_runtime::schema_rdf::{TypePartition, TypeRegime};

/// What the default graph is: stated once, in one of two ways.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DefaultGraph {
    /// The records, written under this regime, with no graph name of their own.
    Regime(TypeRegime),
    /// One of the dataset's data graphs (registered with
    /// [`DatasetSpecBuilder::with_data_graph`]): the default graph *is* that
    /// graph, under its regime.
    DataGraph(String),
}

impl From<TypeRegime> for DefaultGraph {
    fn from(regime: TypeRegime) -> Self {
        Self::Regime(regime)
    }
}

/// Which named graphs the endpoint serves and what the default graph means.
///
/// Always valid: the only way to make one is [`DatasetSpecBuilder::build`]
/// (through [`DatasetSpec::builder`]), which refuses a dataset that contradicts
/// itself, so an entry point that takes a `DatasetSpec` has nothing to check.
/// [`Self::asserted`] is the dataset with an asserted default graph, no named
/// graphs and no schema graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetSpec {
    default: DefaultGraph,
    data_graphs: BTreeMap<String, TypeRegime>,
    schema_graphs: BTreeMap<String, TypeRegime>,
}

/// What a graph IRI was registered as. Names the two sides of a
/// [`DatasetConflict::GraphRegisteredTwice`], so the error says what each
/// registration claimed rather than quoting a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GraphRole {
    /// A named graph over the golden records, under this regime.
    DataGraph(TypeRegime),
    /// The datamodel, its subclass edges published under this regime.
    SchemaGraph(TypeRegime),
}

impl fmt::Display for GraphRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataGraph(regime) => write!(f, "a data graph ({regime})"),
            Self::SchemaGraph(regime) => write!(f, "a schema graph ({regime})"),
        }
    }
}

/// One way a dataset contradicts itself.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DatasetConflict {
    /// The default graph names an IRI that is not a registered data graph.
    DefaultGraphNotADataGraph { iri: String },
    /// One IRI was registered with two different meanings (a data graph and a
    /// schema graph, or two regimes).
    GraphRegisteredTwice {
        iri: String,
        first: GraphRole,
        second: GraphRole,
    },
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
            Self::DefaultGraphNotADataGraph { iri } => write!(
                f,
                "the default graph <{iri}> is not a registered data graph"
            ),
            Self::GraphRegisteredTwice { iri, first, second } => write!(
                f,
                "<{iri}> is registered as {first} and as {second}; a graph has one meaning"
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

/// A [`DatasetSpec`] under construction: registrations in any order, checked
/// together by [`Self::build`].
///
/// A registration that contradicts an earlier one is not refused on the spot
/// (a builder method cannot fail, and the order of the calls is free); it is
/// remembered, and `build` reports it together with every other conflict.
#[derive(Debug, Clone)]
#[must_use = "a builder does nothing until `build()` is called"]
pub struct DatasetSpecBuilder {
    spec: DatasetSpec,
    /// Registrations that contradicted an earlier one.
    conflicts: Vec<DatasetConflict>,
}

impl DatasetSpec {
    /// Start a dataset with this default graph (a regime, or one of the data
    /// graphs) that serves no named graph, until the `with_*` methods say
    /// otherwise. The one place the default graph is stated.
    pub fn builder(default: impl Into<DefaultGraph>) -> DatasetSpecBuilder {
        DatasetSpecBuilder {
            spec: DatasetSpec {
                default: default.into(),
                data_graphs: BTreeMap::new(),
                schema_graphs: BTreeMap::new(),
            },
            conflicts: Vec::new(),
        }
    }

    /// An asserted default graph and no named graph. Stated by name where a
    /// caller wants it, because there is deliberately no [`Default`]: a dataset
    /// is something a deployment decides, and one that is defaulted by
    /// accident plans a different endpoint's query.
    pub fn asserted() -> Self {
        Self {
            default: DefaultGraph::Regime(TypeRegime::ASSERTED),
            data_graphs: BTreeMap::new(),
            schema_graphs: BTreeMap::new(),
        }
    }
}

impl DatasetSpecBuilder {
    /// Serve the golden records as a named graph under `regime`.
    pub fn with_data_graph(mut self, iri: impl Into<String>, regime: TypeRegime) -> Self {
        let iri = iri.into();
        if let Some(seen) = self.spec.schema_graphs.get(&iri) {
            self.conflicts.push(DatasetConflict::GraphRegisteredTwice {
                first: GraphRole::SchemaGraph(*seen),
                second: GraphRole::DataGraph(regime),
                iri,
            });
            return self;
        }
        match self.spec.data_graphs.get(&iri) {
            Some(seen) if *seen != regime => {
                self.conflicts.push(DatasetConflict::GraphRegisteredTwice {
                    first: GraphRole::DataGraph(*seen),
                    second: GraphRole::DataGraph(regime),
                    iri,
                });
            }
            _ => {
                self.spec.data_graphs.insert(iri, regime);
            }
        }
        self
    }

    /// Serve the datamodel as a named graph whose subclass edges are published
    /// under `regime`.
    pub fn with_schema_graph(mut self, iri: impl Into<String>, regime: TypeRegime) -> Self {
        let iri = iri.into();
        if let Some(seen) = self.spec.data_graphs.get(&iri) {
            self.conflicts.push(DatasetConflict::GraphRegisteredTwice {
                first: GraphRole::DataGraph(*seen),
                second: GraphRole::SchemaGraph(regime),
                iri,
            });
            return self;
        }
        match self.spec.schema_graphs.get(&iri) {
            Some(seen) if *seen != regime => {
                self.conflicts.push(DatasetConflict::GraphRegisteredTwice {
                    first: GraphRole::SchemaGraph(*seen),
                    second: GraphRole::SchemaGraph(regime),
                    iri,
                });
            }
            _ => {
                self.spec.schema_graphs.insert(iri, regime);
            }
        }
        self
    }

    /// The dataset, if it describes one coherent endpoint; otherwise **every**
    /// way it does not.
    pub fn build(self) -> Result<DatasetSpec, DatasetSpecError> {
        let Self {
            spec,
            mut conflicts,
        } = self;
        match &spec.default {
            DefaultGraph::DataGraph(iri) if !spec.data_graphs.contains_key(iri) => {
                conflicts.push(DatasetConflict::DefaultGraphNotADataGraph { iri: iri.clone() });
            }
            DefaultGraph::Regime(regime) if !regime.contains(TypePartition::Own) => {
                conflicts.push(DatasetConflict::DefaultRegimeWithoutOwn { regime: *regime });
            }
            _ => {}
        }
        for (iri, regime) in &spec.data_graphs {
            if !regime.contains(TypePartition::Own) {
                conflicts.push(DatasetConflict::DataGraphWithoutOwn {
                    iri: iri.clone(),
                    regime: *regime,
                });
            }
        }
        if conflicts.is_empty() {
            Ok(spec)
        } else {
            Err(DatasetSpecError { conflicts })
        }
    }
}

impl DatasetSpec {
    /// How the default graph was stated.
    pub fn default_graph_spec(&self) -> &DefaultGraph {
        &self.default
    }

    /// The regime a triple pattern outside any `GRAPH` is matched under when
    /// the query names no `FROM`: the default graph's.
    pub fn default_regime(&self) -> TypeRegime {
        match &self.default {
            DefaultGraph::Regime(regime) => *regime,
            DefaultGraph::DataGraph(iri) => *self
                .data_graphs
                .get(iri)
                .expect("build() checked that the default graph is a data graph"),
        }
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

    /// The IRI the default graph is declared to be, if it is a named graph.
    pub fn default_graph(&self) -> Option<&str> {
        match &self.default {
            DefaultGraph::DataGraph(iri) => Some(iri),
            DefaultGraph::Regime(_) => None,
        }
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

    /// The regime the data graphs `iris` share: `Some` when every one is a
    /// data graph and they all carry one regime, `None` for anything else -- a
    /// schema graph, an unknown IRI, data graphs of different regimes, or no
    /// graph. A query that reads graphs of different regimes is refused before
    /// it gets here ([`crate::sparql_graph_clauses::single_data_regime`]), so
    /// the planner never has to say what a mixture means.
    pub(crate) fn regime_of_graph_set(&self, iris: &[&str]) -> Option<TypeRegime> {
        let mut shared = None;
        for iri in iris {
            let regime = self.data_regime(iri)?;
            if *shared.get_or_insert(regime) != regime {
                return None;
            }
        }
        shared
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
    /// The golden records, under this regime. The SQL route reads them.
    Data(TypeRegime),
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
    /// The regime the records are read under: `Some` for [`Self::Data`] and
    /// for nothing else. What a rule about type triples asks, since only the
    /// data graph holds records a scan could read.
    pub fn data_regime(&self) -> Option<TypeRegime> {
        match self {
            Self::Data(regime) => Some(*regime),
            _ => None,
        }
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
    /// ([`crate::sparql_graph_clauses::single_data_regime`]).
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
    /// than one data regime (see
    /// [`crate::sparql_graph_clauses::single_data_regime`]).
    pub(crate) fn new(
        dataset: &'a DatasetSpec,
        query: &spargebra::Query,
    ) -> Result<Self, crate::sparql_graph_clauses::CrossDataGraph> {
        let regime = crate::sparql_graph_clauses::single_data_regime(query, dataset)?;
        Ok(Self {
            regime,
            ..Self::view(dataset, crate::sparql_scoper::dataset_of(query))
        })
    }

    /// The view of a query that writes `clause` (its `FROM` / `FROM NAMED`),
    /// if it writes one, before its regime is known: what
    /// [`crate::sparql_graph_clauses::single_data_regime`] reads the graphs
    /// through. The regime is the dataset's default until [`Self::new`] says.
    pub(crate) fn view(
        dataset: &'a DatasetSpec,
        clause: Option<&spargebra::algebra::QueryDataset>,
    ) -> Self {
        Self {
            regime: dataset.default_regime(),
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

    /// The one regime the query reads its records under.
    pub(crate) fn regime(&self) -> TypeRegime {
        self.regime
    }
}

/// The dataset a test that is not about datasets plans under, stated
/// explicitly rather than defaulted: an asserted default graph and no named
/// graph. Borrowed for as long as a test's rules need it.
#[cfg(test)]
pub(crate) fn default_dataset() -> &'static DatasetSpec {
    static DEFAULT: std::sync::OnceLock<DatasetSpec> = std::sync::OnceLock::new();
    DEFAULT.get_or_init(DatasetSpec::asserted)
}

/// An asserted dataset that serves one schema graph, `iri`.
#[cfg(test)]
pub(crate) fn schema_dataset(iri: &str) -> DatasetSpec {
    DatasetSpec::builder(TypeRegime::ASSERTED)
        .with_schema_graph(iri, TypeRegime::ASSERTED)
        .build()
        .expect("one schema graph is a valid dataset")
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "urn:graph:asserted";
    const I: &str = "urn:graph:inferred";
    const M: &str = "urn:graph:mixins";
    const S: &str = "urn:graph:schema";

    fn served_with(default: impl Into<DefaultGraph>) -> DatasetSpecBuilder {
        DatasetSpec::builder(default)
            .with_data_graph(A, TypeRegime::ASSERTED)
            .with_data_graph(I, TypeRegime::IS_A)
            .with_data_graph(M, TypeRegime::IS_A_AND_MIXINS)
            .with_schema_graph(S, TypeRegime::IS_A)
    }

    fn served_builder() -> DatasetSpecBuilder {
        served_with(TypeRegime::ASSERTED)
    }

    fn served() -> DatasetSpec {
        served_builder().build().expect("a valid dataset")
    }

    #[test]
    fn the_asserted_dataset_is_the_asserted_one_with_no_graphs() {
        let dataset = DatasetSpec::asserted();
        assert_eq!(
            DatasetSpec::builder(TypeRegime::ASSERTED).build(),
            Ok(dataset.clone())
        );
        assert_eq!(dataset.default_regime(), TypeRegime::ASSERTED);
        assert_eq!(dataset.default_graph(), None);
        assert!(!dataset.has_schema_graph());
        assert_eq!(dataset.data_regime(A), None);
    }

    #[test]
    fn a_dataset_answers_for_each_graph_it_serves() {
        let dataset = served_with(DefaultGraph::DataGraph(I.into()))
            .build()
            .expect("a valid dataset");
        assert_eq!(dataset.default_regime(), TypeRegime::IS_A);
        assert_eq!(dataset.default_graph(), Some(I));
        assert_eq!(
            dataset.default_graph_spec(),
            &DefaultGraph::DataGraph(I.into())
        );
        assert_eq!(dataset.data_regime(M), Some(TypeRegime::IS_A_AND_MIXINS));
        assert_eq!(dataset.schema_regime(S), Some(TypeRegime::IS_A));
        assert_eq!(dataset.data_regime(S), None);
        assert_eq!(dataset.schema_regime(A), None);
    }

    fn conflicts_of(builder: DatasetSpecBuilder) -> Vec<DatasetConflict> {
        builder
            .build()
            .expect_err("the dataset contradicts itself")
            .conflicts()
            .to_vec()
    }

    #[test]
    fn a_dataset_that_contradicts_itself_is_refused_by_name() {
        let orphan = DatasetSpec::builder(DefaultGraph::DataGraph(A.into()));
        assert_eq!(
            conflicts_of(orphan),
            vec![DatasetConflict::DefaultGraphNotADataGraph { iri: A.into() }]
        );
        // The order of the calls does not matter.
        let ordered = DatasetSpec::builder(DefaultGraph::DataGraph(A.into()))
            .with_data_graph(A, TypeRegime::IS_A)
            .build()
            .expect("a valid dataset");
        assert_eq!(ordered.default_regime(), TypeRegime::IS_A);

        let twice = |iri: &str, first: GraphRole, second: GraphRole| {
            DatasetConflict::GraphRegisteredTwice {
                iri: iri.into(),
                first,
                second,
            }
        };
        let data = GraphRole::DataGraph;
        let schema = GraphRole::SchemaGraph;
        for (builder, expected) in [
            (
                served_builder().with_data_graph(A, TypeRegime::IS_A),
                twice(A, data(TypeRegime::ASSERTED), data(TypeRegime::IS_A)),
            ),
            (
                served_builder().with_schema_graph(A, TypeRegime::ASSERTED),
                twice(A, data(TypeRegime::ASSERTED), schema(TypeRegime::ASSERTED)),
            ),
            (
                served_builder().with_data_graph(S, TypeRegime::IS_A),
                twice(S, schema(TypeRegime::IS_A), data(TypeRegime::IS_A)),
            ),
            (
                served_builder().with_schema_graph(S, TypeRegime::ASSERTED),
                twice(S, schema(TypeRegime::IS_A), schema(TypeRegime::ASSERTED)),
            ),
        ] {
            assert_eq!(conflicts_of(builder), vec![expected]);
        }
        // The same registration twice is no contradiction.
        assert!(
            served_builder()
                .with_data_graph(A, TypeRegime::ASSERTED)
                .build()
                .is_ok()
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
                conflicts_of(served_builder().with_data_graph("urn:graph:odd", regime)),
                vec![DatasetConflict::DataGraphWithoutOwn {
                    iri: "urn:graph:odd".into(),
                    regime
                }]
            );
            assert_eq!(
                conflicts_of(DatasetSpec::builder(regime)),
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
        let error = DatasetSpec::builder(DefaultGraph::DataGraph("urn:graph:nowhere".into()))
            .with_data_graph(A, TypeRegime::ASSERTED)
            .with_data_graph(A, TypeRegime::IS_A)
            .with_schema_graph(S, TypeRegime::IS_A)
            .with_data_graph(S, TypeRegime::IS_A)
            .build()
            .expect_err("three conflicts");
        assert_eq!(error.conflicts().len(), 3, "{error}");
        let text = error.to_string();
        for mention in [
            "urn:graph:asserted",
            "urn:graph:nowhere",
            "urn:graph:schema",
        ] {
            assert!(text.contains(mention), "{text}");
        }
        // Display names a role by its regime, not by its debug spelling.
        assert!(text.contains("a data graph (asserted)"), "{text}");
        assert!(!text.contains("DataGraph"), "{text}");
    }

    #[test]
    fn a_set_of_graphs_has_a_regime_only_when_they_share_one() {
        let dataset = served();
        let regime = |iris: &[&str]| dataset.regime_of_graph_set(iris);
        assert_eq!(regime(&[A]), Some(TypeRegime::ASSERTED));
        assert_eq!(regime(&[A, A]), Some(TypeRegime::ASSERTED));
        assert_eq!(regime(&[I]), Some(TypeRegime::IS_A));
        // Different regimes are a refusal upstream, never a regime here.
        assert_eq!(regime(&[A, I]), None);
        assert_eq!(regime(&[I, M]), None);
        // A schema graph or an unknown graph is no data: no regime.
        assert_eq!(regime(&[A, S]), None);
        assert_eq!(regime(&[S]), None);
        assert_eq!(regime(&["urn:unknown"]), None);
        assert_eq!(regime(&[]), None);
    }
}

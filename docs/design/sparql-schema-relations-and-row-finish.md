# Schema relations in the statement, and an engine that finishes over rows

Status: **proposed, for review** (revision 1). No code yet. Asset360
issue #494 (pepibru GitLab, asset360/consolidator-server), item A.

## Summary

The InfraGIS query over all 20 563 CivilEngineeringAssets fails on every
route today. This document proposes two mechanisms. Both are generic: each
is written for a class of queries, and neither is a rule for this one.

1. **Schema relations lower into the statement.** The planner already turns
   any region that reads only the schema graph into an inline table
   (`PlanOp::Values`). Today that table can narrow a scan and nothing more.
   It never runs in SQL, so every one of them splits the SQL frontier. This
   proposal gives it a lowering, a constant derived table
   `(VALUES …) AS c1(…)`. A statement can then join it, left-join it and
   project its columns. So any query that looks something up in the
   datamodel and filters or labels records by it can stay one statement.
   Enum labels are one such query. Codes, class and slot metadata, and a
   client-written `VALUES` are others.
2. **The engine finishes over the statement's rows, not over records.**
   Today, when SQL cannot answer alone, the statement *fetches records*
   and the engine reloads them as triples (about 86 triples per CEA) and
   re-runs the whole query. The proposal adds a second engine input. SQL
   answers every *data* read and returns solution rows. The engine then
   evaluates only what is left (labels, `BIND`, `ORDER BY`,
   filters) over those rows plus the schema graph. No record is loaded.

Mechanism 1 decides *what SQL can do*. Mechanism 2 decides *what happens to
the rest*. The planner tries them in that order, and a query that neither
covers falls back to today's routes unchanged.

## Context: what the planner does today, and where this query breaks

The pipeline today, in the vocabulary of `sparql-scopes-as-relations.md`:

* `refine` rewrites the plan to a fixpoint. `SchemaGraphMaterialisation`
  (`src/sparql_materialise.rs`) is one of its rules: a subplan that reads
  only the schema graph, has no free variable and yields at most
  `MAX_MATERIALISED_ROWS` (500) rows is evaluated at plan time and replaced
  by a `PlanOp::Values`.
* Each node is placed `Executor::Sql` or `Executor::Engine`. The frontier
  must be a cut (invariant 3: no SQL node above an engine node), and the
  SQL pass is one statement, so the SQL nodes must form **one island**.
* `Refinement` records the outcome. `UsedAlone` means the statement is the
  answer. `Used` means the statement fetches records and the engine
  re-runs the whole query over them. `Fallback` means no statement was
  possible and the scoper's decomposition is the fetch.
* The engine route loads the fetched records into an oxigraph store,
  bounded by `max_triples` (500 000). Both routes cap the answer at
  `MAX_RESULT_ROWS` (10 000; `sparql/limits.py` in the consolidator,
  `ExecuteLimits::max_result_rows` here).

A `PlanOp::Values` is always `Engine`. The two rules that consume one,
`values_becomes_filter` and `values_narrow_the_joined_scan` (#409), turn it
into a *condition on a scan*. They need an inner `Join`, a join variable
the other side binds in every solution and, for the first rule, a relation
that adds no column. A label read fails all three: it sits under an
`OPTIONAL`, it adds a column (`?typeNl`), and the enum slot is optional. So
the `Values` stays an engine node in the middle of SQL nodes, and each one
splits the island.

### The worked example, measured

The query is in issue #494 unchanged: `?asset a asset360:CivilEngineeringAsset`,
51 selected variables, 18 label `OPTIONAL`s of the form
`OPTIONAL { GRAPH schema: { ?x skos:prefLabel ?l . FILTER(lang(?l) = "nl-be") } }`,
one `BIND(STRAFTER(STR(?builtElement), "BuiltElement/4-") AS ?invId)`
inside an `OPTIONAL`, a `COUNT` sub-select, and `ORDER BY ?name`.

Through the endpoint (DEV copy, 20 563 CEAs, asset360-rust v0.10.13):

| scope | assets | elapsed | result |
|---|---|---|---|
| all CEA, as-is | 20 563 | 0.60 s | 422 `aggregate_not_pushable` |
| all CEA, without the `COUNT` sub-select | 20 563 | 44.91 s | 422 `limit_exceeded`: 1 762 160 triples > 500 000 |
| `ceType:Tunnel` | 130 | 2.75 s | 200, 130 rows |
| `ceType:Rotswand` | 478 | 5.27 s | 200, 478 rows |
| `ceType:DOS` | 604 | 8.50 s | 200, 604 rows |
| `ceType:OVB` | 1 938 | 38.22 s | 422 `evaluation_time_exceeded` |
| `ceType:Duiker` | 4 980 | 44.65 s | 422 `evaluation_time_exceeded` |

Stage by stage on the engine route (DEV):

| scope | plan | fetch | engine | of which loading records |
|---|---|---|---|---|
| Tunnel (130 assets, 605 records) | 1.6 s | 1.7 s | 2.5 s | ≈1.4 s |
| Rotswand (478 assets, 1 020 records, 60 264 triples) | 1.5 s | 3.0 s | 7.8 s | 2.4 s |

The planner on the all-CEA query, with constructs removed one at a time
(`plan_query_refined`, review-1041):

| variant | refinement | islands | SQL answers alone |
|---|---|---|---|
| as-is | fallback | 21 | no |
| − 18 label `OPTIONAL`s | fallback | 13 | no |
| − `BIND` | fallback | 11 | no |
| − nested Station/Bundle `OPTIONAL`s | used | 1 | no: `order ASC(?name)` unclaimed |
| − `ORDER BY` | used | 1 | no: "fetches rows rather than emitting solutions" |
| − superStructure / subStructure | used | 1 | no: same |

Other figures used below:

* The schema graph holds 329 `nl-be` and 314 `fr-be` `skos:prefLabel`s in
  total. A label region therefore materialises well under the 500-row cap.
* Planning takes 0.66 s as-is and 0.16 s without the labels on review-1041
  (1.6 s against 0.3 s on DEV).
* The `COUNT` sub-select alone plans `used_alone`, inside an `OPTIONAL` and
  outside it (#466, PR #49). It is not a gap. The `aggregate_not_pushable`
  in the first row happens because the query as a whole falls back.

So the query has four separate reasons it is not one statement: the labels,
the `BIND`, the nested Station/Bundle join and the `ORDER BY`. On top of
those, the "used" plan's statement fetches records instead of emitting
solutions. Even as one statement it returns 20 563 rows, which is over the
10 000-row cap.

## Scope

In scope:

* **M1.** A lowering for `PlanOp::Values`, whatever produced it, as a
  constant derived table. Inner join, left join and projection of its
  columns. Values whose join column is an enum slot are translated to
  stored codes.
* **M2.** A new engine input: the solutions of the SQL statement instead of
  the records it fetched. The rules that move engine-only operators out of
  an `OPTIONAL`, and `ORDER BY` / `LIMIT` below row-preserving engine
  operators, so that the SQL part is one island at the bottom of the plan.
* The contract bump both need (`PLAN_CONTRACT` 5 → 6) and the consolidator
  changes that go with it (the renderer arm, and the executor branch that
  runs the new pass shape).

Not in scope, each with where it goes:

* **The nested Station/Bundle join.** This is a reference join under an
  `OPTIONAL` nested inside the `hasCoveredSection` unnest. It is a *data*
  read, so neither mechanism helps it. It needs the element-held reference
  widening of op 5 (`sparql-scopes-as-relations.md`, *The widening op 5
  needs for #464*) extended to a nested `OPTIONAL` on the preserved
  element. This query needs it too. It is listed under *Staging* as its own
  step and is not designed here.
* **Several islands answered by several statements** that the engine then
  joins. See open question Q5.
* **Emitting quads directly from a `LinkMLInstance`** (the larger half of
  per-record load cost). That is rust-linkml-core work (see *Where PR #58
  fits*).
* **The 10 000-row cap.** This is a product decision. See Q1.

## M1 — a schema relation is a relation the statement can join

### The operator

A new SQL operator, `Op::Constant`:

```rust
/// An inline table: `(VALUES (…), (…)) AS c{n}(col, …)`.
Constant {
    alias: String,
    columns: Vec<ConstantColumn>,   // one per variable, in row order
    rows: Vec<Vec<Option<String>>>, // SQL text per cell, NULL for UNDEF
}
pub struct ConstantColumn {
    pub var: String,
    /// How the cell text becomes an RDF term. One descriptor per column,
    /// so the column's cells must share one term shape (see below).
    pub descriptor: TermDescriptor,
}
```

`Op::Relation` would not work here. Its body is an operator tree over
records, and a constant has no records to read. A separate kind also means
a renderer built against contract 5 refuses it instead of skipping it,
which is the rule every operator added before it has followed.

A `Constant` enters the statement the way a `Relation` already does:
through `Op::Join` with a column key. That key is `JoinKey::Identity` when
the join variable is a record identity, and a new
`JoinKey::Value { left: ColumnRef, right: ColumnRef }` when it is a slot
value (an enum code, a string). Otherwise it is `JoinKey::Cross`. The join
may be `Inner` or `Left`. Its exported columns are ordinary projection
bindings.

### The rule: `LowerConstantRelation`

It flips a `PlanOp::Values` from `Engine` to `Sql`. Preconditions, each
with the wrong answer it prevents:

* **Every column is uniform.** All non-`UNDEF` cells of a column are the
  same kind of term: all IRIs, or all literals of one datatype and one
  language tag. A column's descriptor is per column, so a mixed column
  would give some cells the wrong type or language. A label column
  filtered to `lang = "nl-be"` is uniform. A mixed column stays an engine
  node, and M2 picks it up.
* **The join variable renders against the other side's column.** This is
  `Expr::to_sql`'s job already. When the other side's column is an enum
  slot, the relation's concept IRIs are mapped to the codes the column
  stores through the slot's `TermDescriptor::enum_map`. A row whose IRI is
  not a permissible value of that enum is **dropped at lowering time**.
  It cannot join, since a stored value outside the enum does not render as
  that concept IRI. This also shrinks a label table from all 329 labels to
  the enum's own values.
* **The join is a join the renderer can state.** An `OPTIONAL { Values }`
  is a `LEFT JOIN … ON <key>`. The key goes in `ON`, never in `WHERE`, for
  the reason `Op::Filter::optional_side` exists.
* **Blank nodes are already excluded** by `materialise`, so a cell is
  always a ground term.

What it claims: the `Values` obligation (`Obligation::Values`) and any
obligation the materialised region discharged. That region's triples and
filters are already folded into the `Values` node's accounting today.

### What this does beyond labels

The rule reads a `Values` node and nothing else, so everything that already
produces one gets it:

* a schema region used to **filter** records under an `OPTIONAL`, or on an
  optional slot, where `values_narrow_the_joined_scan` cannot fire today
  because the other side does not bind the variable in every solution. The
  join now happens in the statement instead of in the engine.
* a schema region that **adds columns**: `skos:notation` codes, a slot's
  annotations, a class's label.
* a client-written `VALUES` block.

The two existing narrowing rules stay. They are cheaper when they apply,
because they turn the table into an `IN` on the scan and allow index use.
`LowerConstantRelation` is what applies when they do not. The narrowing is
still derived where its preconditions hold, because a statement that both
joins the constant and narrows the scan is sound (the join already
implies the narrowing).

A variable class (`?s a ?c` with `?c` from the schema) would also be
"filtering records on the schema", but it is a *scan over a set of
classes*, not a join against a column. See Q6.

## M2 — the engine finishes over the statement's rows

### The pass shape

`EnginePass` gains the input it reads:

```rust
pub enum EngineInput {
    /// Today's: the SQL pass fetched records; load them as triples and
    /// re-run the whole query.
    Records,
    /// New: the SQL pass emitted solutions over `vars`; evaluate the
    /// engine region over them and the schema graph. No record is loaded.
    Solutions { vars: Vec<String> },
}
```

`Refinement` gains `UsedRows(String)`: the statement emits solutions, and
the engine finishes over them. The planner prefers outcomes in this order:
`UsedAlone`, then `UsedRows`, then `Used`, then `Fallback`.

### When the planner chooses it

At plan time, from the plan alone:

1. The SQL nodes form one island, and its root is an ungrouped `Project`
   with bindings or a `Group`. In other words, the statement **emits
   solutions**. Today a `Project` gets bindings only when it is inside the
   frontier. Under M2 the island root gets a synthesised `Project` over the
   variables the engine region reads, built with the same
   `projected_columns` / `scope_columns` machinery an enclosed `OPTIONAL`
   body already uses. This is what fixes "fetches rows rather than
   emitting solutions" in the last two rows of the table.
2. **The engine region reads no instance data.** Every engine node is an
   operator over its inputs (`Extend`, `Filter`, `LeftJoin`, `Join`,
   `Sort`, `Slice`, `Distinct`, `Project`, `Group`), a `Values`, or a read
   inside `GRAPH <schema graph>`. The criterion is the one `schema_only`
   already computes, applied to "everything but the island". If the engine
   region reads anything from the default graph, the statement cannot
   supply it as rows. The plan is then `Used`, exactly as today.

### Getting the SQL part to the bottom: the rewrites

The frontier must still be a cut. With engine-only nodes nested inside
`OPTIONAL`s, they sit between SQL nodes and split the island. Three rewrites
move them up. Each is an equivalence under SPARQL semantics and has a guard.

**R1 — a left join against an engine-only right side moves up.**

    A ⟕ (B ⟕ C)   →   (A ⟕ B) ⟕[BOUND(?j₁) && …] C

`C` is engine-only (a schema region that stayed a `Values` because M1
declined it, or a schema read). `J` is the set of variables `B` and `C`
share. Guards:

* every `?j` in `J` is **definitely bound in `B`** and **not in scope in
  `A`**.
* `C`'s other variables are in scope in neither `A` nor `B`.

The `BOUND` condition is what makes this sound in SPARQL, and it is the
step that is easy to miss. In SQL, `(A ⟕ B) ⟕ C` on a null key matches
nothing. In SPARQL, a solution where `?j` is unbound is *compatible with
every row of `C`*, so without the condition an asset without a `B` match
would pick up every label in the table.

**R2 — an extension over an optional side moves up.**

    A ⟕ Extend(B, ?x, e)   →   Extend(A ⟕ B, ?x, e)

Guards:

* `?x` is not in scope in `A`.
* `e` is **strict** in some variable `?b` that is definitely bound in `B`
  and not in scope in `A`. Strict means `e` errors when `?b` is unbound.
  Every built-in except `BOUND`, `IF`, `COALESCE`, `EXISTS` and the logical
  connectives is strict in each argument. For a row where `B` did not
  match, `e` errors and `?x` stays unbound, which is what the left side
  would have given. For a row where `B` matched, the result is the same.

`BIND(STRAFTER(STR(?builtElement), …) AS ?invId)` qualifies: it is strict
in `?builtElement`, which only the `OPTIONAL` binds.

A left-join *condition* (a `FILTER` directly inside an `OPTIONAL`) does
**not** move up. A failing condition keeps the left row unextended, but
a filter above would drop it. Such a condition must either lower (it
already does, into `ON`) or keep its `OPTIONAL` in the engine. In the
second case the query is `Used`. The label filters are inside the
materialised schema region, so they never reach this case.

**R3 — `ORDER BY` / `LIMIT` / `OFFSET` move below row-preserving engine
operators into SQL.**

    Slice(Sort(E(X), k))   →   E(Slice(Sort(X, k)))

Guards:

* `E` is **row-preserving**: one output row per input row, in input order.
  `Extend` and `Project` always are. A `LeftJoin` against a relation is
  row-preserving when the relation is **functional on the join key** (at
  most one row per key value). The planner checks this at plan time on
  the materialised rows it holds.
* every sort key `k` is bound by `X`.

`Distinct`, `Filter`, `Group` and a non-functional join are not
row-preserving, and `Sort` / `Slice` stay above them in the engine.

These rewrites are ordinary rules in `refine`'s loop. The progress policy
and evidence rules in `sparql-scopes-as-relations.md` apply unchanged, so a
rewrite that makes nothing lower is not kept.

### How the executor runs it

The consolidator executes the statement as it does for `UsedAlone`
(`build_aggregate_sql_from_ops`). It turns the rows into SPARQL terms with
the column descriptors (`to_sparql_results`, unchanged), then calls a new
entry point here:

```rust
pub fn sparql_finish(
    plan: &ExecutionPlan,              // carries the engine region
    solutions: &SparqlSolutions,       // the statement's rows, as terms
    schema_view: &SchemaView,
    limits: ExecuteLimits,
    schema_graph_iri: Option<&str>,
) -> Result<SparqlAnswer, ExecuteError>
```

It writes the engine region back to algebra (`sparql_algebra.rs` already
does this for every `PlanOp`), with the island replaced by a
`GraphPattern::Values` of the rows. It then evaluates that over a store
holding **only the schema graph**. The store has no instance triples, so
the triple ceiling has nothing to count (see below).

The rows could be injected into oxigraph as a solution iterator instead of
an inline table, which would avoid building the `Values`. That is an
implementation choice with no effect on the contract. See Q7.

## How the two compose

For each query, the planner goes through these steps:

1. `SchemaGraphMaterialisation` turns schema regions into `Values`
   (existing behaviour).
2. The narrowing rules (#409) turn `Values` into scan conditions where they
   can (existing behaviour).
3. **M1**: `LowerConstantRelation` flips every remaining `Values` that
   meets its preconditions to `Sql`.
4. **M2**: R1–R3 lift what is still engine-only above the SQL nodes.
5. Placement and outcome: `UsedAlone` if nothing is left for the engine,
   `UsedRows` if what is left reads no instance data, `Used` if it reads
   some, and `Fallback` if no statement is possible.

M1 before M2 because an SQL join is cheaper than an engine join over
10 000 rows. When M1 declines a `Values` (a mixed column, a join key
that does not render), M2 still handles it. So M2 is the general case
and M1 is the optimisation on top of it. A query that M1 alone makes
`UsedAlone` never needs M2.

## Limits: the triple ceiling and the row cap

**`max_triples` (500 000).** Its purpose is to bound the memory of the
store. On the `UsedRows` route, the store holds the schema graph only,
which is fixed and already excluded from the count today. The memory is
in the solution rows instead. Proposal: bound the **bound cells** handed
to `sparql_finish` (rows × bound columns) by the same `max_triples`. A
bound cell costs no more than the triple it would have been on the records
route, and far fewer are handed over, because only the selected columns
cross and not the whole record. That makes `UsedRows` never admit more
data than `Used` would. `sparql_finish` checks the count before
evaluating and refuses with the existing `TripleLimitExceeded` shape,
naming cells. Assumable (Q2).

**`MAX_RESULT_ROWS` (10 000).** The statement is read with
`fetchmany(cap + 1)`, as the `UsedAlone` route already does. Whether that
refusal is right depends on the engine region:

* **row-preserving** (after R3, the common case): statement rows = answer
  rows, so over the cap is over the cap. The query is refused right after
  SQL returns, in about the time the statement takes, not after a
  45-second load.
* **reducing** (`Filter`, `Distinct`, `Group` in the engine): the answer
  may be smaller than the input. The statement is then read with the
  cell budget above as its bound, not the row cap, and the row cap is
  applied to the engine's output as it is today.

The planner knows which case applies, and `EngineInput::Solutions` carries
it as `preserves_rows: bool` so the executor does not have to work it out.

**`MAX_MATERIALISED_ROWS` (500).** Unchanged. A schema region over 500 rows
is not materialised, so it is an engine read. M2 still handles it (the
engine region reads the schema graph, which is allowed), just not M1.

## The worked example, after both

Applying the steps to the all-CEA query:

| construct | today | after |
|---|---|---|
| 18 label `OPTIONAL`s | 8 islands | M1: each becomes a `LEFT JOIN` on a per-enum constant table (codes → one `nl-be` / `fr-be` label). Uniform columns, functional on the key. |
| `BIND(STRAFTER(STR(…)))` in an `OPTIONAL` | 2 islands | M2 R2: lifted above the statement, evaluated by the engine over the rows |
| nested Station/Bundle `OPTIONAL`s | 3 islands | **not covered here**: needs the op 5 widening (Staging step 0) |
| `COUNT` sub-select | lowers (#466) | unchanged |
| `ORDER BY ?name` | unclaimed | M2 R3: moved below `Extend` into SQL (`?name` is a statement column) |
| projection | fetch, not solutions | M2: island root gets a `Project` with bindings |

Outcome: `UsedRows`, with the engine region being one `Extend` for `?invId`
and nothing else. Without a `LIMIT` it still returns 20 563 rows, so it is
refused with `limit_exceeded` / `max_rows` **as soon as the statement
returns 10 001 rows**, instead of after 45 s. With `LIMIT 10000 OFFSET n`
(R3 pushes the slice into SQL), each page is one statement plus an engine
pass over at most 10 000 rows and at most 51 cells each. Whether that fits
the 500 000-cell budget depends on how many of the 51 columns are bound
per asset. That has not been measured yet (Q2).

This is a projection from the plan, not a measurement. The acceptance test
is the query itself on DEV (see *Tests*).

## Where rust PR #58 fits

PR #58 (items B and C) makes the **`Used` route** cheaper. It loads records
in chunks of 64 (299 ms → 83 ms on 2 000 records), and it stops loading
at the triple ceiling instead of after the whole load. That turns the
45-second `limit_exceeded` into a fast one.

This design moves queries *off* that route: an `UsedRows` query loads no
records. PR #58 still matters for every query that stays on `Used`, which
means any query whose engine region reads instance data. That is every
query blocked on the nested Station/Bundle join until Staging step 0, and
every shape R1–R3 decline. The two do not conflict and touch different
code (`sparql_execute`'s load loop, against the planner and a new
`sparql_finish`). PR #58 can merge first on its own, and this PR is based
on `main`, not on #58, for that reason.

## Contract and consolidator changes

`PLAN_CONTRACT` 5 → 6 adds `Op::Constant`, `JoinKey::Value`,
`EngineInput` on `EnginePass` and `Refinement::UsedRows`. A consumer built
against 5 refuses a contract-6 plan, as it must: it would render nothing
for a `Constant` and would load records for a `Solutions` pass.

Consolidator side (asset360/consolidator-server):

* the renderer arm for `Op::Constant` and `JoinKey::Value` in
  `build_aggregate_sql_from_ops`.
* the executor branch for `refinement == "used_rows"`: run the statement,
  apply the row cap or the cell budget per `preserves_rows`, serialise
  with `to_sparql_results`, call `lr.sparql_finish`.
* the contract check raised to 6.

## Tests that would prove it

* **Oracle tests, per rule.** Every shape admitted by `LowerConstantRelation`
  or R1–R3 runs on both the new route and the engine's `Used` route over
  the same records, and the answers must agree (the same discipline
  `UsedAlone` shapes follow). R1 needs the counterexample that breaks it
  without the `BOUND` condition: an asset without the optional match must
  not pick up every label. R2 needs a non-strict `e` (`COALESCE`,
  `BOUND`) that must be declined.
* **A mixed-column `Values`** (IRIs and literals in one column) must stay
  engine-side and still answer through M2.
* **Contract skew**: a contract-5 consumer refuses a contract-6 plan.
* **Acceptance on DEV**: the all-CEA query, unchanged, plans `UsedRows`.
  It is refused on `max_rows` within the statement's time without a
  `LIMIT`, and answers each `LIMIT 10000` page. Report plan, statement and
  finish time, and the cell count per page.

## Staging

0. **The nested reference join (item A3)**: op 5's element-held reference
   extended to an `OPTIONAL` nested under the unnest. Without it this query
   stays `Used`, whatever else lands. It is a planner rule of the existing
   kind, not part of this design.
1. **M1**: `Op::Constant`, `JoinKey::Value`, `LowerConstantRelation` and
   the renderer arm. Useful alone: every query whose only gap is a schema
   lookup becomes `UsedAlone`.
2. **M2**: `EngineInput::Solutions`, `UsedRows`, the synthesised island
   projection, R1–R3, `sparql_finish` and the executor branch.

Steps 1 and 2 are one contract bump and one rust release, following the
one-rust-PR-per-release rule. Step 0 can go in the same release.

## Rollout

After this design is approved, implementation does not wait for an
asset360-rust release. The consolidator-server MR is built against the
unreleased rust branch (the `a360-unreleased-rust` skill in
consolidator-server) in the same pass as the rust PR. Integration is where
the remaining rust bugs show up, and they should be found before the
release. The pin bump follows the release.

## Open questions

Each is marked **blocking** (the design cannot be settled without an
answer) or **assumable** (the design proceeds on the stated assumption
unless the reviewer says otherwise).

* **Q1 — the 10 000-row cap for this query.** *Blocking for acceptance,
  not for the design.* All-CEA returns 20 563 rows. Either the cap moves
  (a product decision, in `sparql/limits.py`) or InfraGIS pages with
  `LIMIT`/`OFFSET`. The design makes paging cheap (R3). It does not decide
  which.
* **Q2 — the cell budget on `UsedRows`.** *Assumable.* Assumption: bound
  cells handed to the engine are counted against `max_triples` (500 000),
  and the count is refused before evaluation. The per-page cell count for
  this query is the first thing to measure. If a 10 000-row page does not
  fit, the fallback is a separate `max_solution_cells` limit, sized from a
  measurement and not guessed.
* **Q3 — scope of R1–R3.** *Assumable.* Assumption: these three rewrites
  and nothing more. Moving a left-join *condition* out of an `OPTIONAL`
  is not equivalent, and stays out. Other engine-only operators found
  nested in data regions go into the ledger as `Used` with the reason,
  and each new rewrite is added only with an oracle test.
* **Q4 — planning time of materialisation.** *Assumable.* The 18 label
  regions cost about 0.5 s of planning (0.66 s against 0.16 s). Assumption:
  most of it is evaluating 36 near-identical schema regions one at a time,
  and M1's implementation memoises a region's relation by its written-back
  query text. This will be measured before and after, and if it is
  something else (#468 remains), it is reported, not assumed.
* **Q5 — several islands.** *Assumable.* Assumption: `UsedRows` requires
  one statement. Several statements joined in the engine would handle more
  shapes, but each island is then bounded only by its own scan, and an
  unbounded island is the 1.76 M-triple problem in another form. Not
  designed until a query needs it.
* **Q6 — a class read from the schema** (`GRAPH schema { ?c rdfs:subClassOf* :X } ?s a ?c`).
  *Assumable.* Assumption: out of scope. It is "filter records on the
  schema", but its lowering is a scan over a set of `asset_type`s, not a
  join against a constant. It would reuse M1's `Values`, but with a
  different consumer rule.
* **Q7 — how the rows enter oxigraph.** *Assumable.* Assumption: as a
  `GraphPattern::Values` substituted for the island. This reuses the
  existing write-back and needs no oxigraph API. If building a 10 000-row
  inline table turns out to be measurably slow, switch to an injected
  solution iterator. The contract does not change either way.

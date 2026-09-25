# Schema relations in the statement, and an engine that finishes over rows

Status: **proposed, for review** (revision 3). No code yet. Asset360
issue #494 (pepibru GitLab, asset360/consolidator-server), item A.

<details><summary>What revision 3 changed: an effects guard on R1, a progress measure instead of the island-count gate, the ordinal through every projection</summary>

The review of revision 2 accepted the revision 1 findings as addressed
and raised two blockers and one ordering detail. Both counterexamples
reproduce on PyOxigraph 0.5.11 (*Appendix*).

| finding | revision 2 | revision 3 | kind |
|---|---|---|---|
| **R1** a volatile condition `f` (`RAND()`) | G1c checked `f`'s inputs and `Opaque`, not its effects. The lift evaluates `f` once per `(a, b, c)` instead of once per `(b, c)` | **G1d**: `f` and every expression inside `C` are effect-free (`evaluates_the_same_out_of_context`), or R1 declines. Regression `r1_volatile_condition` asserts the decline, deterministically | narrowing |
| **island-count gate** discards R3 | "a rewrite that does not reduce the island count is not kept" | the gate is withdrawn. R1–R3 are accepted by a **progress measure** Φ = (islands, pending barriers, query modifiers above the engine), lexicographic, which every application lowers strictly. R3 moves the query's own `Sort`/`Slice` into an existing island (islands 1 → 1, modifiers 2 → 0). The restoring `Sort(?__ord)` has `origin: Ordinal`, counts in no measure and matches no rule | construction |
| **R1, R2** lift over an engine-side `B` | allowed | **G1e / G2e**: `A` and `B` are SQL. Needed for the measure to drop, and a lift that unites no islands makes no statement possible | narrowing |
| **ordinal** through `Project` / `SubSelect∅` | not stated | every `Project` and `SubSelect∅` between the island root and the restoring sort exports `?__ord`, the witness-cleanup projections of R1/R2 included. A rule builds its projections as "scope minus what it drops", never as a list. The transition check `ordinal_reaches_its_sort` verifies this. Only the query's final projection drops it | construction |

The review also asked whether the effects flaw appears in the other rules
that move an expression. **R2**: no, since G2c already required
effect-free. **R3**: no. It moves no expression, only `Sort`/`Slice` with
variable terms (G3b). Each expression of `E` is still evaluated once per
row it keeps, over the same input row (see R3, *Effects*). **M2**: it
moves the region into another evaluator. It is safe because the island
evaluates no expression that is not effect-free, and that is now an
explicit eligibility check (check 5), not an accident of what SQL
lowering supports today. So `NOW()` is evaluated in one evaluation only.

Also new: R1–R3 are kept only when placement reaches `UsedRows`. On
`Used` the engine re-runs the whole query over fetched records, and a
`Slice` pushed into that fetch would drop records its `ORDER BY` needs.

</details>

<details><summary>What revision 2 changed: five soundness blockers, the ordering contract, printouts and scopes</summary>

The review of revision 1 found that five guards admitted plans that are
not equivalent, and gave an Oxigraph counterexample for each. Every one
reproduces on PyOxigraph 0.5.11 (*Appendix: the counterexamples*). For
each finding the design either gives a construction argued sound, or
narrows the rule so that it declines the case. The table says which.

| finding | revision 1 | revision 2 | kind |
|---|---|---|---|
| **R1** `BOUND(?j)` tests the merged mapping | guard `BOUND(?j₁) && …` on the shared variables | a **match witness**: a fresh variable the left join exports iff its right side matched. C cannot bind it, and it covers an empty shared set. Plus two guards revision 1 lacked: on C's variables, and on the inner condition | construction + narrowing |
| **M1** SQL `=` is not SPARQL compatibility | join on a column key, prune IRIs outside the enum | lower only when compatibility **reduces to** SQL equality: every shared variable definitely bound on the other side, no `UNDEF` in a key column, and a key descriptor whose stored-to-term map is invertible. Pruning is justified by the definite binding. Anything else declines | narrowing |
| **R2** strictness does not preserve inputs | `e` strict in a variable B binds | the **witness** replaces strictness. `e` may read no variable that A binds and B does not definitely bind (**input preservation**). `e` must be **effect-free** (the existing `evaluates_the_same_out_of_context`) | construction + narrowing |
| **R3** a functional key is not one compatible mapping | C functional on its concrete key values | a **compatible-mapping proof**: a key set definitely bound on the left, `UNDEF`-free on the right, and unique under `sameTerm` | narrowing |
| **M2** eligibility ignores expressions, write-back is partial | `schema_only` on the region, "write-back covers every `PlanOp`" | an **expression-aware read set**: no `Opaque` (`EXISTS`), no `InClass`, no `SERVICE`, no variable `GRAPH`. The write-back is **built at plan time and carried in the plan**, so an unrepresentable region never reaches `UsedRows`. Revision 1's claim that write-back is total is withdrawn | narrowing |
| **P2** ordering across the boundary | "VALUES vs iterator is contract-neutral" | an explicit **ordinal**. The executor numbers the statement's rows `?__ord` in the order they arrive. The finish query ends in `ORDER BY ?__ord`. No engine operator is trusted to preserve order | construction |
| printouts | none | *Plan printouts*: today's plans (real) and the proposed ones (projected), with witnesses, keys, the ordinal and refusal reasons | — |
| scopes | not addressed | *Scopes, barriers and renaming*: which barrier each rule crosses and which it refuses, how the obligation ledger and the producer invariant follow the edit, and alpha-renaming | — |
| regressions | "oracle tests, per rule" | *Tests*: one named oracle regression per counterexample, with before/after bag equality, and sequence checks where the query is ordered | — |

Nothing in the review is contested. Two statements of revision 1 were
wrong in their own right, and are corrected where they stood.
`sparql_algebra.rs` does not write back every `PlanOp`: `Service`,
`Opaque` and `Slot` have no algebra. And `schema_only` is not an
expression-aware read set: it walks operator inputs only.

</details>

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
   project its columns, **where SPARQL compatibility on the join reduces
   to SQL equality** (see *The precondition*). So a query that looks
   something up in the datamodel and filters or labels records by it can
   stay one statement. Enum labels are one such query. Codes, class and
   slot metadata, and a client-written `VALUES` are others.
2. **The engine finishes over the statement's rows, not over records.**
   Today, when SQL cannot answer alone, the statement *fetches records*
   and the engine reloads them as triples (about 86 triples per CEA) and
   re-runs the whole query. The proposal adds a second engine input. SQL
   answers every *data* read and returns solution rows. The engine then
   evaluates only what is left (labels, `BIND`, filters) over those rows
   plus the schema graph. No record is loaded. The part that is left is
   only allowed to read the schema graph, and the planner checks that
   **inside expressions too**. It must also be writable back as SPARQL
   at plan time.

Mechanism 1 decides *what SQL can do*. Mechanism 2 decides *what happens to
the rest*. The planner tries them in that order, and a query that neither
covers falls back to today's routes unchanged. Every rewrite below either
has an equivalence argument over SPARQL's own semantics (compatible
mappings, bags, unbound variables, expression errors) or it declines.

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
`OPTIONAL`, it adds a column (`?typeNl`), and it left-joins. So the
`Values` stays an engine node between SQL nodes, and each one splits the
island. *Plan printouts* shows this on today's planner.

### The worked example, measured

The query is in issue #494 unchanged: `?asset a asset360:CivilEngineeringAsset`,
51 selected variables, 18 label `OPTIONAL`s of the form
`OPTIONAL { GRAPH schema: { ?x skos:prefLabel ?l . FILTER(lang(?l) = "nl-be") } }`,
each nested under the `OPTIONAL` that reads `?x` (the scoper refuses the
un-nested form as a cartesian product, see *Plan printouts*, P0). It also
has one `BIND(STRAFTER(STR(?builtElement), "BuiltElement/4-") AS ?invId)`
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
  columns, under the precondition that makes SQL equality mean SPARQL
  compatibility. Values whose key is an enum slot are translated to stored
  codes.
* **M2.** A new engine input: the solutions of the SQL statement instead of
  the records it fetched. Three rewrites, R1–R3, that move engine-only
  operators out of an `OPTIONAL`, and `ORDER BY` / `LIMIT` below
  row-preserving engine operators, so that the SQL part is one island at
  the bottom of the plan. The ordering contract across the boundary.
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

## The facts every guard below is stated in

Revision 1 stated its guards in loose words ("bound in `B`", "functional").
The review showed that each loose word hid the case that broke it. So the
guards below use only these facts. Each one already exists in
`sparql_refine.rs`, or is defined here once.

| fact | meaning | where |
|---|---|---|
| `scope(n)` | the variables in scope at `n`: they *may* be bound in a solution from `n` | `Plan::variables_of` |
| `bound(n)` | the variables bound **in every** solution from `n`. `UNDEF` in a `VALUES` column, an `OPTIONAL` right side, a `UNION` arm that omits the variable, and a `BIND` that can error all take a variable out | `Plan::definitely_bound_of` (default arm empty, so an unknown node guarantees nothing) |
| compatible | μ₁ and μ₂ agree, **by `sameTerm`**, on every variable *both* bind. A variable unbound on one side constrains nothing | SPARQL 1.1 §18.3 |
| effect-free `e` | evaluating `e` gives the same value in any evaluator, any number of times: no `RAND`/`UUID`/`STRUUID`/`BNODE`/`NOW`/`IRI`, no custom function, no `Opaque` | `Expr::evaluates_the_same_out_of_context` (reused unchanged) |
| `vars(e)` | the variables `e` names. **Undefined** when `e` contains an `Opaque` (an `EXISTS` substitutes the whole mapping into its pattern), so every guard that reads `vars(e)` first requires `!e.contains_an_opaque_subquery()` | `variables_used`, `Expr::contains_an_opaque_subquery` |
| barrier kind | an `OPTIONAL` body is `SubSelect { domain: None }` (the identity projection). A sub-`SELECT` is `SubSelect { domain: Some(d) }` with private variables renamed `?v__d{d}` | `PlanOp::SubSelect` |
| fresh `?__m{n}`, `?__ord` | a name no node in the plan mentions, chosen when the rule fires. The parser accepts `?__x`, so the name is checked against `Plan::variables` and not assumed | new |

## M1 — a schema relation is a relation the statement can join

### The operator

A new SQL operator, `Op::Constant`:

```rust
/// An inline table: `(VALUES (…), (…)) AS c{n}(col, …)`.
Constant {
    alias: String,
    columns: Vec<ConstantColumn>,   // one per variable, in row order
    rows: Vec<Vec<Option<String>>>, // stored representation per cell, NULL for UNDEF
}
pub struct ConstantColumn {
    pub var: String,
    /// How the cell becomes an RDF term, as for a scan column.
    pub descriptor: TermDescriptor,
    /// Set on a key column: the other side's column it is compared with,
    /// and the translation that made the cells comparable.
    pub key: Option<ConstantKey>,
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
value (an enum code, a string). The join may be `Inner` or `Left`. Its
exported columns are ordinary projection bindings. There is no
`JoinKey::Cross` for a constant: a join with no shared variable declines
(see below).

### The precondition: when SQL equality *is* compatibility

This is the section revision 1 got wrong. A SPARQL join matches a pair of
mappings that are **compatible**. An SQL join matches a pair whose key
columns are **equal**. The two agree only under conditions, and the rule
fires only when all of them hold. `X` is the other input of the `Join` /
`LeftJoin`, and `J = scope(X) ∩ vars(Values)` is its `on` list.

**K1: every shared variable is definitely bound on the other side.** For
every `?j ∈ J`, `?j ∈ bound(X)`. A solution of `X` with `?j` unbound is
compatible with *every* row of the table, and SQL's `NULL = v` matches
none. The review's counterexample is that case: an asset with no `?j`
still joins `(<urn:outside-enum> "label")` and acquires both bindings
(regression `m1_unbound_key_joins_every_row`). When K1 fails, M1 declines.
The `Values` stays an engine node, and M2 handles it if it can.

**K2: no key cell is `UNDEF`.** For every `?j ∈ J`, every row binds `?j`
(`?j ∈ bound(Values)`). An `UNDEF` cell is compatible with every `X` row,
and SQL's `NULL` matches none (regression `m1_undef_key_cell`). A non-key
column may hold `UNDEF`: it renders `NULL`, which reads back as unbound,
and merging with an unbound variable is exactly that.

**K3: `J` is not empty.** An empty `J` is a cross product. It would be
sound as `JoinKey::Cross`, but no query in the corpus needs it, and
declining is the conservative reading of "the join key renders". It stays
in the engine.

**K4: the key's term map is invertible, so equality of stored values is
`sameTerm`.** Compatibility compares RDF terms. SQL compares stored text.
They agree only when the other side's column descriptor maps stored values
to terms **injectively**, and the planner can compute the inverse image of
each cell. Supported key descriptors:

| other side's descriptor | cell translation | what cannot match, and is dropped |
|---|---|---|
| record identity (`JoinKey::Identity`) | the identity column's own encoding, as op 5 already compares it | a cell that is not an IRI |
| `EnumIri` | IRI → the stored code(s) whose concept IRI it is (inverse of `enum_map`. If two codes share a meaning, the row is emitted once per code, and each stored value still matches exactly one of them) | an IRI outside the enum's image. A **literal** cell makes the rule decline: a stored code the enum does not permit renders as a literal, so a literal could match it, and inverting that is not worth a rule |
| `Literal` with no datatype, or `rdf:langString` with a fixed `lang` | lexical form = stored text | a cell with another datatype or language tag |

Every other descriptor declines: numeric and date literals (Oxigraph
canonicalises `"01"^^xsd:integer` to `"1"`, and the stored JSON text need
not be canonical), and `Iri` range slots whose stored form may be a CURIE.

**Dropping a row is sound only because of K1.** A dropped cell's term is
outside the image of the other side's column. The other side binds `?j`
in every solution (K1), so no solution of `X` is `sameTerm`-equal to it,
and the row is compatible with nothing. Revision 1 dropped rows without
K1, and that was the unsound case. With K1 the drop is the same argument
`values_narrow_the_joined_scan` already relies on (#409). The label table
still shrinks from all 329 labels to the enum's own values.

**K5: multiplicity is kept.** The constant is rendered with every row the
`Values` has, duplicates included, and never under `DISTINCT`. An `X` row
compatible with *k* table rows gives *k* rows. In a left join, an `X` row
compatible with none gives one row with the table's columns `NULL`, which
is `LeftJoin`'s bag semantics.

**K6: a left-join condition goes in `ON` or declines.** It goes in `ON`
when it lowers (the existing `optional_side` path), and never in `WHERE`.
Otherwise the rule declines.

**K7: the columns are uniform.** Each column's non-`UNDEF` cells are one
kind of term: all IRIs, or all literals of one datatype and one language
tag. A column carries one descriptor.

**Blank nodes are already excluded** by `materialise`, so a cell is
always a ground term.

What it claims: the `Values` obligation and any obligation the
materialised region discharged. That region's triples and filters are
already folded into the `Values` node's accounting today.

**Merged bindings.** Revision 1 did not state these, and the review asked
for them. Under K1 and K2 every shared variable is bound on both sides of
every matching pair, so the merged mapping takes the shared variables from
either side (they are `sameTerm`-equal) and the table's other columns from
the table. The case the review describes, where the join must export the
right side's binding because the left left it unbound, is excluded by K1
and not constructed.

### What this does beyond labels

The rule reads a `Values` node and nothing else, so everything that already
produces one gets it:

* a schema region used to **filter** records, where
  `values_narrow_the_joined_scan` cannot fire because the table adds a
  column. The join now happens in the statement instead of in the engine.
  *Plan printouts*, P2, is this case.
* a schema region that **adds columns**: `skos:notation` codes, a slot's
  annotations, a class's label, whenever K1 holds, that is, where the
  lookup sits inside the `OPTIONAL` that binds its key. The #494 labels
  have this shape.
* a client-written `VALUES` block with the same shape.

The two existing narrowing rules stay. They are cheaper when they apply,
because they turn the table into an `IN` on the scan and allow index use.
`LowerConstantRelation` applies when they do not. The narrowing is still
derived where its preconditions hold, because a statement that both
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
    /// New: the SQL pass emitted solutions over `vars`; evaluate `finish`
    /// over them and the schema graph. No record is loaded.
    Solutions {
        vars: Vec<String>,
        /// The variable the executor numbers the rows in, 1.., in the order
        /// the statement returns them. `None` when the query has no order.
        ordinal: Option<String>,
        /// The engine region as a SPARQL query, built at plan time, with
        /// the island as the placeholder `VALUES` the executor fills.
        finish: String,
        /// One output row per statement row (see *Limits*).
        preserves_rows: bool,
    },
}
```

`Refinement` gains `UsedRows(String)`: the statement emits solutions, and
the engine finishes over them. The planner prefers outcomes in this order:
`UsedAlone`, then `UsedRows`, then `Used`, then `Fallback`.

### When the planner chooses it

At plan time, from the plan alone. **All five must hold, and any doubt
declines to `Used`**, which is today's route and always correct.

1. **The statement emits solutions.** The SQL nodes form one island, and
   its root is an ungrouped `Project` with bindings or a `Group`. Under M2
   the island root gets a synthesised `Project` over `demand` of the
   engine region: the variables it reads, plus any witnesses and the
   ordinal. It is built with the `projected_columns` / `scope_columns`
   machinery an enclosed `OPTIONAL` body already uses. This is what fixes
   "fetches rows rather than emitting solutions" in the last two rows of
   the table.
2. **The engine region reads no instance data, including inside its
   expressions.** Revision 1 used `schema_only`, which walks operator
   inputs only. A `Filter`, `Bind`, `LeftJoin` condition, `Sort` term,
   measure or `HAVING` whose expression is `Opaque` (`EXISTS`,
   `NOT EXISTS`) reads the default graph without an operator for it. With
   rows and the schema graph only, `BIND(EXISTS { ?s :p ?v } AS ?found)`
   evaluates `false` where the records route says `true` (regression
   `m2_exists_in_bind_reads_instances`). The new check,
   `finish_region_reads_schema_only`, walks the region and fails on:
   * a `Match` or `Path` outside a `GRAPH` naming the schema graph, a
     `GRAPH ?g` with a variable name, a `Service`, a `Scan` or an
     `Unnest`. These are what `schema_only` already refuses.
   * **any expression on any region node that contains an `Opaque`**
     (`contains_an_opaque_subquery`) **or an `InClass`**. An `InClass` is
     `EXISTS { ?v a C }`, which reads an instance type.
   * an `AntiJoin` whose right side fails this same check.
3. **The write-back succeeds, and its result is what runs.** At plan
   time, the planner writes the region back with `plan_to_algebra`, with
   the island replaced by a placeholder `VALUES` over the exported
   variables. If that returns `None`, the plan is `Used`. This happens
   for an `Opaque`, a `Slot` expression left in the region, a `Service`,
   or a non-`SELECT` form. On success the query text goes into
   `EngineInput::Solutions::finish`. The executor evaluates that text and
   nothing it derives itself. The review is right that revision 1's
   "write-back already covers every `PlanOp`" was false. This makes the
   partiality a refusal at plan time instead of a failure at execution
   time.
4. **The finish query has the original's evaluation context.** The
   written-back query carries the original `BASE` (`plan_to_query` builds
   it with `base_iri: None` today, which is review finding 9's shape).
   `sparql_finish` builds its evaluator with the same constructor as
   `sparql_execute`: the same custom-function registry (spargeo), the same
   limits. A query with a dataset clause (`FROM`, `FROM NAMED`) declines,
   since the plan does not carry the dataset (review finding 4).
5. **Every volatile expression is evaluated in the finish query, and
   only there.** M2 splits one evaluation into two: the statement and the
   finish query. A `NOW()` must return one value within a query, so a
   `NOW()` evaluated in SQL and another in the finish query would be two
   evaluations. `RAND`/`BNODE`/`STRUUID` also count once per row on each
   side. The check: every expression the island lowers passes
   `evaluates_the_same_out_of_context`. Today that holds by construction.
   SQL lowers slot comparisons and memberships against literals, and
   `PushProjection` declines a `BIND`. But the check makes it a stated
   precondition, not an accident of what lowering happens to support. A
   future lowering of `NOW()` would then decline `UsedRows` instead of
   splitting the timestamp. Schema regions materialised at plan time are
   a third evaluation, and they already pass the same check
   (`sparql_materialise.rs`).

The partition itself, "island below, engine region above", is what R1–R3
produce.

### Getting the SQL part to the bottom: the rewrites

The frontier must still be a cut. With engine-only nodes nested inside
`OPTIONAL`s, they sit between SQL nodes and split the island. Three
rewrites move them up. Each is stated as *Match / Edit / Equivalence /
Declines / Regression*, which is the shape `sparql-scopes-as-relations.md`
uses for its ops.

#### The match witness

R1 and R2 share one device, and it answers the review's main point on R1.
A `LeftJoin` may carry `witness: Some(?__m{n})`, a fresh variable bound to
`true` in exactly the solutions where its right side matched, and unbound
where the left row was kept unextended. In algebra (for the oracle and for
`finish`) it is `BIND(true AS ?__m{n})` appended to the right side. In SQL
it is a column that is non-`NULL` exactly when the right side joined. It
is a constant `TRUE` inside a relation's derived table, or
`CASE WHEN <right scan identity> IS NULL THEN NULL ELSE TRUE END` for a
flat right scan. An identity column is never `NULL` on a joined row.

Three properties make it a witness, and each is checked when the rule
fires:

* **fresh**: no node in the plan mentions the name. So nothing on the
  left, and nothing that will later be joined to it (C in R1), can bind
  it. This is the property `BOUND(?j)` lacked. C *can* bind `?j`.
* **bound iff matched**: by construction, on both routes.
* **observed by nobody else**: the rule places a projection that drops the
  witness directly above its last consumer. A `DISTINCT`, a `GROUP`, a
  `COUNT(DISTINCT *)` or a join above never sees it. In `demand` terms:
  the witness's demand is exactly its consumers.

#### R1 — a left join against an engine-only right side moves up

    A ⟕ SubSelect∅(B ⟕_f SubSelect∅(C))
      →   π₋ₘ( (A ⟕ₘ SubSelect∅(B)) ⟕_{BOUND(?m) ∧ f} SubSelect∅(C) )

`SubSelect∅` is an `OPTIONAL`-body barrier (`domain: None`). `⟕ₘ` is a
left join that exports the witness `?m`. `π₋ₘ` drops it. `C` is
engine-only: a schema region M1 declined, or one over the materialisation
cap.

**Match.** A `LeftJoin` whose right input is a `domain: None` barrier
whose input is a `LeftJoin` whose right input is a `domain: None` barrier
over an engine-only subtree `C` (`finish_region_reads_schema_only(C)`),
where:

* **G1a**: the outer `LeftJoin` has no condition. With one, the original
  evaluates it over `a ⊕ b ⊕ c` and the rewrite would have to split it
  between two joins. This is declined, not constructed.
* **G1b**: `vars(C) ∩ scope(A) ⊆ bound(B)`. A variable C shares with A
  must be pinned by every B row.
* **G1c**: the inner condition `f` has no `Opaque`, and
  `vars(f) ∩ scope(A) ⊆ bound(B) ∪ bound(C)`.
* **G1d (effects)**: `f.evaluates_the_same_out_of_context()`, and the
  same for every expression on every node of `C`. G1c fixes `f`'s
  *inputs*. It does not fix its *value*. In the original, the inner join
  evaluates `f` once per `(b, c)` pair, and every `a` that joins that
  pair shares the one result. After the lift, `f` is evaluated once per
  `(a, b, c)`. With `f = RAND() < 0.5`, the original gives every `a` the
  same decision, and the rewrite mixes them (regression
  `r1_volatile_condition`: 40 left rows, 40 or 0 labels originally, 22,
  20, 21 … after the lift). `C`'s own expressions are covered too, as a
  precaution rather than a proof. SPARQL evaluates `C` once on both routes, and Oxigraph
  does too (`r1_volatile_inside_c`). But the proof should not rest on how
  an engine schedules a right side, and declining costs nothing:
  a materialised schema region already passes this check
  (`sparql_materialise.rs`, `every_expression_evaluates_the_same_out_of_context`).
* **G1e**: `A` and `B` are SQL. The rule exists to join them in one
  statement, and the progress measure depends on it (*Evidence and
  progress*).

**Equivalence.** Fix a left row `a`. For a pair `(b, c)`: in the
original, `b ⊕ c` is produced when `c ~ b` and `f(b ⊕ c)`, and it is kept
when `a ~ b ⊕ c`. In the rewrite, `a ⊕ b ⊕ m` is produced when `a ~ b`,
and extended by `c` when `c ~ a ⊕ b` and `f(a ⊕ b ⊕ c)`.

* The pairs that merge are the same. `a ~ b ⊕ c` holds iff `a ~ b` and
  `a ~ c` on the variables C and A share. By G1b those are bound in `b`,
  so `c ~ b` and `a ~ b` give `a ~ c`. The condition takes the same value
  on both routes: by G1c, every variable of `f` that `a` could supply is
  already bound by `b` or `c`, with the same term, since the mappings are
  compatible. By G1d, the same inputs give the same value, however many
  times `f` is evaluated. Revision 2's argument stopped at the inputs,
  and that is the gap the review found.
* The unmatched cases are the same. In the original, a `b` with no
  qualifying `c` yields `b` alone. In the rewrite, `a ⊕ b ⊕ m` finds no
  qualifying `c` for the same reason and is kept alone. A left row with no
  compatible `b`: in the original, `B ⟕ C` offers it nothing, so it stays
  `a`. In the rewrite it stays `a` *without* `?m`. `BOUND(?m)` is then
  false for every `c`, and it stays `a`. This is exactly where the
  revision 1 guard failed: C bound `?j` itself and made `BOUND(?j)`
  true.
* **An empty shared set** (`vars(B) ∩ vars(C) = ∅`, the review's
  "no protection at all") is covered by the same argument. `BOUND(?m)`
  does not depend on `J`.
* Multiplicity: each `(a, b)` contributes the same number of rows on both
  sides, namely the qualifying `c`s, or one row.
* `π₋ₘ` removes the only variable the rewrite added.

**Declines.** A `domain: Some` barrier (a sub-`SELECT`), anywhere on the
path. Its modifiers (`LIMIT`, `DISTINCT`, `GROUP`) would count different
rows, and C could reference a private `?v__d{n}`. An outer condition
(G1a). A variable of C that A binds and B may not (G1b, regression
`r1_c_shares_with_a_not_pinned_by_b`). A condition reading what only A
supplies (G1c, regression `r1_inner_condition_reads_a`). A volatile `f`,
or a volatile expression inside `C` (G1d, regression
`r1_volatile_condition`). An `A` or `B` that is not SQL (G1e).

**Regressions.** `r1_bound_j_tests_merged_mapping` (the review's),
`r1_empty_shared_set`, and the three decline cases above. Each is evaluated
before and after on the oracle (bag equality), and each decline case
asserts that the rule did not fire. `r1_volatile_condition` is the one
exception to "evaluate the rejected rewrite and assert it differs". Its
automated assertion is only that R1 declines and that the `declined` line
names G1d. The random draw is recorded in the appendix and never runs in
CI. `r1_effect_free_condition` is its twin with `STRLEN(?label) = 5` in
place of `RAND() < 0.5`. It must fire, and it is compared as a bag.

#### R2 — an extension over an optional side moves up

    A ⟕ SubSelect∅(Extend(B, ?x, e))
      →   π₋ₘ( Extend(A ⟕ₘ SubSelect∅(B), ?x, IF(BOUND(?m), e, ?__never)) )

`?__never` is a fresh variable nothing binds. Evaluating it is an error,
and an error leaves `?x` unbound. That is SPARQL's `BIND` rule, and it is
what the original gives for an unmatched row.

**Match.** A `LeftJoin` whose right input is a `domain: None` barrier
directly over a `Bind`, where:

* **G2a**: `?x ∉ scope(A)`.
* **G2b (input preservation)**: `e` has no `Opaque`, and
  `vars(e) ∩ scope(A) ⊆ bound(B)`. This is the review's point. With
  `e = ?b + ?a`, `?a` is in `scope(A)` and not in `bound(B)`, so the rule
  declines. The original computes `?x` without `?a` (unbound), and the
  lift would compute it with `?a`.
* **G2c (effects)**: `e.evaluates_the_same_out_of_context()`. Inside B,
  `e` is evaluated once per B row. After the lift, it is evaluated once
  per *joined* row, and in a different evaluator on the `UsedRows` route.
  A volatile `e` is therefore declined, whatever its strictness:
  `BIND(BNODE() AS ?x)` gives one blank node for a B row joined by two A
  rows in the original, and two after the lift (regression
  `r2_bnode_once_per_b_row`).
* **G2d**: the `LeftJoin` has no condition. The original evaluates it with
  `?x` available.
* **G2e**: `A` and `B` are SQL, as G1e.

**Equivalence.** Take a row `a` matched by `b`. By G2b, every variable of
`e` has the same binding in `a ⊕ b` as in `b`: it is either unbound in
`a`'s scope, or bound by `b` with a compatible, hence identical, term.
`BOUND(?m)` is true, so `e` gets the same inputs and, by G2c, gives the
same value. Take an unmatched row `a`. `?m` is unbound, so the `IF`
evaluates `?__never`, which errors, and `?x` stays unbound. In the
original, `a` is kept without `?x`. Multiplicity: `Extend` is one row in,
one row out. **Strictness is no longer used.** Revision 1's list of
strict built-ins was a correctness assumption. The witness makes it
unnecessary, and `COALESCE(?b, 0)`, which revision 1 had to decline, now
qualifies (regression `r2_non_strict_expression`).

**Edit, exports.** The barrier's `vars` loses `?x` and gains `vars(e)` and
the witness. `vars(e)` are exactly what the naive barrier exported (an
`OPTIONAL` body exports everything it binds), so this reverses a prune
`PruneUnusedExports` was entitled to make and no longer is, and its
demand check runs again after the edit.

**Declines.** A sub-`SELECT` barrier. G2a–G2e. A `Bind` that is not
directly under the barrier. That covers a `BIND` followed by a `FILTER`,
which spargebra lifts into the left-join condition (G2d), and a `BIND`
under a join inside the body (this rule does not move it past that
join).

**Regressions.** `r2_input_from_a` (the review's `?b + ?a`),
`r2_bnode_once_per_b_row`, `r2_non_strict_expression`, and
`r2_qualifying_strafter` (the #494 shape, which must fire).

A left-join *condition* (a `FILTER` directly inside an `OPTIONAL`) does
**not** move up. A failing condition keeps the left row unextended, but
a filter above would drop it. Such a condition must either lower (it
already does, into `ON`) or keep its `OPTIONAL` in the engine. In the
second case the query is `Used`. The label filters are inside the
materialised schema region, so they never reach this case.

#### R3 — `ORDER BY` / `LIMIT` / `OFFSET` move below one-to-one engine operators into SQL

    Slice(Project(Sort(E(X), k)))
      →   Project(Sort(E(Slice(Sort(X, k))), ?__ord))

The inner `Sort`/`Slice` go to SQL, where `PushProjection` already lowers
them with SPARQL's order: `COLLATE "C"` text, numbers by value, unbound
first ascending, ties settled on the row key. The outer `Sort(?__ord)` is
the ordering contract (next section). `?__ord` is projected away by the
query's own `Project`.

The two sorts are different operators, and the plan says so. A `Sort`
carries `origin: Query` (the query's `ORDER BY`, the one R3 moves) or
`origin: Ordinal` (the one R3 inserts). R3 matches only `origin: Query`,
and no progress measure counts an `Ordinal` sort. So R3 cannot match its
own output, and the restoring sort cannot look like unfinished work
(*Evidence and progress*).

**Match.** The root scope's modifier chain (not a sub-`SELECT`'s), whose
`Sort` has `origin: Query`, over a chain `E` of engine nodes above the
island, where:

* **G3a (one-to-one)**: every node of `E` gives **exactly one output row
  per input row**. Revision 1 said "one output per input, in input
  order". Order is now the ordinal's job, so only the count is required.
  Qualifying nodes:
  * `Bind`: always, since an error leaves the variable unbound and keeps
    the row.
  * `Project` / `SubSelect∅`: always.
  * `LeftJoin(X', C)` **with a compatible-mapping proof**. `C` is a
    `Values` (the planner holds its rows), and there is a key set
    `K ⊆ scope(X') ∩ vars(C)` with (i) `K ⊆ bound(X')`, (ii) no row of
    `C` has `UNDEF` in `K`, and (iii) no two rows of `C` agree on `K`
    under `sameTerm`. Then every left row is compatible with at most one
    `C` row, because a compatible row must agree with it on all of `K`,
    which the left row binds. A left join always keeps its left row, so
    the count is exactly one. A condition can only turn a match into a
    non-match, which is still one row. The review's counterexample fails
    (i): `(id=1, j=UNDEF)` has `?j ∉ bound(X')`, so it is compatible
    with all three rows (regression `r3_undef_left_key_fans_out`).
* **G3b**: every sort term is a variable of `scope(X)` that no node of
  `E` binds, so its value on an output row is its value on the input
  row. `E` introduces only fresh variables (G2a, and C's non-key columns
  are outside `scope(X')`), so this holds for every variable of `X`. An
  expression term declines, as it does in `PushProjection`.
* **G3c**: `X` itself satisfies `PushProjection`'s acceptance for its sort
  terms: they are readable columns.

`Distinct`, `Reduced`, `Filter`, `Group`, an inner `Join`, and a
`LeftJoin` without the proof are not one-to-one. `Sort` / `Slice` stay
above them in the engine.

**Equivalence.** Let `X` produce the sequence `x₁ … xₙ` in `k` order
(ties settled by SQL). `E` maps each `xᵢ` to exactly one `yᵢ` with the
same `k` values (G3a, G3b). So `Sort(E(X), k)` is `y₁ … yₙ`, a valid
SPARQL order, and `Slice` takes `y_{o+1} … y_{o+l}`. That is
`E(x_{o+1} … x_{o+l})`, which is what the rewrite computes. `Sort(?__ord)`
then reinstates the sequence whatever order `E`'s evaluation emitted.
SQL's tie order may differ from the engine's. Both are conforming (an
`ORDER BY` on a non-unique key is a partial order in SPARQL too), and
`PushProjection` already documents this.

**Effects.** R3 moves no expression. The sort terms are variables (G3b),
and `Slice` has none. What changes is *which* rows `E` is evaluated over:
the page, not the full input. Every expression of `E` is still evaluated
exactly once per row that reaches the answer, over the same input row
(G3a, G3b). So a volatile expression in `E` gets one fresh draw per
answer row on both routes, and every answer of the rewrite is an answer
the original can give. That is why R3 needs no effects guard, while R1
does: R1 changes *how many times* `f` is evaluated for one result, and R3
does not. Pinned by `r3_volatile_bind_in_e`, which must fire. Its
assertion compares the `?id` sequence, never the drawn values.

**Edit, the ordinal's path.** The executor binds `?__ord` at the island
root. The restoring `Sort(?__ord)` consumes it. Every node between them
must therefore keep it in scope. G3a lets `E` contain `Project` and
`SubSelect∅` nodes, and R1/R2 put their witness-cleanup projections `π₋ₘ`
there too. So:

* R3 adds `?__ord` to the `vars` of every `Project` and `SubSelect∅` on
  the path from the island root to the restoring sort. Its demand is that
  sort, so `PruneUnusedExports` keeps it.
* Every projection a rule introduces, `π₋ₘ` included, is built as
  **scope minus what it drops**, never as a list of what it keeps. An R1
  or R2 application in a later round therefore passes `?__ord` through
  without knowing about it.
* Only the query's final `Project`, above the restoring sort, drops it.
* The transition check `ordinal_reaches_its_sort` runs after every
  application (R1–R3 and every other rule): `?__ord ∈ scope(n)` for every
  node `n` on that path. A failure is a `PlanDefect::Transition`, as a
  lost demanded export already is.

**Regressions.** `r3_undef_left_key_fans_out` (the review's),
`r3_duplicate_key_in_c` (declines), and `r3_qualifying_page` (fires). The
last is compared as a **sequence**, and as page membership over
`OFFSET 0, 2, 4, …` tiling the full ordered answer on a total-order key.
Two composition regressions (*Tests*):
`r3_after_r2_one_island` and `r3_ordinal_through_projections`.

#### The ordering contract across the SQL/engine boundary

The review's P2 is right: the finish query receives the rows as a
`VALUES` (or an iterator), and neither carries an order in SPARQL
algebra. Oxigraph does not keep it through an inner join. Forty rows in
descending `?id`, joined to a seven-row table, come back grouped by the
table's key (regression `p2_join_reorders_values`). It happened to keep
the order through an `OPTIONAL`, which is behaviour, not contract.

So order is data:

* `EngineInput::Solutions::ordinal` names `?__ord`. The executor binds it
  to `1, 2, …` in the order the statement's rows arrive. For a
  statement with a top-level `ORDER BY`, SQL guarantees that order.
* The finish query ends in `ORDER BY ?__ord` below its projection, and the
  projection drops `?__ord`. No projection between the island and that
  `ORDER BY` may drop it, including the witness-cleanup ones (R3,
  *Edit, the ordinal's path*).
* The finish query has no `LIMIT`/`OFFSET` of its own after R3. Page
  membership is decided once, in SQL.
* **VALUES versus an injected iterator is now contract-neutral, and
  provably so:** the finish query sorts by a total key it was handed, so
  the order the rows enter in cannot matter (Q7).

When R3 did not fire, the `Sort`/`Slice` stay in the finish query as the
query wrote them. The ordinal is then `None`, because nothing below needs
to carry an order.

### How the executor runs it

The consolidator executes the statement as it does for `UsedAlone`
(`build_aggregate_sql_from_ops`). It turns the rows into SPARQL terms with
the column descriptors (`to_sparql_results`, unchanged), numbers them
into `?__ord` when `ordinal` is set, and then calls a new entry point
here:

```rust
pub fn sparql_finish(
    plan: &ExecutionPlan,              // carries EngineInput::Solutions, incl. `finish`
    solutions: &SparqlSolutions,       // the statement's rows, as terms
    schema_view: &SchemaView,
    limits: ExecuteLimits,
    schema_graph_iri: Option<&str>,
) -> Result<SparqlAnswer, ExecuteError>
```

It substitutes the rows for the placeholder in `finish` and evaluates the
result over a store holding **only the schema graph**. The store has no
instance triples, so the triple ceiling has nothing to count (see
*Limits*). It computes nothing from the plan on its own. The query it
runs is the one the planner verified and printed.

## Scopes, barriers and renaming

The review asked how R1–R3 interact with nested `SubSelect` / `Relation`
scopes and with the invariants `sparql-scopes-as-relations.md` keeps.
Rule by rule:

| | crosses | refuses | why |
|---|---|---|---|
| M1 | nothing: it re-places one `Values` and its join | — | a local join lowering. Inside a pushed barrier it renders in that relation's own alias space (`c{n}` numbered per statement body), and its non-key columns leave the relation as relation columns whose `term_of` is the constant column's descriptor |
| R1 | two `domain: None` barriers (OPTIONAL bodies, identity projections) | any `domain: Some` barrier on the path | a sub-`SELECT`'s modifiers count its own rows, and its private variables are not visible outside |
| R2 | one `domain: None` barrier | a `domain: Some` barrier | same |
| R3 | the root scope's modifier chain, and `domain: None` right sides inside `E` | a sub-`SELECT`'s modifiers | those are lowered with the sub-select by op 4 (`PushBarrier`) or stay in the engine with it |

**Obligations stay in their scope, via a transfer.** R1 moves C, and the
obligations C claims (the label triple and its `lang` filter), from the
inner `OPTIONAL` scope to the outer join. That breaks the invariant
"an obligation raised in scope S is discharged in S or by the node that
combines S with the outside" unless the move is recorded. R1 records it
as a `Transfer` on each such obligation (op 3b's mechanism, one step,
`via` the new outer `LeftJoin`'s `NodeKey`), and `Plan::check()`
validates it step by step as it does 3b's. R2 moves a `Bind`, which
discharges no obligation. R3 moves the `order` and `slice` obligations
within the root scope, so no transfer is needed.

**Producers.** After R1, C's variables are produced outside the body and
the barrier stops exporting them. After R2, the lifted `Bind` produces
`?x` and the barrier exports `vars(e)` and the witness. Scope closure on
producer slots (*every reference resolves to exactly one producer*) is
the check that catches a rule that left both copies, or none.

**Evidence and progress.** Each rule has its own transition check in
`refine`, computed from the current plan by `NodeKey` and never cached,
as `PruneUnusedExports` does: R1/R2 check that the witness is fresh and
bound by one node only, and that the dropping projection sits above its
last consumer. R3 re-derives the compatible-mapping proof on the
post-edit plan.

**Progress is a measure, not the island count.** Revision 2 kept a
rewrite only if it reduced the island count. The review showed that this
discards R3 exactly where it is wanted. In P4, R2 has already made the
SQL part one island, and R3 moves the query's `Sort`/`Slice` *into* that
island. The count stays at one, so the gate discarded it. The existing
`refine` driver has no such gate either: a rule fires when it matches,
and the driver runs to a fixpoint under `MAX_ROUNDS`. So the gate is
withdrawn. R1–R3 are accepted by a measure that every application
strictly lowers:

    Φ = ( I, D, S )   compared lexicographically

* `I`: the number of SQL islands.
* `D`: the sum, over engine nodes `n`, of the number of `domain: None`
  barriers above `n`. That is how deep engine work sits inside `OPTIONAL`
  bodies.
* `S`: the number of `Sort`/`Slice` nodes with `origin: Query` that have
  an engine node between them and the island. An `origin: Ordinal` sort
  is counted nowhere.

Each application lowers Φ, and none raises an earlier component. `k` is
the number of `domain: None` barriers above the outer left join:

| rule | `I` | `D` | `S` |
|---|---|---|---|
| R1 | lowered by one: A and B were two islands (the engine inner join separated them), and `A ⟕ₘ B` is one (G1e) | lowered by `k + 2 + \|C\|` for a lift at depth `k`: C's nodes lose a barrier, and the inner join and its barrier leave the engine | unchanged: C stays below the same modifiers |
| R2 | lowered by one, as for R1 (G2e) | lowered by `k + 1`: the `Bind` loses its barrier, and the barrier and the left join become SQL | unchanged |
| R3 | unchanged: `Sort`/`Slice` join the island directly below them | unchanged: they leave the engine, and the new `Sort(?__ord)` sits in the root scope, at depth 0 | lowered by the number of modifiers moved (2 in P4) |

R1 and R2 lower `I` on their own, so for them the revision 2 gate was
harmless. R3 is the rule that only `S` sees. G1e and G2e are new in
revision 3, and the measure is why they are needed. Lifting over an
engine-side `B` unites no islands, and at depth `k ≥ 2` it can *raise*
`D`: the new `π₋ₘ` and the joins sit at depth `k`, while `B` stays at
`k + 1`. Such a lift also makes no statement possible. The inner rules
fire first, bottom up, and make `B` SQL where they can.

`Φ` lives in ℕ³, which is well-ordered lexicographically, so R1–R3 can
fire only finitely often. The transition check computes Φ before and after
each R-rule application and fails the plan with a `PlanDefect::Transition`
if Φ did not drop. It is the same shape as the existing export check: a
statement about two plans, computed fresh. `MAX_ROUNDS` and the fixpoint
assertion remain the backstop for the rule set as a whole.

Φ answers "does this rewrite make progress?" It does not answer "is it
worth keeping?" The outcome answers that. R1–R3 exist to make `UsedRows`
possible. If placement does not reach `UsedRows`, the planner refines
again without R1–R3 and places that plan. On `Used`, the engine re-runs
the whole query over the fetched records, and a `Slice` in the fetch
would drop records that its `ORDER BY` needs. The second refine happens
only when an R-rule fired, and planning costs milliseconds.

Pinned in composition, not only per rule (*Tests*):
`r3_after_r2_one_island` is P4 end to end. R2 fires, leaving one island,
and then R3 fires with `I` 1 → 1 and `S` 2 → 0 and is **retained**. The
statement carries `ORDER BY` / `LIMIT`, and the finish query ends in
`ORDER BY ?__ord`. The answer equals the oracle's as a sequence.

**Alpha-renaming.** M1 matches on producers, not names, so a private
`?t__d1` inside a sub-`SELECT` lowers exactly as a root-scope `?t`. R1–R3
never look inside a `domain: Some` barrier, so a private variable can
never become visible outside. Pinned by an extension of the existing
alpha-renaming property test: for each shape in *Plan printouts* P2–P5,
renaming every private inner variable must leave the refined plan
identical up to names, including which rules fired and which declined.
The witness and ordinal are fresh by construction. The test also renames
a user variable to `?__m1` and checks that the rule then picks
`?__m2`.

## Plan printouts

The standing rule is that `to_string` on the raw and refined plan makes
it readable what happens. Printouts P0 and P1 are today's planner at
`8d722c2` (`naive_plan_text`, `refined_plan_text`, `plan_query_refined`
on the `tests/data` schema with the schema graph configured, obligations
elided). P2–P5 are **projected**: the format the implementation must
produce, in today's notation plus four additions:

* `lowered as c{n}, key …` on a `values` line that M1 flipped;
* `by value ?v = c{n}.v (<column>, <translation>)` on a join M1 keyed;
* `witness ?__m{n}` on a left join, and `if BOUND(?__m{n})` on its consumer;
* a `declined` section: rule, node, and the guard that failed, for the
  four new rules, whenever a rule's match succeeded and a guard stopped
  it.

**P0 (today): an un-nested label lookup is refused.** The scoper refuses
`OPTIONAL { ?s asset360:signalType ?t } OPTIONAL { GRAPH schema { ?t
skos:prefLabel ?typeNl FILTER(lang(?typeNl) = "nl-be") } }` as a
cartesian product ("joined … only through ?t, which that pattern may
leave unbound"). That is K1's case, refused one level earlier. The
nested form is the one to plan.

**P1 (today): the nested label lookup splits the island.**

```
SELECT ?s ?name ?typeNl WHERE {
  ?s a asset360:Signal ; asset360:NationalUniqueID ?name .
  OPTIONAL { ?s asset360:signalType ?t .
    OPTIONAL { GRAPH <…/schema> { ?t skos:prefLabel ?typeNl FILTER(lang(?typeNl) = "nl-be") } } } }

=== refined (today)
  n0   scan      asset360:Signal as ?s, requires [NationalUniqueID→?name, signalType?] [S]  claims o0 o1
  n1   scan      asset360:Signal as ?s, requires [signalType→?t] [S]  claims o2 o5
  n2   values    ?t ?typeNl × 3 row(s)                        [E]  claims o3 o4
  n3   leftjoin  n1, n2                                       [E]
  n4   subselect ?s ?typeNl                                   [E]
  n5   leftjoin  n0, n4                                       [E]
  n6   project   ?s ?name ?typeNl                             [E]
=== execution (today)
  pass 0  SQL     asset360:Signal   (fetch)
  pass 1  ENGINE  input [0]
  fallback  not lowerable: the SQL frontier is 2 islands, and a pass is one statement
```

**P2 (projected): M1 lowers it.** `?t ∈ bound(n1)` (a required read), so
K1 holds. The three label rows are IRIs of `SignalTypes` (1),
`SignalRegime` (2). Only `GSA` is in `signalType`'s enum, so two rows
cannot match any `n1` row and are dropped. Today's
`values_narrow_the_joined_scan` already narrows the inner-join variant
of this query to `signalType = 'GSA'` on the same argument.

```
=== refined (projected)
  n0   scan      asset360:Signal as ?s, requires [NationalUniqueID→?name, signalType?] [S]  claims o0 o1
  n1   scan      asset360:Signal as ?s, requires [signalType→?t] [S]  claims o2 o5
  n2   values    ?t ?typeNl × 3 row(s)  lowered as c1, key ?t: enum code, 1 kept, 2 outside enum [S]  claims o3 o4
  n3   leftjoin  n1, n2  by value ?t = c1.t (signalType, enum→code)  [S]
  n4   subselect ?s ?typeNl                                   [S]
  n5   leftjoin  n0, n4  by identity ?s of asset360:Signal    [S]
  n6   project   ?s ?name ?typeNl                             [S]
=== execution (projected)
ExecutionPlan (contract 6, all in SQL)
  pass 0  SQL     asset360:Signal   → ?s ?name ?typeNl
      scan      asset360:Signal  as ?s
      relation  q0 [?s:identity ?typeNl:constant]
          scan      asset360:Signal  as ?s
          constant  c1 (t, typeNl) × 1   t: enum code of signalType, typeNl: literal@nl-be
          join      value s.signalType = c1.t   left
          column    ?s ← <identity>   iri
          column    ?typeNl ← c1.typeNl   literal@nl-be
      join      identity s.<identity> = q0.s   left
      column    ?s ← <identity>   iri
      column    ?name ← NationalUniqueID   literal
      column    ?typeNl ← q0.typeNl   literal@nl-be
```

**P3 (projected): M1 declines, and says why.** The M1 counterexample's
shape: the label is left-joined where its key may be unbound.

```
  n2   values    ?j ?label × 1 row(s)                         [E]
  n3   leftjoin  n1, n2                                       [E]
  declined
      LowerConstantRelation  n2  K1: ?j is in scope in n1 but not bound in every solution (optional read)
```

**P4 (projected): R2 and R3, finishing over rows.** Today this plans as
two islands (the `Bind` is `[E]` inside the `OPTIONAL`) and falls back.

```
SELECT ?s ?name ?tail WHERE {
  ?s a asset360:Signal ; asset360:NationalUniqueID ?name .
  OPTIONAL { ?s asset360:refersToEulSignal ?e . BIND(STRAFTER(STR(?e), "#") AS ?tail) } }
ORDER BY ?name LIMIT 10

=== refined (projected)
  n0   scan      asset360:Signal as ?s, requires [NationalUniqueID→?name, refersToEulSignal?] [S]  claims o0 o1
  n1   scan      asset360:Signal as ?s, requires [refersToEulSignal→?e] [S]  claims o2 o3
  n2   subselect ?s ?e                                        [S]
  n3   leftjoin  n0, n2  by identity ?s of asset360:Signal  witness ?__m1  [S]
  n4   sort      ?name asc                                    [S]  claims o5
  n5   slice     limit 10 offset 0                            [S]  claims o4
  n6   project   ?s ?name ?e ?__m1  ordinal ?__ord            [S]
  n7   bind      ?tail ← IF(BOUND(?__m1), STRAFTER(STR(?e), "#"), ?__never)  [E]
  n8   sort      ?__ord asc  origin ordinal                   [E]
  n9   project   ?s ?name ?tail                               [E]
  rewrites
      R2  n7  lifted from under n3; inputs {?e} ⊆ bound(n1); effect-free; witness ?__m1
      R3  n4 n5  moved below n7 (bind: one-to-one); sort key ?name ∈ scope(n0); islands 1 → 1, query modifiers above the engine 2 → 0; ?__ord in scope n6 → n8
=== execution (projected)
ExecutionPlan (contract 6, SQL answers, engine finishes over rows)
  pass 0  SQL     asset360:Signal   → ?s ?name ?e ?__m1  (numbered ?__ord)
      …
      order     binding #1 asc
      limit     10 offset 0
  pass 1  ENGINE  solutions of [0], schema graph only, preserves rows
      finish    SELECT ?s ?name ?tail WHERE { VALUES (?s ?name ?e ?__m1 ?__ord) { <pass 0> }
                  BIND(IF(BOUND(?__m1), STRAFTER(STR(?e), "#"), ?__never) AS ?tail) } ORDER BY ?__ord
```

The `rewrites` section is new too. It prints each R-rule application
with the facts it used, so the proof obligations of *Match* are on the
page.

**P5 (projected): a sub-`SELECT` body.** The same lookup, written as
`OPTIONAL { SELECT ?s ?typeNl WHERE { … } }`. Private `?t` is renamed
`?t__d1` by the scoper. M1 fires inside the barrier, exactly as in P2
and up to the name. If an engine-only `C` were left inside, R1 would
decline:

```
  n5   subselect ?s ?typeNl  domain 1                         [E]
  declined
      LiftOptionalRightSide  n6  the body is a sub-SELECT (domain 1): its modifiers and private variables stay inside
```

## How the two compose

For each query, the planner goes through these steps:

1. `SchemaGraphMaterialisation` turns schema regions into `Values`
   (existing behaviour).
2. The narrowing rules (#409) turn `Values` into scan conditions where they
   can (existing behaviour).
3. **M1**: `LowerConstantRelation` flips every remaining `Values` that
   meets K1–K7 to `Sql`.
4. **M2**: R1–R3 lift what is still engine-only above the SQL nodes.
5. Placement and outcome: `UsedAlone` if nothing is left for the engine,
   `UsedRows` if what is left passes the five eligibility checks, `Used`
   if it does not, and `Fallback` if no statement is possible.

M1 before M2 because an SQL join is cheaper than an engine join over
10 000 rows. When M1 declines a `Values` (a mixed column, a key that is
not definitely bound, an untranslatable descriptor), R1 can still lift it
out of an `OPTIONAL` for M2. So M2 is the general case and M1 is the
optimisation on top of it. A query that M1 alone makes `UsedAlone` never
needs M2.

## Limits: the triple ceiling and the row cap

**`max_triples` (500 000).** Its purpose is to bound the memory of the
store. On the `UsedRows` route, the store holds the schema graph only,
which is fixed and already excluded from the count today. The memory is
in the solution rows instead. Proposal: bound the **bound cells** handed
to `sparql_finish` (rows × bound columns, witnesses and ordinal included)
by the same `max_triples`. A bound cell costs no more than the triple it
would have been on the records route, and far fewer are handed over,
because only the demanded columns cross and not the whole record. That
makes `UsedRows` never admit more data than `Used` would.
`sparql_finish` checks the count before evaluating and refuses with the
existing `TripleLimitExceeded` shape, naming cells. Assumable (Q2).

**`MAX_RESULT_ROWS` (10 000).** The statement is read with
`fetchmany(cap + 1)`, as the `UsedAlone` route already does. Whether that
refusal is right depends on the engine region:

* **one-to-one** (every region node passes G3a; after R3 the common case):
  statement rows = answer rows, so over the cap is over the cap. The
  query is refused right after SQL returns, in about the time the
  statement takes, not after a 45-second load.
* **reducing** (`Filter`, `Distinct`, `Group` in the engine): the answer
  may be smaller than the input. The statement is then read with the
  cell budget above as its bound, not the row cap, and the row cap is
  applied to the engine's output as it is today.

The planner knows which case applies, from the same G3a test, and
`EngineInput::Solutions` carries it as `preserves_rows`, so the executor
does not have to work it out.

**`MAX_MATERIALISED_ROWS` (500).** Unchanged. A schema region over 500 rows
is not materialised, so it is an engine read. M2 still handles it (the
engine region reads the schema graph, which is allowed), just not M1.

## The worked example, after both

Applying the steps to the all-CEA query:

| construct | today | after |
|---|---|---|
| 18 label `OPTIONAL`s | 8 islands | M1: each is nested under the `OPTIONAL` that reads its `?x` as a required read, so K1 holds. Each becomes a `LEFT JOIN` on a per-enum constant table (codes → one `nl-be` / `fr-be` label), with uniform columns. As P2 |
| `BIND(STRAFTER(STR(…)))` in an `OPTIONAL` | 2 islands | M2 R2: input `?builtElement` is a required read of the body, not in the outer scope; effect-free; lifted with a witness. As P4 |
| nested Station/Bundle `OPTIONAL`s | 3 islands | **not covered here**: needs the op 5 widening (Staging step 0) |
| `COUNT` sub-select | lowers (#466) | unchanged |
| `ORDER BY ?name` | unclaimed | M2 R3: the engine region is one `Bind`, which is one-to-one; `?name` is a statement column. Ordinal restores the order |
| projection | fetch, not solutions | M2: island root gets a `Project` with bindings |

Outcome: `UsedRows`, with the finish query being one `BIND` and
`ORDER BY ?__ord`. Without a `LIMIT` it still returns 20 563 rows, so it
is refused with `limit_exceeded` / `max_rows` **as soon as the statement
returns 10 001 rows**, instead of after 45 s. With `LIMIT 10000 OFFSET n`
(R3 pushes the slice into SQL), each page is one statement plus an engine
pass over at most 10 000 rows and at most 53 cells each (51 selected, a
witness and the ordinal). Whether that fits the 500 000-cell budget
depends on how many of the 51 columns are bound per asset. That has not
been measured yet (Q2).

This is a projection from the plan, not a measurement. The acceptance test
is the query itself on DEV (see *Tests*). Whether each of the 18 label
`OPTIONAL`s really has a required read of its `?x` in the body will be
checked on the query itself. A body that reads `?x` through an optional
unnest would fail K1, and that label would stay an engine node for R1.

## Where rust PR #58 fits

PR #58 (items B and C) makes the **`Used` route** cheaper. It loads records
in chunks of 64 (299 ms → 83 ms on 2 000 records), and it stops loading
at the triple ceiling instead of after the whole load. That turns the
45-second `limit_exceeded` into a fast one.

This design moves queries *off* that route: an `UsedRows` query loads no
records. PR #58 still matters for every query that stays on `Used`. That
is any query whose engine region reads instance data or cannot be
written back, every query blocked on the nested Station/Bundle join until
Staging step 0, and every shape R1–R3 decline. The two do not conflict
and touch different code (`sparql_execute`'s load loop, against the
planner and a new `sparql_finish`). PR #58 can merge first on its own,
and this PR is based on `main`, not on #58, for that reason.

## Contract and consolidator changes

`PLAN_CONTRACT` 5 → 6 adds `Op::Constant`, `JoinKey::Value`, the
left join's `witness`, `EngineInput` on `EnginePass` (with `ordinal`,
`finish` and `preserves_rows`) and `Refinement::UsedRows`. A consumer
built against 5 refuses a contract-6 plan, as it must. It would render
nothing for a `Constant`, drop a witness column and load records for a
`Solutions` pass.

Consolidator side (asset360/consolidator-server):

* the renderer arms for `Op::Constant`, `JoinKey::Value` and the witness
  column in `build_aggregate_sql_from_ops`.
* the executor branch for `refinement == "used_rows"`: run the statement,
  apply the row cap or the cell budget per `preserves_rows`, serialise
  with `to_sparql_results`, number `?__ord` when `ordinal` is set, and
  call `lr.sparql_finish`.
* the contract check raised to 6.

## Tests that would prove it

* **Per-rule oracle regressions, one per counterexample.** Every entry in
  the appendix becomes a test named there. For a rule that must **fire**,
  the plan is written back before and after the rule (test 2(d) of
  `sparql-scopes-as-relations.md`), and both are evaluated on the
  in-memory oracle over the fixture: bags equal, and sequences equal
  where the query has an `ORDER BY`. For a rule that must **decline**,
  the test asserts that the rule did not fire and that the `declined`
  line names the guard. It also evaluates the rejected rewrite on the
  oracle and asserts it *differs*, which keeps the counterexample honest
  if the fixture changes.
* **Route differential.** Every shape admitted by `LowerConstantRelation`
  or R1–R3 runs on the new route (`UsedAlone` or `UsedRows`) and on the
  engine's `Used` route over the same records, and the answers must
  agree term for term. This is the discipline `UsedAlone` shapes already
  follow.
* **Ordering.** `r3_qualifying_page` pages an ordered query with a
  total-order key through `OFFSET 0, n, 2n, …`, and the concatenation must
  equal the oracle's full sequence. With a non-unique key, each page's
  multiset of sort-key values must equal the oracle's, which is page
  membership up to ties.
* **Composition, not only rules in isolation.** Two full-pipeline
  regressions run `plan_query_refined` with the whole rule list and
  check the printout as well as the answer:
  * `r3_after_r2_one_island`: P4. R2 fires first and leaves one island.
    R3 then fires with `I` 1 → 1 and `S` 2 → 0, and **it is retained**.
    The statement carries the `ORDER BY` and `LIMIT`. The finish query
    has neither, and ends in `ORDER BY ?__ord`. Answers equal the
    oracle's as a sequence, and the refine log shows R2 before R3 with
    `reached_fixpoint`.
  * `r3_ordinal_through_projections`: P4 with its `E` widened so that
    the path from the island root to the restoring sort holds R2's `π₋ₘ`
    and a `SubSelect∅` over a qualifying `LeftJoin` with a `Values`
    (G3a). It asserts `?__ord ∈ scope(n)` for every node on that path.
    Only the final projection drops it. The answer equals the oracle's
    as a sequence. Its mutation twin builds `π₋ₘ` as an explicit keep
    list, and must fail `ordinal_reaches_its_sort` with a
    `PlanDefect::Transition` naming R2.
* **Effects.** `r1_volatile_condition` asserts that R1 declines on G1d,
  and nothing about random values. `r1_effect_free_condition` and
  `r3_volatile_bind_in_e` must fire. The second compares only the
  deterministic columns.
* **Eligibility.** `m2_exists_in_bind_reads_instances` must plan `Used`,
  with a reason. A region whose write-back is `None` must plan `Used`,
  never `UsedRows` followed by a failure. A `BASE` query using `IRI()` in
  the region must give the records route's answer.
* **A mixed-column `Values`** (IRIs and literals in one column) must stay
  engine-side and still answer through M2.
* **Alpha-renaming** as in *Scopes, barriers and renaming*.
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
1. **M1**: `Op::Constant`, `JoinKey::Value`, `LowerConstantRelation` with
   K1–K7, the `declined` printout, the renderer arm, and the M1
   regressions. Useful alone: every query whose only gap is a schema
   lookup whose key is bound becomes `UsedAlone`.
2. **M2**: the witness, `EngineInput::Solutions`, `UsedRows`, the
   synthesised island projection, the eligibility checks, R1–R3 with
   their transition checks and regressions, the ordinal, `sparql_finish`
   and the executor branch.

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
* **Q3 — scope of R1–R3.** *Assumable.* Assumption: these three rewrites,
  with the guards above, and nothing more. The declines listed under each
  are deliberate. Two could become constructions if a query needs them:
  an outer left-join condition in R1 (split it by the variables it
  reads), and a cross-product constant in M1 (`JoinKey::Cross`). Each
  would be added only with a counterexample and an oracle regression.
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
* **Q7 — how the rows enter oxigraph.** *Assumable.* Assumption: as the
  `VALUES` placeholder in `finish`. With the ordinal (*The ordering
  contract*), the entry order cannot affect the answer, so switching to an
  injected solution iterator, if building a 10 000-row inline table turns
  out to be measurably slow, changes neither the contract nor the answer.

## Appendix: the counterexamples

Every probe runs on PyOxigraph **0.5.11**, the version this crate
declares, with no data unless stated. "Original" is the query as written.
"Rewrite" is what the named guard admitted, or what the revision 2/3
construction produces. Each row is a planned regression. The revision 3
rows (`r1_volatile_*`, `r1_effect_free_condition`,
`r3_volatile_bind_in_e`) were each run five times, because what they show
is the spread of a random draw. The table cites those draws. The planned
tests assert only the decline, or the deterministic columns.

| regression | rule | original | rewrite | result on Oxigraph |
|---|---|---|---|---|
| `r1_bound_j_tests_merged_mapping` | R1 rev 1 | `VALUES ?a {1} OPTIONAL { VALUES ?j {2} FILTER(false) OPTIONAL { VALUES (?j ?label) {(2 "label")} } }` | `… OPTIONAL { VALUES ?j {2} FILTER(false) } OPTIONAL { VALUES (?j ?label) {(2 "label")} FILTER(BOUND(?j)) }` | differ: `{a}` vs `{a, j, label}` |
| (same, fixed) | R1 rev 2 | same | `… OPTIONAL { VALUES ?j {2} FILTER(false) BIND(true AS ?__m) } OPTIONAL { VALUES (?j ?label) {(2 "label")} FILTER(BOUND(?__m)) }` | equal: `{a}` |
| `r1_empty_shared_set` | R1 | `VALUES ?a {1} OPTIONAL { VALUES ?b {2} FILTER(false) OPTIONAL { VALUES ?label {"x"} } }` | rev 1 (no conjunct): differ, `{a}` vs `{a, label}`. Rev 2 witness: equal | as stated |
| `r1_c_shares_with_a_not_pinned_by_b` | R1 G1b | `VALUES (?a ?k) {(1 7)} OPTIONAL { VALUES ?j {2} OPTIONAL { VALUES (?j ?k ?label) {(2 8 "label")} } }` | witness rewrite without G1b | differ: `{a, k}` vs `{a, k, j}`, so G1b must decline |
| `r1_inner_condition_reads_a` | R1 G1c | `VALUES ?a {1} OPTIONAL { VALUES ?j {2} OPTIONAL { VALUES (?j ?label) {(2 "label")} FILTER(!BOUND(?a)) } }` | witness rewrite without G1c | differ: `{a, j, label}` vs `{a, j}`, so G1c must decline |
| `m1_unbound_key_joins_every_row` | M1 K1 | data `<urn:s1> <urn:type> <urn:A>`. `?s <urn:type> <urn:A> OPTIONAL { ?s <urn:j> ?j } OPTIONAL { VALUES (?j ?label) {(<urn:outside-enum> "label")} }` | the table with the outside-enum row pruned | differ: `{s, j, label}` vs `{s}` |
| `m1_undef_key_cell` | M1 K2 | data `<urn:s1> <urn:j> <urn:E1>`. `?s <urn:j> ?j OPTIONAL { VALUES (?j ?label) {(UNDEF "any")} }` | SQL `NULL = v` semantics (the key never matches) | differ: `{s, j, label}` vs `{s, j}` |
| `r2_input_from_a` | R2 G2b | `VALUES ?a {10} OPTIONAL { VALUES ?b {2} BIND(?b + ?a AS ?x) }` | `… OPTIONAL { VALUES ?b {2} } BIND(?b + ?a AS ?x)` | differ: `{a, b}` vs `{a, b, x=12}` |
| `r2_non_strict_expression` | R2 rev 2 | `VALUES ?a {10} OPTIONAL { VALUES ?b {2} FILTER(false) BIND(COALESCE(?b, 0) AS ?x) }` | witness + `IF(BOUND(?__m), COALESCE(?b, 0), ?__never)` | equal: `{a}`. Rev 1 had to decline this |
| `r2_bnode_once_per_b_row` | R2 G2c | `SELECT (COUNT(DISTINCT ?x) AS ?n) { VALUES ?a {1 2} OPTIONAL { { SELECT ?x { VALUES ?b {7} BIND(BNODE() AS ?x) } } } }` | `… OPTIONAL { VALUES ?b {7} } BIND(BNODE() AS ?x)` | differ: `n=1` vs `n=2` |
| `r2_qualifying_strafter` | R2 rev 2 | data `<urn:s1> <urn:j> <urn:E1>`. `?s <urn:j> ?j OPTIONAL { ?s <urn:j> ?e BIND(STRAFTER(STR(?e), "urn:") AS ?t) }` | witness + `IF(BOUND(?__m), …, ?__never)` | equal: must fire |
| `r3_undef_left_key_fans_out` | R3 G3a | `VALUES (?id ?j) {(1 UNDEF) (2 3)} OPTIONAL { VALUES (?j ?label) {(1 "a") (2 "b") (3 "c")} } ORDER BY ?id LIMIT 1` | `{ SELECT … ORDER BY ?id LIMIT 1 } OPTIONAL { … }` | differ: 1 row vs 3 rows |
| `r3_qualifying_page` | R3 rev 2 | `VALUES (?id ?j) {(3 2) (1 3) (2 1)} OPTIONAL { VALUES (?j ?label) {…} } ORDER BY ?id LIMIT 2` | `VALUES (?__ord ?id ?j) {(1 1 3) (2 2 1)} OPTIONAL { … } ORDER BY ?__ord` | equal as a **sequence** |
| `p2_join_reorders_values` | ordering | 40 rows `VALUES (?id ?k)` in descending `?id` | the same rows inner-joined to a 7-row `VALUES (?k ?label)`, no `ORDER BY` | differ as a sequence: grouped by `?k`. Through an `OPTIONAL` the order happened to survive, which is not a contract |
| `r1_volatile_condition` | R1 G1d | `VALUES ?a {1 … 40} OPTIONAL { VALUES ?j {2} OPTIONAL { VALUES (?j ?label) {(2 "label")} FILTER(RAND() < 0.5) } }` | witness rewrite: `OPTIONAL { VALUES ?j {2} BIND(true AS ?m) } OPTIONAL { VALUES (?j ?label) {(2 "label")} FILTER(BOUND(?m) && RAND() < 0.5) }` | bound labels over five runs. Original: 0, 40, 40, 0, 40, always all or none. Rewrite: 22, 20, 21, 23, 27, which the original cannot produce. So G1d must decline (the review's counterexample) |
| `r1_volatile_inside_c` | R1 G1d | the same, with the `FILTER(RAND() < 0.5)` inside a group in `C`: `OPTIONAL { { VALUES … FILTER(RAND() < 0.5) } }` | witness rewrite, `C` unchanged | both all or none (40, 0, 0, 40, 40 vs 40, 0, 40, 40, 0). Oxigraph evaluates `C` once on both routes, which is what SPARQL says. G1d declines anyway, so that the proof does not rest on it |
| `r1_effect_free_condition` | R1 G1d | the same, with `FILTER(STRLEN(?label) = 5)` | witness rewrite | equal: 40 bound labels every run. Must fire |
| `r3_volatile_bind_in_e` | R3 | `VALUES ?id {3 1 2 4} BIND(RAND() AS ?r) ORDER BY ?id LIMIT 2` | `{ SELECT ?id { VALUES ?id {3 1 2 4} } ORDER BY ?id LIMIT 2 } BIND(RAND() AS ?r)` | `?id` sequence `1, 2` on both, with a fresh `?r` per row on both. R3 changes which rows `E` evaluates, not how often per row. No flaw, and it must fire |
| `m2_exists_in_bind_reads_instances` | M2 | data `<urn:s1> <urn:p> <urn:v>`. `VALUES ?s {<urn:s1>} BIND(EXISTS { ?s <urn:p> ?v } AS ?found)` | the same query over a store without the instance triple (the finish store) | differ: `found=true` vs `found=false` |

A `r3_duplicate_key_in_c` regression (two `C` rows with the same key)
completes the R3 set. It needs no engine probe: the proof fails on the
table alone, and the test asserts the decline.

# A body is a relation: lowering a grouped sub-select and an `OPTIONAL` body as one derived table

Status: **design, nothing built.** Draft PR for review; the two issues it
answers stay parked until the design is agreed. **Revision 2**, after
[review round 1](https://github.com/Kapernikov/asset360-rust/pull/49#issuecomment-5732197215):
not implementation-ready until the derived-property contracts of "What a
relational subtree derives" are agreed.

<details><summary>What revision 2 changed, point by point</summary>

* **Op 3 was two transformations sold as one.** The semi-join argument
  restricts the *completed* relation at the join boundary; carrying that
  restriction down to a scan crosses every operator in between, and
  `ORDER BY ?s LIMIT 1` inside the body changes the answer (the review's
  counter-example, now the first test). Op 3 is split: **3a** places a
  restriction at the scope root with the boundary proof, **3b** pushes it
  down one operator at a time with a commutation rule per operator and a
  hard stop at `Slice` and a keyless `Group`. `narrowed_by_interface` is
  replaced by the obligation the boundary filter raised, so the provenance
  is a ledger entry, not a flag. The `LeftJoin` match says *right side
  only*. Open question 4 is closed by this, not left as a widening.
* **`certainly_binds` is a derived output property with a transfer function
  per operator**, not a list of node kinds. A group key is bound only if its
  input guarantees it; a measure only if its aggregate guarantees it; a
  scan identity stops being guaranteed once it passes through the right
  side of a `LeftJoin`. Term identity and binding guarantee are two
  properties. Op 5 checks both.
* **`applies_to_every_answer` is scope-local**, and crossing a barrier is a
  separate proof (the boundary proof of 3a) that the *consumer* asks. The
  fallback narrowing (`keep_what_the_rules_proved`) indexes scans by
  producer node, not by variable name — the review's hidden-`?s` COUNT
  example is the second test, and it is run with the SQL lowering
  deliberately refused so the fallback route is the one under test.
* **The interface is a list of producer slots, not names**, and a scope has
  *exports* and *correlated inputs* as two lists. `AntiJoin` and `Minus`
  right sides export nothing; `AntiJoin` reads outer bindings. Closure is
  checked on producer slots; predicate placement is the obligation ledger's
  job, and the "structurally unreachable" claim about the 18-vs-17 trap is
  withdrawn for the correct one. `?cs` and `?a` in the #464 interface are
  fixed, and the interface is derived by `Scope::of`, never written by hand.
* **Op 2 has an expression-effects precondition**:
  `Expr::evaluates_the_same_out_of_context`, the predicate 28h and
  `sparql_materialise.rs` already use, and it is the rule's contract rather
  than a renderer's later refusal.
* **Op 5's empty-key case** is stated: `on = []` is a cross join with no
  boundness requirement.
* **The optional-body boundary is uniform (op 1 always), and absorption
  versus derived table is a lowering choice**, not a rule-order accident.
  Open question 2 is closed that way.
* **Every relational subtree derives the same five properties**, and the
  rules consume them; the "same vocabulary" table gains the review's nested
  case (two grouped sub-selects joined inside a third).
* **The test plan** gains an independent oracle (the original query on the
  full fixture, in memory), bag comparison with unboundness and
  multiplicity, order compared only where defined, and the review's grammar
  extensions.

</details>

Refs:
* [#466 (pepibru GitLab)](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/466)
  — a grouped sub-select on the driving variable is refused
  (`aggregate_not_pushable`).
* [#464 (pepibru GitLab)](https://gitlab.pp.kapernikov.com/asset360/consolidator-server/-/issues/464)
  — an `OPTIONAL` through an inline array to a record costs ~15 ms per row
  in the engine.
* consolidator-server `doc_book/src/design/28d-plan-refinement.md` — the
  planner this extends: the naive plan, the rules, the invariants, the
  "collapse fully, or fall back" decision. Cited, not restated. Section
  names below are its section names.
* consolidator-server `doc_book/src/design/28h-sparql-plan-semantics.md` —
  the plan vocabulary.

## The spine, in the words that set it

> the general design must remain this:
>
> * sparql query is translated in a plan
> * the plan is lowered to engine ops (sql? in memory?) using equivalence
>   ops. equivalence op translates a query plan to a query plan with
>   something lowered while assuring the new queryplan is equivalent.
>
> my main concern is that the planner ops must remain composable. its very
> easy to get the planner design wrong and introduce a lot of code just to
> support one extra query in the literal shape we now think of, or worse
> (wrong results)

Two tests every section below is held to:

1. **Nothing here is written for a query shape.** Every op is stated as a
   precondition on a plan and an equivalence argument, and the two issues are
   *worked examples* of the same vocabulary — the last section shows the same
   ops applying to shapes neither issue mentions.
2. **A wrong answer is impossible without a rule lying about its precondition,
   and a lie fails an invariant.** Each op names the invariant that would
   catch a bad application of it, in the style 28d calls "an invariant can
   only catch what the representation makes local".

## The two shapes, and the one fact the plan cannot state

### #466 — a count beside a row

```sparql
SELECT ?name ?trackQty WHERE {
  ?a a asset360:CivilEngineeringAsset ;
     asset360:hasCivilEngineeringAssetType asset360:CEAssetType\#Tunnel ;
     asset360:identification ?name .
  { SELECT ?a (COUNT(?cs) AS ?trackQty) WHERE {
      ?a a asset360:CivilEngineeringAsset ; asset360:hasCoveredSection ?cs
    } GROUP BY ?a }
}
```

The naive plan (`sparql_refine.rs`, `Builder::pattern`) is the algebra
faithfully: the inner block is `match`/`join` → `group keys=[?a]
count(?cs)` → `bind ?trackQty` → **`subselect [?a, ?trackQty]`**, joined by a
`join on=[?a]` to the outer star's matches, under the root `project`. The
sub-select is built with `on_spine = false`, which is what makes it a
`SubSelect` node and not a `Project` — 28d's "barrier the variables above
it cannot see through".

What the rules do with it today, each a fact in the code rather than a
judgement:

* `FoldMatchesIntoScan` folds **neither** star. It counts the `rdf:type`
  matches on a star variable across the whole plan and declines when the
  count is not one (`classes_on_star != 1`); `?a a :CivilEngineeringAsset`
  is written twice — once outside, once inside the sub-select — so the
  variable looks like a star typed twice, and nothing folds. That is the
  "outer star is not even scanned in the refined plan" of the parked note:
  the scoper's fetch carries it (it merges the two identical types into one
  star), the refined plan does not.
* `PushGrouping` is **root-anchored**: `grouping_tail` walks single
  consumers upward and returns `None` unless the chain reaches
  `plan.nodes.len() - 1` through Bind/Filter/Sort/Distinct/Slice/Project
  only. A `SubSelect` above the group, or a `Join`, ends the walk. The
  grouping stays `[E]`.
* `PushProjection` bails when **any** `Group`, `Union` or `SubSelect` exists
  anywhere in the plan (`sparql_rules.rs`, `PushProjection::apply`).
* `lower_refined` has no arm for `SubSelect` (`LoweringRefusal::UnknownOperator`),
  and two `[S]` scans under an `[E]` join are `SeveralIslands`.

So the refined plan for this query has no `[S]` node at all (`NothingPushed`
— and had the stars folded, two islands the lowering refuses), and the engine leg's gate
(`planning.fallback_can_answer`) refuses to materialise the class:
`422 aggregate_not_pushable: group GROUP BY ?a (not lowerable: no operator
runs in SQL)`.

### #464 — an `OPTIONAL` that reads three slots of an array element

```sparql
SELECT * WHERE {
  ?a a asset360:CivilEngineeringAsset ; asset360:belongsToSubZone <…> ;
     asset360:identification ?name .
  OPTIONAL {
    ?a asset360:hasCoveredSection ?cs .
    ?cs asset360:isReference true ; asset360:belongsToTrack ?track ;
        asset360:hasEntryPointM ?kpFromMeter .
    ?track a asset360:Track ; irsm:name ?trackName ; irsm:longname ?trackCode .
    OPTIONAL { ?track asset360:hasTrackType ?trackDiscr }
  }
}
```

Correct today, on the engine, at 17 s for 1 167 assets. The fetch is one
`LEFT JOIN` through a path edge (`fetch_joined`, 0.37 s); the engine then
re-walks every covered section of every fetched asset through oxigraph's
memory store. #464's thread names why no rule takes the block, and both
reasons are the *same* missing fact:

* **the two-read `OPTIONAL`.** `AbsorbOptionalRead` takes an optional side
  that is exactly one `match` reading one slot of the preserved star, and
  delivers it as one nullable column. Three reads of `?cs` cannot be three
  nullable columns: SPARQL binds `?kpFromMeter`, `?track` and the
  `isReference` test **together or not at all**, and three independent
  `NULL`s cannot say "together". 28d lists this as "still declined, and
  worth their own rule: an `OPTIONAL` reading two slots of one star".
* **a reference held by an element, not a star.** `?cs :belongsToTrack ?track`
  hangs off an unnested element of `?a`'s payload. `foreign_key_on` finds a
  reference only on a `Scan` slot of depth one, so `AbsorbOptionalReference`
  and `PushLeftJoin` never see the edge.

### The missing fact

Both are the plan being unable to say **"this body is evaluated as a unit,
and its interface is these variables."** SPARQL says exactly that of every
group graph pattern: an `OPTIONAL { … }` body, a sub-select, a `UNION` arm,
a `NOT EXISTS` block are each evaluated bottom-up to a solution multiset,
and only then combined with the outside on the variables they share. The
plan has one node that states it — `PlanOp::SubSelect` — and every rule
that looks across it (`Visible::below` via `plan.feeds`,
`applies_to_every_answer`) treats it as transparent, while the rules that
would use it (`PushGrouping`, `PushProjection`) treat it as a stop sign.

SQL has the same unit: a **derived table** `( SELECT … ) AS q`, joined on
its columns. A body that is entirely SQL renders as one; a `LEFT JOIN` to it
is SPARQL's `LeftJoin` with the "together or not at all" for free, because
a derived-table row either exists or it does not. That is the whole design:
**make the unit a first-class fact of the plan, and let every existing rule
run inside it unchanged.**

## The vocabulary

One representational fact, five derived properties every relational
subtree carries, one lowering target, and five equivalence ops (one of them
in two halves). Three of the five are new rules; two are widenings of rules
that exist, each a precondition made precise rather than a branch added.
Nothing in the list names a predicate, a class, or a query.

### The fact: a scope

```rust
/// Where a subtree is a unit of its own: evaluated to solutions, then
/// combined with the outside through its interface and nothing else.
///
/// Computed by `Scope::of(plan, root)`, never written by hand. A `SubSelect`
/// node's input is one; so is the right side of a `LeftJoin`, of an
/// `AntiJoin`, of a `Minus`, and each arm of a `Union`. `Plan::scope_of(node)`
/// answers which scope a node is in, and `Plan::scope_root(scope)` its root.
pub struct Scope {
    pub root: NodeId,
    pub kind: ScopeKind,
    /// What the outside may read: a variable *and the slot that produces it*.
    /// Two scopes binding `?a` are two producers; a reference from outside
    /// resolves to one of them or to nothing, never to "some `?a`".
    pub exports: Vec<Export>,
    /// What the body reads from the outside. Empty for every kind but a
    /// `Testing` scope under `AntiJoin`.
    pub correlated_inputs: Vec<String>,
}
pub struct Export { pub var: String, pub producer: OutputSlot }
pub struct OutputSlot { pub node: NodeId, pub var: String }
pub enum ScopeKind {
    /// Rows come out and are joined: `SubSelect`, `LeftJoin.right`, a `Union` arm.
    Exporting,
    /// Rows are only tested against: `AntiJoin.right` (correlated — `EXISTS` is
    /// defined on the current solution, §18.6), `Minus.right` (uncorrelated,
    /// compatibility on the shared domain). Exports nothing.
    Testing,
}
```

`exports` for a `SubSelect` is its `vars`, each resolved to the body's
producer; for a `LeftJoin` right side and a `Union` arm, every variable the
body binds, each with its producer. A `Testing` scope exports nothing —
neither `NOT EXISTS` nor `MINUS` puts a right-side binding into the
surrounding result — and this document lowers none of them: an
independently evaluated derived table is the contract of an `Exporting`
scope only, and a `Testing` scope's lowering (a correlated `NOT EXISTS
(SELECT …)`) is its own op with its own proof, listed in the table as a
follow-up and no more. A scope's root is its topmost node; the plan root is
the root of the outermost scope.

### What a relational subtree derives

Every scope root, and every node, answers the same five questions, each an
analysis with a transfer function per operator. Rules consume the answers;
none of them re-walks the plan for descendants. That is the composability
claim in a form that can be tested: a new operator adds five arms to five
functions, and every rule that reads them applies to it without knowing it
exists.

| property | question | transfer, per operator |
|---|---|---|
| `outputs(n)` | which variables may be bound at `n`'s output | `Match`/`Scan`/`Unnest`/`Values`/`Path`: what they bind. `Join`/`LeftJoin`: union of both sides. `Union`: union. `Minus`/`AntiJoin`: the left side. `Filter`/`Sort`/`Distinct`/`Reduced`/`Slice`: input. `Bind`: input + the bound variable. `Group`: keys + measures. `Project`/`SubSelect`: `vars`. |
| `guaranteed(n)` — *certainly bound* | which of `outputs(n)` are bound in **every** solution | `Match`/`Path`: all. `Scan`: identity + `Required` slots. `Unnest` under a required read: the element. `Values`: rows with no `UNDEF` in that column. `Join`: union of both sides. **`LeftJoin`: the left side only.** `Union`: the intersection of the arms. `Filter`/`Sort`/`Distinct`/`Reduced`/`Slice`: input. `Bind`: input, plus the variable iff the expression is total over guaranteed inputs (an arithmetic on a guaranteed variable; not a `Bind` that can error). **`Group`: a key iff it is a variable guaranteed by the input, or an expression total over guaranteed inputs; a measure iff its aggregate guarantees a value** — `COUNT` always; `MIN`/`MAX`/`SUM`/`AVG` only when the aggregated expression is guaranteed and cannot error over the group's rows; keyless `Group` measures over an empty input are `COUNT`=0 and the rest unbound. `Project`/`SubSelect`: intersection with `vars`. |
| `term_of(n, ?v)` | what kind of term `?v` is, when bound | `Identity(class)` for a scan's star; a slot binding (path, reading, presence); `Measure`; `Structure` for an unnested element; a relation column carries its body's answer through a pushed barrier; `Ambiguous` when two producers disagree (a `Union` of two classes). This is today's `Visible::identity_of` / `slot_of` restated as a derived property with a transfer rule for the barrier. |
| `correlated_inputs(n)` | which outer variables the subtree reads | Empty everywhere except a `Testing` scope under `AntiJoin`, where it is the variables the right side shares with the left. A rule that lowers a scope as an independent relation requires it empty. |
| `effects(e)` for an expression | may `e` be evaluated in a different place, or a different number of times, and mean the same | `Expr::evaluates_the_same_out_of_context` today: pure → yes; `RAND`/`UUID`/`STRUUID`/`BNODE`/`NOW`/`IRI`, a custom function, an `Opaque` subplan → no. Any rule that moves an expression across an operator checks it. |

Two of these are the ones the first revision got wrong: `guaranteed` was a
list of node kinds ("a group key or measure" was on it, and a `GROUP BY ?k`
over an optional `?k` keeps the unbound-key group, §18.5 Group allows an
error in a key); `term_of` and `guaranteed` were conflated, so a scan's
identity looked bound at every consumer, including the one below a
`LeftJoin` right side where it is not. **Term identity and binding
guarantee are two properties, and a rule that needs both asks both.**

Four things change to read the scope, and they are the whole "let existing
rules run inside" claim:

* **A star variable is a star per scope.** `?a` outside the sub-select and
  `?a` inside it are two rows of the same table that the join makes equal;
  SPARQL says so (the inner `?a` is a variable of the inner query, exported
  by name), and SQL says so (`t0` and `t1`, joined on identity).
  `FoldMatchesIntoScan`'s `classes_on_star` count becomes a count *within
  the scope*, and each scope gets its own scan. This is the change that
  makes #466's two stars fold at all, and it is a precondition being made
  precise rather than removed: two types on one variable **in one scope**
  still decline.
* **`grouping_tail` and `projection_tail` walk to the scope root, not the
  plan root.** The one line each that reads `current == plan.nodes.len() - 1`
  becomes `current == plan.scope_root(scope)`. `PushGrouping` then takes a
  grouping inside a sub-select exactly as it takes the query's own; a
  `SubSelect` above the chain is the scope's projection, accepted where a
  `Project` is. `PushProjection`'s blanket "any `SubSelect` in the plan"
  bail becomes "a `Group`/`Union`/`SubSelect` **in this scope**".
* **`Visible` stops at a scope boundary and reads the barrier's columns
  instead.** `Visible::below(plan, base)` today enumerates every `Sql`
  `Scan` that `feeds` `base`, through anything. With scopes: a scan inside a
  *pushed* barrier is invisible above it, and the barrier contributes one
  `Column` per export, typed by `term_of` at the scope root and flagged by
  `guaranteed` there — `Identity(class)` when the body's scan bound it, a
  slot binding (path, reading, presence), `Measure` for an aggregate.
  `identity_of(?a)` above a pushed sub-select therefore answers the inner
  star's class, which is what lets a join key on it, and `slot_of(?a)`
  stays ambiguous, so a filter on it still declines. A scan inside an
  *unpushed* barrier stays invisible too: nothing outside may address it,
  which is what "barrier" meant all along.
* **`applies_to_every_answer` becomes scope-local, and the barrier is where
  it stops.** Revision 1 said "unchanged: a `SubSelect` on the mandatory
  side of a `Join` passes its rows through". That is false as a statement
  about a *variable*, and the review's example says why:

  ```sparql
  SELECT ?s ?n WHERE {
    ?s a :C .
    { SELECT (COUNT(*) AS ?n) WHERE { ?s a :C ; :p 1 } }
  }
  ```

  The inner `?s` is hidden. `:p 1` decides the count, not which outer `:C`
  rows survive; two `:C` records, one with `:p 1`, must answer two rows
  with `?n = 1`. Yet `applies_to_every_answer` today answers *true* for the
  `:p 1` filter (nothing above it is a `LeftJoin`/`Union`/`Minus`/`AntiJoin`),
  and `keep_what_the_rules_proved` then keys the filter on the star's
  **name** `?s` and hands it to the fetch of the outer `?s` — one answer
  lost. Two facts, two fixes:

  * `applies_to_every_answer(plan, node)` answers **for the scope `node` is
    in**: does the constraint decide every solution the scope emits. A
    keyless `Group` is added to its non-passing list beside `LeftJoin.right`
    — it emits one row whether or not its input has any — and the walk
    stops at the scope root. Whether that restriction also decides the
    *enclosing* scope's answers is a second question with a second proof:
    the boundary proof of op 3a, read in the other direction (exported,
    guaranteed, a `Join` side or the right of a `LeftJoin` is not enough —
    only a `Join` side, and nothing between the constraint and the root
    that fails 3b's commutation). A consumer that wants to carry a
    restriction across a barrier asks both.
  * `keep_what_the_rules_proved` indexes scans by **producer node**, and
    the fetch it narrows is the fetch of *that* scan. A restriction whose
    producer lives in another scope reaches the outer fetch only through
    the boundary proof, and a restriction that rests on a variable the
    scope does not export never does. 28h's distinction — valid in the
    refined plan, invalid in the reconstructed fetch — is exactly this,
    and it is tested with the SQL lowering deliberately refused so the
    fallback route is the one carrying the narrowing.

The invariants that make it local, added to `Plan::check()`:

* **Scope closure, on producer slots.** Every variable reference outside
  a scope (`Expr::vars()` of every filter, sort term, key, measure, and
  `on` list) resolves to an `Export.producer` of a scope visible from that
  node or to a producer in its own scope — and to exactly one. Comparing
  name strings against a name list cannot tell a reference to an exported
  relation column from a reference to the inner scan that happens to bind
  the same name; the producer slot can. A rule that resolves an inner
  scan's column from outside fails here rather than rendering
  `t1.object_data->>'x'` for a row that is not `t1`'s.
* **Predicate placement is the obligation ledger's, not closure's.**
  Closure does not stop a rule from moving an optional-side filter to the
  outer `WHERE`: optional variables are exported, so the reference
  resolves. What stops it is that the filter's obligation was raised
  *inside* the `OPTIONAL` (`o_n` carries where it was written, and 28d's
  `optional_side` says which side must not lose rows), and a rule that
  reparents it outside its scope fails `ledger_balances`' sibling,
  **obligations stay in their scope**: an obligation raised in scope *S*
  is discharged by a node in *S* or by the node that combines *S* with the
  outside (the `LeftJoin`'s `condition`, for a lifted one), never above.
  Revision 1's "structurally unreachable" claim about the 18-vs-17 trap
  is withdrawn in favour of this, which is checkable.

### The lowering target: a relation

```rust
/// A derived table: the body rendered as a statement of its own, joined by
/// column.
Op::Relation {
    body: OpTree,                  // rendered recursively, own alias space
    alias: String,                 // q0, q1, …
    columns: Vec<RelationColumn>,  // one per export
}
/// `guaranteed` is `guaranteed(body root)` for this export, carried so a
/// consumer above the barrier reads a fact rather than re-deriving one it
/// cannot see into.
pub struct RelationColumn { pub var: String, pub kind: ColumnKind, pub descriptor: TermDescriptor, pub guaranteed: bool }
pub enum ColumnKind { Identity { class_uri: String }, Slot(BindingSpec), Measure }

/// `Op::Join` gains a key beside the reference edge it has today.
pub enum JoinKey {
    /// `holder.object_data->>'slot' = referenced.asset360_uri`: today's edge.
    Reference(JoinEdge),
    /// `left.<col> = right.<col>`: two identities of the same class, or a
    /// relation column against a star's identity.
    Identity { left: ColumnRef, right: ColumnRef },
    /// `on = []`: every pair. `CROSS JOIN`, or `LEFT JOIN … ON true`.
    Cross,
}
```

`PLAN_CONTRACT` 4 → 5, so a renderer that predates `Relation` refuses the
plan by version instead of dropping the node (`plan_ops.stars_and_joins_from_ops`
logs and drops an unknown operator kind on the *fetch* route; the statement
route already refuses by name, and the contract bump makes both refuse).

**The renderer's one addition** (`sql_builder.py`): a FROM item that is not a
table. `_from_join_where` today knows tables (`t{i}`) and lateral unnests; it
gains `( <statement> ) AS q{i}` where `<statement>` is
`build_aggregate_sql_from_ops(body)` — the existing statement renderer,
called recursively with its own alias space and its parameters spliced in
order. The join condition for `JoinKey::Identity` is one equality on two
named columns. No new expression vocabulary: the body's `GROUP BY`,
`HAVING`, `DISTINCT`, `ORDER BY … LIMIT`, `count(col) < count(*)`
unbound rule, `array_agg(DISTINCT …)` term-lexical distinctness — all of it
is the renderer that already exists, rendering a smaller statement.

**Island analysis becomes per scope.** An island root is an `Sql` node no
`Sql` node *in the same scope* reads; a pushed barrier is a leaf of its
parent's island. `SeveralIslands` is then asked of each scope, and a scope
whose barrier is `[E]` is not counted in its parent — its own island, if
any, is a fetch the scoper still carries (unchanged from today).

**Optionality is per scope too.** `lower_refined` marks every node feeding
the right side of a pushed `LeftJoin` as optional (`Op::Scan::is_optional`,
`Op::Filter::optional_side`), and the renderer moves an optional star's
conditions into the `ON`. Under a left-joined **relation**, the optional
thing is the relation; the scans *inside* its body are mandatory within
their own statement, and their conditions are that statement's `WHERE`.
That is not a special case of the rule but the rule applied one scope
down, and it is the reason the two-read problem disappears (see #464).

**Precedent in the renderer.** `_from_join_where` already emits one derived
table: the bounded driving scan `(SELECT t0.* … ORDER BY t0.asset360_uri
LIMIT n) t0` on the fetch route. What is new is a derived table whose body
is a *rendered statement* rather than a bounded copy of a table — the
recursion, not the syntax.

### Op 1 — `EncloseOptionalBody` *(new rule; an identity projection)*

**Match.** A `LeftJoin { right }` whose `right` is not already a `SubSelect`.

**Edit.** Insert `SubSelect { input: right, vars: vars(right) }` between
them.

**Equivalence.** `Project(R, vars(R)) ≡ R`: a projection of every variable
a pattern binds is the pattern. SPARQL evaluates the `OPTIONAL` body
bottom-up (§18.2, group graph patterns), so the body is already a unit and
the node only states it. The one thing that is *not* inside the body is
the condition spargebra lifts out of `OPTIONAL { … FILTER(x) }`, which
stays on the `LeftJoin` — see op 2.

**What it changes.** Nothing that answers; it states the boundary SPARQL
already evaluates, so that op 4 can lower it and every other rule can read
its exports. **It runs first, on every `LeftJoin`, unconditionally** — the
logical plan has one shape for an `OPTIONAL` body, and what to *do* with
that body is decided after. Revision 1 ran it late, after
`AbsorbOptionalRead`, `AbsorbOptionalReference` and `PushLeftJoin`, so
those kept their cheaper statement (a nullable column beats a derived
table) and the barrier caught what they declined. The review's objection
holds: that makes the logical boundary depend on rule order, and
"reversing one rule list still lowers" is evidence, not a proof that the
rules compose across a boundary they were not written to see. So:

* **Logical boundary, uniform.** Every `LeftJoin.right` is a `SubSelect`
  after op 1, always.
* **Physical choice, at lowering.** A pushed barrier whose body is one
  `Scan` reading one slot of the preserved star lowers as **absorption** —
  the nullable column `AbsorbOptionalRead` emits today; one whose body is
  one `Scan` joined to the preserved star by a reference lowers as the
  `LEFT JOIN … ON` `AbsorbOptionalReference`/`PushLeftJoin` emit; any
  other pushed barrier lowers as `Op::Relation`. The three are equivalent
  statements of the same logical plan, chosen by `lower_refined` from the
  body's *shape after refinement*, and the three absorb rules become the
  match arms of that choice rather than rules that fire before a boundary
  exists. Their preconditions do not change; where they are checked does.
* **The baseline diff is then honest about what moved.** The 114 frozen
  shapes must show *zero route changes and the same SQL text* for every
  shape the absorb rules served — same statement, reached through a
  barrier — and that is the assertion, not "the order did not matter".

**Would a bad application be caught?** A barrier with the wrong `exports`
fails *scope closure* the moment anything above reads a dropped variable;
`Scope::of` derives them, so the only way to get them wrong is a bug in
one function with one test.

### Op 2 — `SinkLiftedCondition` *(new rule; a filter moves into the unit)*

**Match.** `LeftJoin { right: SubSelect(body), condition: Some(c) }` where
every variable of `c` is in `guaranteed(body root)` **and
`effects(c)` says `c` may be evaluated elsewhere** —
`Expr::evaluates_the_same_out_of_context(c)`, the predicate 28h's five
effect classes collapse onto today.

**Edit.** `condition = None`; `body := Filter(body, c)`; the obligation
moves from the left join to the new filter (it stays in the scope it was
raised in, so *obligations stay in their scope* holds).

**Equivalence.** SPARQL §18.5: `LeftJoin(Ω1, Ω2, c) = { merge(μ1, μ2) |
compatible, c(merge) } ∪ { μ1 | no compatible μ2 with c(merge) }`. When
`c` reads only variables Ω2 binds in every solution, `c(merge(μ1, μ2)) =
c(μ2)`, so both sets equal those of `LeftJoin(Ω1, Filter(c, Ω2))`. The
first precondition is exactly where this fails: a `c` reading a left-only
variable, or a variable the body binds *optionally* (unbound in some μ2 —
then `c(merge)` may read μ1's binding), is not sinkable.

The second precondition is where the variable test is vacuous. `RAND() <
0.5` has no variables and passes the first test; moved from the joined
pairs to the body's rows it is drawn once per μ2 instead of once per
(μ1, μ2) pair, and pairs that shared a draw no longer do — SPARQL promises
a fresh value per invocation. The renderer would refuse `RAND()` later,
but a renderer refusal is not a proof that the logical rewrite was
equivalent; the rule's own contract is. A `c` with an `Opaque` subplan is
refused by the same predicate.

**What it closes.** 28d "an `OPTIONAL` whose condition is lifted into the
join — served as a fetch": the condition was given up to the residual
because the renderer had no `ON` to put it in. Inside a derived table it is
a `WHERE`, and the ledger claims it honestly. What stays declined is the
case with a real correctness argument against it.

### Op 3 — restriction at a boundary, then pushed down *(two rules; the second is a family)*

Revision 1 had one rule: "the other side of the join binds `?v` as an
identity of `class`, so fold the body's matches on `?v` into a scan of
`class`". The review's counter-example:

```sparql
SELECT ?s WHERE {
  ?s a :C .
  { SELECT ?s WHERE { ?s :p ?x } ORDER BY ?s LIMIT 1 }
}
```

with `:a a :Other; :p 1` and `:b a :C; :p 2`, `:a` sorting first: the
sub-select picks `:a`, the join finds no `:C` partner, the answer is
empty. Narrow the inner match to a `:C` scan and the sub-select picks
`:b`, and the rewritten query answers `:b`. Every precondition of the
revision-1 rule held. The semi-join argument licenses restricting the
**completed** relation — the rows that reach the join — and says nothing
about restricting a scan under a `Slice`. That is two transformations,
and they get two rules.

#### Op 3a — `RestrictScopeAtBoundary` *(new rule; a filter at the root)*

**Match.** An `Exporting` scope *S* that is a side of a `Join`, or the
**right** side of a `LeftJoin`, with `?v ∈ on`; `?v ∈ guaranteed(S.root)`
and `?v ∈ guaranteed(other side)`; `term_of(other side, ?v) =
Identity(class)`; and *S* does not already carry a restriction of `?v` to
`class` at its root.

**Edit.** `S.root := Filter(S.root, ?v ∈ Identity(class))`, a filter kind
that renders as `<col> IN (SELECT asset360_uri FROM golden_records WHERE
asset_type = 'class')` — or, when 3b carries it to a scan, as the scan's
own `asset_type` condition — and raises a new obligation, `o_boundary(S,
?v, class)`, in *S*. The obligation is the provenance the review asked for
in place of `narrowed_by_interface: true`: whoever discharges it names the
rule and the boundary that justified it.

**Equivalence.** A semi-join reduction on the completed relation: for a
join on `?v`, rows of *S* whose `?v` no row of the other side carries
contribute nothing to `Join`, and nothing to `LeftJoin` when *S* is the
**right** side — a left row with no partner is kept unchanged either way,
so restricting the *right* relation cannot delete or alter a left row,
while restricting the *left* relation would delete preserved rows. Every
`?v` the other side carries is an IRI of `class` (`term_of`), bound in
every row (`guaranteed`); records of one URI in two classes do not exist
(the identifier column is the table's key). So the filter removes only
rows that joined nothing. This is `ValuesNarrowTheJoinedScan`'s argument
with a scan in place of the `VALUES` block, and it is made at the root of
*S*, where the relation is complete.

**What it declines.** The left side of a `LeftJoin`; `Minus` and
`AntiJoin` (a `MINUS` keeps rows with *no* partner, so shrinking the right
side widens the answer); a `Union` arm as a side (the arm is not the
relation the join sees); a `?v` either side binds optionally.

#### Op 3b — `PushFilterThroughOperator` *(one rule per operator; the family exists)*

The boundary filter now sits at the root. Getting it to a scan, where it is
an indexed `asset_type = …` and where `FoldMatchesIntoScan` can fold the
matches on `?v` into a `Scan` of `class`, is a walk **down**, one operator
at a time, and each step has its own commutation rule. Most of these exist
already as `PushComparisonFilter`'s placement logic; they are listed so
the ones that *stop* are visible:

| operator between the filter and the scan | may the filter pass below it? |
|---|---|
| `Filter`, `Bind` (not binding `?v`), `Sort`, `Distinct`, `Reduced` | yes: a row test commutes with another row test, with a binding it does not read, with an order, and with dedup |
| `Project`, `SubSelect` exporting `?v` | yes, to the input |
| `Join` | yes, to **each** side that has `?v ∈ guaranteed(side)`; a side where `?v` is not guaranteed keeps rows whose `?v` is unbound, which the filter must not test |
| `LeftJoin` | to the **left** side only, and only if `?v ∈ guaranteed(left)`; never to the right (that deletes bindings, not rows) |
| `Union` | to both arms, each on its own guarantee |
| `Minus`, `AntiJoin` | to the left side only |
| `Group` **with `?v` among its keys** | yes: rows are partitioned by `?v`, so a test on `?v` removes whole groups and changes no surviving group's aggregate |
| `Group` **without `?v` as a key** (a keyless aggregate, or `?v` only inside a measure) | **stop.** The aggregate's value depends on the rows removed |
| `Slice` | **stop.** Which rows the offset/limit keep depends on the rows removed — the counter-example above |
| `Path`, `Service`, `Opaque` | stop |

The walk records each step as the rule that took it, and the obligation
`o_boundary` travels with the filter. Where the walk stops, the filter
stays: still correct (the boundary proof holds at any point above the
stop, because every step preserved the multiset), just not folded into a
scan. In the counter-example the filter stops above `Slice`, renders as
`… ORDER BY ?s LIMIT 1` inside the derived table with a `WHERE s IN
(class)` **outside** the slice — i.e. as an outer query over the sliced
one — and the answer is empty, as written.

**Would a bad application be caught?** A filter below a `Slice` or a
keyless `Group` with an `o_boundary` obligation fails a new invariant,
**a boundary restriction is above every stop**, which walks from each
`o_boundary` discharge up to its scope root and requires no `Slice` and no
keyless `Group` on the way. The obligation is what makes the check local:
without it, the invariant could not tell a user-written filter (which may
sit anywhere the user put it) from a derived one.

**Why the body scans the class again rather than reading the outer row**
is unchanged from revision 1: inside the derived table, `?a`'s record is a
fresh `Scan` joined back to the outer `t0` on identity, a self-join
Postgres answers by index; a `LATERAL` rendering reading `t0` directly is
an equivalent statement and a renderer choice (open question 3).

### Op 4 — `PushBarrier` *(new rule; the projection rule, at a scope root)*

**Match.** An `Exporting` `SubSelect` that is `[E]`, whose scope root is
itself (no modifier above it inside the scope — `PushProjection`/`PushGrouping`
inside the scope have already absorbed any), whose `input` is `[S]`, whose
`correlated_inputs` is empty, and every export's producer resolves through
the body's `Visible` to a renderable column (`key_is_readable`, the
grouping rule's own test, which `PushProjection::column_is_readable`
already reuses). A `Testing` scope never matches: it is not a relation
the outside joins.

**Edit.** Flip the `SubSelect` to `[S]`. The lowering emits `Op::Relation`.

**Equivalence.** The body's rows *are* its solutions — that is what "every
node below is `Sql`" plus `answers_alone`'s emit condition means, and it is
the same admission 28d's path B applies to a whole query, now applied to a
scope. A `SubSelect` over solutions is a column selection, and a derived
table over a statement's rows is the same column selection.

**Inherited refusals, unchanged.** `IdentityIsNotATriple` (a body resting on
an identifier-slot restriction answers `0`/empty on the engine; the barrier
must not invent an answer the outer query would then join on);
`COUNT(DISTINCT *)`, `GROUP_CONCAT`, `SAMPLE`, `AVG`/`SUM` over a float, a
`xsd:double` term — every one is `PushGrouping`'s decline, and a body it
declined has an `[E]` group, so the barrier does not match. **No new
refusal vocabulary**, because a barrier is admitted on the same terms as a
query.

**Would a bad application be caught?** A barrier flipped over an `[E]` node
fails `frontier_is_a_cut`; one exporting a variable no body column binds
fails the lowering's `Unrenderable`; one whose body fans out above a
collapse fails `fanout_restored` — the invariant already walks
`collapses_rows` between a scan and its unnest, and a `SubSelect` is not a
collapse (its input's rows pass through unchanged), so the check is
unchanged.

### Op 5 — `PushJoinOnIdentity` *(widening of `PushReferenceJoin` / `PushLeftJoin`)*

**Match.** An `[E]` `Join` or `LeftJoin` whose sides are both `[S]` and
whose `on` is **either empty or one variable `?v`**:

* `on = []`: no further condition. A natural join on no shared variable
  is every pair — SPARQL's `Join` over disjoint domains — and it lowers as
  `CROSS JOIN` (a `LeftJoin` with `on = []` and no condition is the same
  pairing, `LEFT JOIN … ON true`, and keeps the left row when the right is
  empty). This is the scalar-beside-every-row shape; revision 1 promised
  it in the table and required one key in the match.
* `on = [?v]`: **both** sides have `term_of(side, ?v) = Identity(class)`
  for one `class` — a scan's or a relation column's — **and** `?v ∈
  guaranteed(side)` on both. Two checks against two derived properties;
  an identity column that passed through a `LeftJoin` right side has the
  first and not the second, and declines.

A join on two or more variables is a later widening (a conjunction of
per-variable keys, each with this proof) and not this rule.

**Edit.** Flip to `[S]`; record `JoinKey::Identity { left, right }` on the
node the way `ReferenceEdge` is recorded today. For a `LeftJoin`, the
`condition` must be `None` (op 2 sank it, or it is unsinkable and the rule
declines, as `PushLeftJoin` does).

**Equivalence.** SPARQL joins on **term equality** of the shared variable.
Two identities of one class are two IRIs; the identity column holds the IRI
text; text equality of two IRIs is term equality. This is why the rule is
*identity only* and not "any two columns that bind the same variable": a
value join (`?a :name ?n . ?b :hasName ?n`) compares stored text where
SPARQL compares terms, and `"1"` and `"1.0"` are equal numbers and different
terms — the fidelity argument 28d makes for constants, made for join keys.
Widening to typed value columns is a later op with its own precondition
(`constant_is_the_columns_term`'s sibling for two columns); not this one.

**Guaranteed, again.** A SPARQL join is on *compatible* solutions, and
an unbound `?v` is compatible with everything: a group whose key is unbound
(a key read from an `OPTIONAL`) joins every outer row. SQL's `NULL = x` is
never true. So the rule requires `?v ∈ guaranteed` on both sides, and
declines otherwise — the missing-value bucket stays an engine shape when
its key is a join key. `guaranteed` for a `Group` key is *derived from the
input's guarantee* (the transfer table above), which is what makes this
decline reachable: a definition that called every group key bound would
have let the unbound-key group through.

**`LeftJoin` semantics.** `t0 LEFT JOIN q ON q.a = t0.asset360_uri`: the
left row survives, the relation's columns are `NULL` where no `q` row
matched, and the serialiser already omits a `NULL` binding. One row per
`(t0, q)` pair — the body's multiplicity is the derived table's row count,
which is the SPARQL multiset. A grouped body has one row per key, so a
grouped sub-select never multiplies the outer row: that is the "one count
beside the row" #466 asks for, obtained from the semantics rather than
assumed.

**Would a bad application be caught?** A new invariant, the sibling of
`reference_joins_agree`: **join keys agree** — a `JoinKey::Identity`'s two
columns are each the identity of a scan or a relation column of kind
`Identity`, on the side the key says, of one class, **each in
`guaranteed` of its side**, and `on` is that variable alone; a
`JoinKey::Cross` has `on = []`.

### The widening op 5 needs for #464: a reference held by an element

`foreign_key_on` finds a reference on a `Scan` slot with `path.len() == 1`,
required, single-valued. For `?cs :belongsToTrack ?track` the holder is an
**unnested element**: `ScanSlot { path: [hasCoveredSection], multivalued }`
plus `Unnest` binding `?cs`, and `belongsToTrack` is a slot *of the
element*. The widening: a `ReferenceEdge.holder` may be `Element { star,
unnest_var }` with `slot` read relative to it, and the renderer's `ON` reads
`e.value->>'belongsToTrack'` where it reads `t1.object_data->>'slot'` today.
Precondition: the unnest is below the join (so the element is a row) and
the element class (`class_at_path`, the same lookup `FoldNestedMatchIntoPath`
does) declares the slot as a single-valued reference. Equivalence: an
unnested element is a row with columns, and its reference slot is a column
of that row — nothing about the join changes.

**This is the first place the fetch renderer and the statement renderer
would say different things for the same edge.** The fetch's path edge
(`right_path`, `jsonb_path_query … [*]`) is an *any-element* containment —
correct for a fetch that over-fetches — and would be **wrong** for a
statement that binds `?cs`: it joins a track referenced by *any* covered
section to *every* unnested element. The plan already carries the
distinction (`SlotReading::{AnyElement, BoundElement}`), and the lowering
must state it on the edge, so the renderer cannot pick the fetch's
spelling. That is the correction the shape needs, and it is a fact on the
node rather than a renderer habit — the same move 28d made for
`optional_side`.

## Worked case: #466, plan by plan

Naive (abbreviated; `[E]` everywhere):

```
n0  match   ?a a :CivilEngineeringAsset                 [E]
n1  match   ?a :hasCivilEngineeringAssetType <Tunnel>   [E]
n2  match   ?a :identification ?name                    [E]
n3  join    n0 n1 n2                                    [E]
n4  match   ?a a :CivilEngineeringAsset                 [E]
n5  match   ?a :hasCoveredSection ?cs                   [E]
n6  join    n4, n5  on ?a                               [E]
n7  group   keys=[?a] count(?cs) as ?agg0               [E]  o_group o_aggr
n8  bind    ?trackQty := ?agg0                          [E]
n9  subselect [?a, ?trackQty]                           [E]   ← scope S1 root
n10 join    n3, n9  on ?a                               [E]
n11 project ?name ?trackQty                             [E]
```

After the existing rules — the fold now counting types per scope, so each
`?a` gets its scan; the constant-object filter; and `PushGrouping` anchored
at the scope root, which is the only change it needs:

```
n0  scan    :CEA as ?a, requires [identification]        [S]
n1  filter  ?a.hasCivilEngineeringAssetType = <Tunnel>   [S]
n2  scan    :CEA as ?a, requires [hasCoveredSection]     [S]   (scope S1)
n3  unnest  ?a.hasCoveredSection as ?cs                  [S]   (scope S1)
n4  group   keys=[?a] count(?cs) as ?trackQty            [S]   (scope S1)
n5  subselect [?a, ?trackQty]                            [E]   ← op 4 fires
n6  join    n1, n5  on ?a                                [E]   ← op 5 fires
n7  project ?name ?trackQty                              [E]   ← PushProjection
```

Op 4: `n4` is `[S]`, `?a` resolves to `Identity(CEA)` inside S1 (`term_of`)
and is guaranteed there (a scan identity, through a keyed `Group` whose
key it is), `?trackQty` to a `COUNT` measure, guaranteed → `n5 [S]`.
Op 5: `n1`'s `term_of(?a) = Identity(CEA)`, guaranteed; `n5`'s column says
the same; one class → `n6 [S]`, key recorded. (Op 3a does not fire: S1
already scans `?a` as CEA, so a restriction to CEA at its root adds
nothing — the "already carries" clause.) `PushProjection`: no `Group` **in the outer
scope** (the one in S1 is behind a pushed barrier), body `[S]`, both
projected variables readable (`?name` a column of `n0`, `?trackQty` a
relation column) → `n7 [S]`. Every node `[S]`, one island per scope,
`answers_alone` holds — the statement is the answer:

```sql
SELECT t0.object_data->>'identification' AS name, q0.track_qty
FROM golden_records t0
JOIN (
  SELECT t1.asset360_uri AS a, count(*) AS track_qty
  FROM golden_records t1
  CROSS JOIN LATERAL (SELECT DISTINCT e.value FROM jsonb_array_elements(t1.object_data->'hasCoveredSection') e) e
  WHERE t1.asset_type = 'CivilEngineeringAsset'
  GROUP BY t1.asset360_uri
) q0 ON q0.a = t0.asset360_uri
WHERE t0.asset_type = 'CivilEngineeringAsset'
  AND t0.object_data->>'hasCivilEngineeringAssetType' = '…Tunnel'
  AND t0.object_data ? 'identification'
```

(The inner statement is what `PushGrouping` + the renderer already emit for
the flat `GROUP BY ?a` form; `count(*)` over the fanned-out element is
`COUNT(?cs)` by the existing unbound rule, since a required element is
never `NULL`. The `SELECT DISTINCT e.value` is the existing term-set
dedup.)

**Equivalence, the cases that matter:**

* **A tunnel with no covered section.** The inner `?a :hasCoveredSection ?cs`
  is mandatory, so the group has no row for it, and SPARQL's `Join` drops
  the tunnel: the query as written answers **no row**, not `0`. The inner
  join to `q0` does the same. Wanting `0` is a different query —
  `OPTIONAL { { SELECT … } }` — and the same ops give it: op 5 on the
  `LeftJoin`, `?trackQty` unbound for that tunnel. `BIND(COALESCE(?trackQty,
  0))` on top is a `Bind` the engine keeps — a fetch, not the statement —
  unless `COALESCE` becomes an `Expr::to_sql` case, which is orthogonal.
* **A duplicate entry in the array.** RDF holds one triple per distinct
  value; the existing `SELECT DISTINCT e.value` dedups before the count on
  both routes. Unchanged.
* **JSON `null` / absent key.** A required read excludes both; the element
  unnest of a missing array yields no rows. Unchanged.
* **Multi-valued key.** `GROUP BY ?a` on an identity: one row per record.
  `GROUP BY ?cs` (an element) would be a `BoundElement` key, which
  `key_is_readable` already accepts; the join above would then need `?cs`
  on both sides as an *identity*, which it is not (it is a structure) — op 5
  declines, the barrier stays, the plan falls back. Correct and slow, as
  today.
* **Two records of the same URI.** Not possible: `asset360_uri` is the
  table's key within a table target. The overlay (edit-session) table is a
  merged view with the same key.

## Worked case: #464, plan by plan

Naive (the block, abbreviated):

```
n0  scan    :CEA as ?a, requires [identification]; belongsToSubZone = <Z>   [S]  (after fold)
n1  match   ?a :hasCoveredSection ?cs                     [E]
n2  match   ?cs :isReference true                         [E]
n3  match   ?cs :belongsToTrack ?track                    [E]
n4  match   ?cs :hasEntryPointM ?kpFromMeter              [E]
n5  match   ?track a :Track                               [E]
n6  match   ?track irsm:name ?trackName                   [E]
n7  match   ?track irsm:longname ?trackCode               [E]
n8  join    n1..n4, n5..n7                                [E]
n9  match   ?track :hasTrackType ?trackDiscr              [E]
n10 leftjoin n8, n9                                       [E]
n11 leftjoin n0, n10                                      [E]
n12 project *                                             [E]
```

Today: `AbsorbOptionalRead` declines `n11` (not one match); `DeliverOptionalRead`
hands `hasCoveredSection` to the engine as a delivered column;
`AbsorbOptionalReference` finds no depth-one reference; the block is
`[E]`, the fetch is `n0` left-joined to `Track` through the path edge, and
the engine does the rest.

With the vocabulary:

1. **Op 1** wraps `n10` in `subselect [?a ?cs ?track ?kpFromMeter
   ?trackName ?trackCode ?trackDiscr]` — scope S1, exports everything the
   body binds, **`?a` included**: the body reads `?a`, exports it, and the
   `LeftJoin` joins on it. (Revision 1 left `?a` out of the list and joined
   on it two steps later; `Scope::of` derives the list, which is the fix.)
   `guaranteed(S1.root)` is everything but `?trackDiscr`, which is behind
   the inner `LeftJoin`.
2. **Op 3a** places `?a ∈ Identity(CEA)` at S1's root: the outer side of
   `n11` has `term_of(?a) = Identity(CEA)` and `?a` guaranteed, S1 is the
   right side of a `LeftJoin`, `?a` is guaranteed in S1. **Op 3b** walks it
   down through `n8`'s `Join` (to the side that guarantees `?a`) to the
   matches on `?a`, meeting no `Slice` and no `Group`; `FoldMatchesIntoScan`
   then folds `n1`–`n4` into `scan :CEA as ?a` discharging `o_boundary`,
   with `unnest hasCoveredSection as ?cs`, `?cs`'s three reads as element
   paths (`hasCoveredSection[each].isReference` etc.). `FoldMatchesIntoScan`
   folds `n5`–`n7` into `scan :Track as ?track`. `ConstantObjectBecomesFilter`
   makes `isReference = true` a `BoundElement` filter above the unnest.
3. **The element-held edge** lets `PushReferenceJoin` push `n8`'s join on
   `?track` between the element and the Track scan.
4. `AbsorbOptionalRead` absorbs `n9`/`n10` — one match, one single-valued
   slot of `?track`, the existing rule, now inside S1.
5. **Op 4** flips the barrier: every node in S1 is `[S]`, no correlated
   inputs, every export a column (`?cs` — see open question 5; `?a` and
   `?track` identities; the rest slots; `?trackDiscr` a nullable slot,
   not guaranteed, which the column records).
6. **Op 5** pushes `n11` as a `LeftJoin` on `?a`: `Identity(CEA)` on both
   sides, `?a ∈ guaranteed` on both — on the *outer* side because it is
   `n0`'s scan identity on the preserved side, on S1 because the derived
   property says so at S1's root; "an identity is always bound", which
   revision 1 wrote here, is not a rule — `condition: None`.
7. `PushProjection` takes `SELECT *` over an all-`[S]` outer scope.

```sql
SELECT t0.object_data->>'identification' AS name, q0.kp_from_meter, q0.track, q0.track_name, q0.track_code, q0.track_discr, …
FROM golden_records t0
LEFT JOIN (
  SELECT t1.asset360_uri AS a,
         e.value->>'hasEntryPointM' AS kp_from_meter,
         t2.asset360_uri AS track, t2.object_data->>'name' AS track_name,
         t2.object_data->>'longname' AS track_code,
         t2.object_data->>'hasTrackType' AS track_discr
  FROM golden_records t1
  CROSS JOIN LATERAL (SELECT DISTINCT e.value FROM jsonb_array_elements(t1.object_data->'hasCoveredSection') e) e
  JOIN golden_records t2 ON t2.asset360_uri = e.value->>'belongsToTrack' AND t2.asset_type = 'Track'
  WHERE t1.asset_type = 'CivilEngineeringAsset'
    AND e.value->>'isReference' = 'true'
    AND e.value ? 'hasEntryPointM' AND t2.object_data ? 'name' AND t2.object_data ? 'longname'
) q0 ON q0.a = t0.asset360_uri
WHERE t0.asset_type = 'CivilEngineeringAsset' AND t0.object_data->>'belongsToSubZone' = '<Z>' …
```

**Equivalence, the cases that matter:**

* **"Together or not at all."** A covered section with `belongsToTrack` but
  no `hasEntryPointM` is not a solution of the body — the `WHERE` inside the
  derived table drops it — so the outer row gets `NULL` in every `q0`
  column, i.e. every optional variable unbound. Three nullable columns on
  `t0` could never say this; a derived-table row cannot say anything else.
  This is the two-read problem *dissolving* rather than being solved by a
  rule.
* **The asset with no reference section.** Kept, everything unbound: the
  left join. The asset with three reference sections: three rows, as SPARQL
  answers. `fanout_restored` walks the unnest inside S1 and finds no
  collapse above it in S1 — the `LeftJoin` outside is not a collapse.
* **The nested `OPTIONAL { ?track :hasTrackType ?d }`.** Absorbed as a
  nullable column of `t2` *inside* the body: `?d` may be unbound while the
  rest is bound, which is what the nested `OPTIONAL` means, and it is the
  existing rule doing the existing thing one scope down.
* **The 18-vs-17 trap** (28d, "a condition on the optional side sat in the
  `WHERE`"). `isReference = true` is in the *derived table's* `WHERE`, which
  is the body's own row test, and the outer `LEFT JOIN … ON` carries only
  the key. What keeps it there is not closure — `?cs` is exported, so an
  outer reference to it resolves — but *obligations stay in their scope*:
  the obligation for `?cs :isReference true` was raised in S1, and the
  only nodes that may discharge it are in S1 or on the `LeftJoin` that
  combines S1 with the outside. A rule that moved it to the outer `WHERE`
  fails that invariant at the rule.
* **A `FILTER` outside the `OPTIONAL` on a variable it binds** (`FILTER(?kpFromMeter
  > 100)` after the block). Sits above `n11` in the outer scope, reads a
  relation column; `PushComparisonFilter` renders it as an outer `WHERE`
  where `NULL` fails — which is SPARQL's "unbound → error → false". The
  existing `solution_conditions` placement, on a relation column. `!BOUND`
  stays refused as today.

## The same vocabulary, on shapes neither issue wrote

This is the check against "code for one literal shape". Each row is a
query the ops serve with **no addition**, and each is a test in the
proposed suite.

| shape | ops that fire | statement |
|---|---|---|
| `{ SELECT ?s WHERE { ?s a :Signal } ORDER BY ?s LIMIT 3 } ?s :hasName ?n` — top-N, then read | `PushProjection` in the scope, 4, 5 | `JOIN (SELECT … ORDER BY … LIMIT 3) q ON q.s = t0.asset360_uri` |
| `OPTIONAL { { SELECT ?a (COUNT(?cs) AS ?n) … GROUP BY ?a } }` — #466 with zeros | `PushGrouping` in the scope, 4, 5 (left) | `LEFT JOIN (grouped) q`, `?n` unbound for a tunnel with none |
| `OPTIONAL { ?s :ref ?t . ?t a T ; :x ?p ; :y ?q }` — 28d's declined two-read over a reference | 1, 3, `PushReferenceJoin`, 4, 5 | `LEFT JOIN (SELECT … FROM t1 JOIN t2 …) q` |
| `OPTIONAL { ?s :ref ?t . ?t a T . FILTER(?t.x > 5) }` — the lifted condition | 1, 2, then as above | condition in the derived table's `WHERE` |
| `{ SELECT (COUNT(*) AS ?total) WHERE { ?s a :Signal } } ?s a :Signal ; :hasName ?n` — a scalar beside every row | `PushGrouping` in the scope (no keys), 4; the outer join has `on = []` | `CROSS JOIN (SELECT count(*) …) q` — op 5 with an empty key is a cross join, which is what a natural join on no variables is; worth its own row in the tests |
| `{ SELECT ?s (COUNT(?x) AS ?nx) WHERE { ?s :x ?x } GROUP BY ?s } { SELECT ?s (COUNT(?y) AS ?ny) WHERE { ?s :y ?y } GROUP BY ?s } ?s a :Signal` — two grouped sub-selects joined on one identity | `PushGrouping` in each scope, 4 twice, 5 twice | two derived tables joined on `s` |
| `{ SELECT ?s ?nx ?ny WHERE { { SELECT ?s (COUNT(?x) AS ?nx) … GROUP BY ?s } { SELECT ?s (COUNT(?y) AS ?ny) … GROUP BY ?s } } } ?s :hasName ?n` — the review's case: the two joined **inside** a third sub-select | the row above inside S0, then 4 and 5 for S0 — the derived properties are recursive, so the outer barrier sees `?s` as `Identity`, guaranteed, and `?nx`/`?ny` as measures, without a descendant search | a derived table whose body joins two derived tables |
| `?s a :Signal . { SELECT ?s WHERE { ?s :p ?x } ORDER BY ?s LIMIT 1 }` — the review's op-3 counter-example | 3a at the root, 3b **stops at `Slice`**, 4, 5 | `JOIN (SELECT * FROM (SELECT … ORDER BY … LIMIT 1) i WHERE i.s IN (Signal)) q` — empty when the first `?s` is not a Signal, as written |
| `?s a :Signal . FILTER NOT EXISTS { ?s :ref ?t . ?t a T ; :x ?p ; :y ?q }` | a `Testing` scope: op 1 does not apply (it is not a `LeftJoin`), op 4 does not match (`correlated_inputs` non-empty, exports nothing) — the correlated `NOT EXISTS (SELECT …)` is its own op | a widening of `PushNotExists` from "exactly one scan" to "a lowered testing scope", with its own proof, left as a follow-up row |

What does **not** appear: a rule that mentions `hasCoveredSection`, a count,
an array, a tunnel, or "sub-select joined to the driving variable".

## What stays refused, and says so

Each is a precondition of one op, and the plan printout names the node.

* **A join on a value column** (`?a :code ?c . { SELECT ?c … }`): op 5,
  identity only. Reason: term fidelity of two stored texts.
* **A join key the body binds optionally** (`{ SELECT ?k … WHERE { OPTIONAL
  { ?s :key ?k } } GROUP BY ?k }`): op 5, `guaranteed`. Reason: an
  unbound key joins everything in SPARQL and nothing in SQL.
* **A lifted condition reading an outer variable** (`OPTIONAL { ?t :len ?l
  FILTER(?l > ?outer) }`): op 2 declines, `PushLeftJoin`/op 5 refuse a
  `LeftJoin` with a condition. As today.
* **A lifted condition with an effect** (`OPTIONAL { ?t :len ?l
  FILTER(RAND() < 0.5) }`): op 2 declines on `effects`, before any
  renderer sees it.
* **A boundary restriction under a `Slice` or a keyless `Group`**: op 3b
  stops; the restriction renders above the stop, and the scan below is
  not narrowed. Correct, and the printout shows where it stopped.
* **A `NOT EXISTS` / `MINUS` body**: a `Testing` scope; op 4 never
  matches it. As today.
* **A body with an engine node** (a `REGEX` filter inside the sub-select,
  a property path, `GROUP_CONCAT`, `COUNT(DISTINCT *)`, a float `SUM`):
  op 4 does not match; the barrier stays `[E]`, the outer scan is a fetch,
  the engine answers — and for an aggregate the 422 gate stays exactly
  where it is. The message improves for free: the printout shows *which*
  node inside the scope stayed `[E]`.
* **A scope on the right of `MINUS`, or a `UNION` arm**: op 3a declines
  (semi-join reduction is unsound there); a `Union` arm may still be
  pushed by op 4 (a barrier is a barrier), a `Minus` right side is
  `Testing` and is not. Unchanged in effect.
* **A sub-select whose body restricts an identifier slot** (`?s :id "X"`):
  `IdentityIsNotATriple`, inherited.
* **`!BOUND(?relationColumn)`** above a pushed left join: refused as today.

## Tests that would prove equivalence

Two oracles, because one is not enough. The engine leg
(`tests/support.py::force_engine_leg`) re-runs the original query over the
materialised fetch — the oracle every path-B capability carries, and it
stays. But both routes share the fetch, and a fetch narrowed by a wrong
provenance (the hidden-`?s` case above) makes both routes agree on a wrong
answer. So the second oracle is **the original query over the complete
fixture, in memory**: the same small seed loaded into an oxigraph store
with no fetch in front of it. Each case asserts its admission
(`used_alone`) so a comparison cannot become one route against itself,
and compares against the second oracle as well.

How answers are compared: as **bags of solution mappings** over RDF terms
— unbound is a value, multiplicity counts, `"1"` and `"1.0"` differ —
and order is compared only where the query defines one (`ORDER BY` with
a total key). "Row for row under a total order" in revision 1 was the
test framework's convenience, not the semantics.

1. **Named cases**, one per equivalence argument above: the tunnel with no
   section (row absent / unbound under `OPTIONAL`); the section missing one
   of three slots (all unbound together); three reference sections (three
   rows); the duplicate array entry; the nested `OPTIONAL` bound alone; the
   outer `FILTER` on a relation column; the lifted condition sunk; the
   scalar sub-select cross join; the top-N-then-read.
2. **Property-style**, in the spirit of `the_rule_order_does_not_decide_the_fixpoint`:
   generate bodies from a small grammar — a driving class; 0–2 reads of it;
   an optional or mandatory array hop with 1–3 element reads; an optional
   or mandatory reference hop with 0–2 reads and an optional nested read;
   a `GROUP BY` on the identity **or on an optionally-bound variable, or
   no key**, with `COUNT`/`MIN`/`MAX` or none; an optional `ORDER BY` +
   `LIMIT`/`OFFSET` on the body; a body variable that **shadows** an
   outer name without exporting it; **one or two** shared variables with
   the outside; **nesting** one generated body inside another, one level;
   placed as a mandatory sub-select, an `OPTIONAL` body, or `OPTIONAL { {
   SELECT … } }`. The seed has an empty class, a record with a duplicate
   array entry, and two records identical in every read slot (duplicate
   mappings). For each generated query assert (a) `refine` reaches a
   fixpoint with every invariant holding, (b) the plan lowers or names the
   node that stopped it, (c) where it lowers, the statement's answer equals
   both oracles as a bag, and **(d) each logical rewrite, taken alone, is
   answer-preserving**: run the query with the rule set truncated after
   each rule in turn, and with three legal schedules (the list, the list
   reversed, one random permutation under a fixed seed), and compare every
   plan that lowers against the oracles. A rewrite that is only right in
   combination with a later one is a rewrite whose precondition lies. The
   grammar is small enough to enumerate exhaustively rather than sample.
3. **Invariants driven by a bad rule**, as 28d does for the frontier: a
   barrier that exports a variable its body does not bind; a join key on a
   slot column; an identity join with the key recorded on the wrong side;
   an identity join on a key `term_of` accepts and `guaranteed` does not;
   a lifted condition sunk although it reads an outer variable, and one
   sunk although it calls `RAND()`; an optional-side filter reparented to
   the outer scope (*obligations stay in their scope*); a boundary
   restriction pushed below a `Slice`. Each must fail at the rule, not in
   a result.
6. **The fallback with a hidden variable**: the review's COUNT case, run
   with the SQL lowering *deliberately refused* (`tests/support.py`'s
   sibling of `force_engine_leg` that fails `lower_refined` by name) so
   the plan is refined, the fetch reconstructed by
   `keep_what_the_rules_proved`, and the engine answers over it. Both
   `:C` records must come back with `?n = 1`. Then the same with the
   inner and outer variable spelled the *same* and the inner one
   exported — the case where the narrowing *is* valid, through the
   boundary proof — asserting the fetch was narrowed.
7. **The review's op-3 counter-example**, both routes and the in-memory
   oracle: empty answer, and the SQL text shows the restriction outside
   the `LIMIT`.
4. **Both routes agree on the shapes that had a cheaper statement**: every
   `OPTIONAL` the absorb rules already serve keeps its plan (the frozen
   `planner_baseline.json` — 114 shapes — is the reviewable diff, and the
   expected diff is *zero* route changes plus the new capabilities named in
   `PATH_B_CAPABILITIES`) **and the same SQL text**, now reached through
   a barrier. Schedule independence is test 2(d), not a single reversal.
5. **The fetch-versus-statement edge**: an unnested element reference
   rendered `BoundElement` in the statement and `AnyElement` in the fetch,
   asserted by reading the SQL, since the two spellings answer differently
   and only one is the statement's.

Where they live, in today's files: an `assert_routes_agree(…, selects=True)`
method per named case in `sparql/tests/test_pushdown_oracle.py`; the shapes
in `sparql/tests/planner_inventory.py`, with `planner_baseline.json`'s
`"negative: sub-select"` entry moving out of the negative corpus and its
`alone` flipping to `true` as a reviewed diff; the SQL text assertions in
`test_aggregate_sql.py` / `test_ops_render.py`; the property grammar and
the bad-rule cases in `sparql_rules.rs` beside
`the_rule_order_does_not_decide_the_fixpoint`.

## What it costs

**Runtime.** #466: the grouped derived table is the flat form's statement
(measured 1.1 s for 130 tunnels on review-591's seed; it scans every
CivilEngineeringAsset's array once) plus an index join on the outer rows —
so about the flat form, in one call instead of two. Postgres pushes an
equality on a grouping key into the derived table when it is a constant,
not when it is a join, so the group is over the class; a `LATERAL` rendering
(open question 3) would bound it to the outer rows and is the win to
measure. #464: the statement is the fetch's own join plus the lateral
unnest, on the order of the 0.4 s the fetch already takes, against 17 s —
and the block pages by the statement since #457 (pepibru GitLab, paging by the statement). Both numbers are
predictions to be measured on the review app, not promises.

**Code.** One computed `Scope` with `Scope::of`; the five derived
properties as functions over the plan (`outputs`, `guaranteed`, `term_of`
absorbing today's `Visible::identity_of`/`slot_of`, `correlated_inputs`,
and `effects` which exists); `Visible` reading barrier columns;
`applies_to_every_answer` scope-local with a keyless-`Group` arm;
`keep_what_the_rules_proved` keyed by producer; two one-line anchor
changes in the two tail walks; new rules ops 1, 2, 3a, 4; op 3b as a
per-operator table that mostly restates `PushComparisonFilter`'s
placement; the three absorb rules moved from `tier_one_rules` to
`lower_refined`'s choice of physical form; one widened rule (op 5, and the
element-held edge in `foreign_key_on`); `Op::Relation`, `JoinKey`
(`Identity`, `Cross`), `PLAN_CONTRACT` 5; four invariants (closure on
producers, obligations stay in their scope, join keys agree, boundary
restriction above every stop); the recursive FROM item in `sql_builder.py`
and its `_OpRelation` in `plan_ops.py`; the in-memory oracle in
`tests/support.py`. One asset360-rust release, one pin bump. More than
revision 1 said — the derived properties and the fallback provenance are
the first MR's cost and were missing — and about twice #463 (pepibru
GitLab, N and nested reference `OPTIONAL`s) for the first MR, then #463
each for the other two.

**Risk.** The renderer recursion is the part with no precedent in this
codebase, and it is where a parameter-order or alias-collision bug would
live; the property suite's row-for-row comparison is what would find it.
The second risk is the one the human named — an op that is really a shape
in disguise — and the "same vocabulary" table above is the standing test
for it: a new row that needs a new op is a reason to reopen this document.
The third, which the review surfaced, is a rule whose precondition is
right at the boundary and wrong below it: the guard is that every property
a rule reads is *derived at the node it reads it from*, and test 2(d).

## Alternatives rejected

* **A correlated scalar subquery** (`(SELECT count(*) FROM
  jsonb_array_elements(t0.object_data->'hasCoveredSection'))` as a computed
  column — #466's option 3). Cheap, and wrong twice: it answers `0` where
  the query as written answers no row, and it is a rule that recognises one
  spelling (count of one own array of the driving variable). It is the
  literal-shape op this document exists to not write.
* **Slot-wise absorb with `CASE`-wrapped columns** for the two-read
  `OPTIONAL` (bind each column only when every required sibling is
  non-null). Correct for two reads of one star; does not compose with a
  reference hop or an unnest inside the body, so #464 would need a third
  mechanism. The barrier subsumes it.
* **A residual evaluator** (28d, decided against). A derived table is not a
  partial collapse: the body collapses wholly *inside* the relation, and
  the outside sees rows. "Collapse fully, or fall back" holds per scope.
* **`LATERAL` as the primary rendering.** Equivalent SQL, better for a
  narrowed outer scan, but it makes the derived table read the outer alias
  and so needs the renderer to know which scope a relation is correlated
  with. The self-join form keeps the relation self-contained and lets the
  renderer recurse blindly; `LATERAL` is a renderer optimisation on the
  same plan, decided by measurement.

## Open questions

Numbered, each with the assumption the design proceeds on and whether the
answer blocks the first MR.

1. **Join keys beyond identity.** Assume identity only (op 5), with typed
   value columns as a later op behind a two-column fidelity check. *Not
   blocking.*
2. **Where op 1 sits — closed by review round 1.** Op 1 runs first and
   always; absorption versus derived table is `lower_refined`'s choice of
   physical form for a pushed barrier, and the absorb rules' preconditions
   become that choice's match arms. The baseline must show the same SQL
   text for every shape they served. *The remaining question is cost:
   moving three rules from refinement to lowering is the second MR's
   largest item. Blocking for the second MR, not the first.*
3. **`LATERAL` versus self-join rendering of a relation whose scan is the
   outer star.** Assume self-join first, `LATERAL` measured afterwards on
   the review app. *Not blocking.*
4. **Op 3 on a user-written sub-select — closed by review round 1.** It
   was a correctness question, not a widening: the boundary restriction
   (3a) applies to any `Exporting` scope on a `Join` side or a `LeftJoin`
   right side; whether it reaches the scan is 3b's per-operator walk, which
   stops at `Slice` and a keyless `Group`. *What is open is only whether
   3b's table is complete — a reviewer who can name an operator it
   commutes through wrongly reopens it. Blocking for the first MR, since
   #466's own body has a `Group` and the keyed-`Group` arm is the one it
   needs.*
5. **An element as an interface variable.** `?cs` in #464 is a structure,
   not a term the serialiser can emit (`SELECT *` asks for it). Assume the
   barrier exports it as a column of kind `Structure` that the outer scope
   may join on (an element identity — see GitHub PR #37, "Wasm element-identity bindings", in this repository) but not project; `SELECT *` over it then declines op 4 and stays
   a fetch, while `SELECT ?name ?kpFromMeter ?trackName …` (what the export
   actually needs) lowers. If the human wants `SELECT *` to lower, the
   element needs a term — a decision about the datamodel, not the planner.
   *Blocking for #464's `SELECT *` spelling only.*
6. **Reads of an element's slots as scan paths.** Assume `ScanSlot.path`
   below a multivalued hop resolves relative to the unnested element
   (`hasCoveredSection[each].isReference`) with `BoundElement` reading,
   the way `BindingSpec.containers` already describes per-step containers
   for the renderer. If the plan's `ScanSlot` cannot say "path under an
   element" today, that is the representation change to make first, in the
   fifth-invariant style. *Blocking for #464, not #466.*
7. **`PushNotExists` over a barrier.** Assume out of scope; listed as a
   follow-up row so the table is honest about what the vocabulary would
   serve. *Not blocking.*
8. **The 422 gate.** Assume unchanged: a sub-select whose body does not
   lower still reaches `fallback_can_answer` and is refused for a class too
   large to materialise. *Not blocking.*
9. **`guaranteed` for a `Bind` and for `MIN`/`MAX`/`SUM`/`AVG`.** The
   transfer table says "total over guaranteed inputs" and "cannot error
   over the group's rows". Assume the first MR implements the conservative
   half — a `Bind` is never guaranteed, `COUNT` is the only guaranteed
   measure — and the rest is a widening with its own tests. #466 needs
   `COUNT` only. *Not blocking; conservative is correct.*
10. **Carrying a restriction across a barrier into the fallback fetch.**
    The boundary proof, read downward, lets `keep_what_the_rules_proved`
    narrow the outer fetch by a restriction inside a mandatory sub-select
    on an exported, guaranteed identity with no stop between. Assume the
    first MR does **not** do this — a restriction inside a scope narrows
    only that scope's own fetch — and the widening comes with test 6's
    second half. *Not blocking; refusing is correct, and #466's answer is
    the statement, not the fallback.*

## Staging

Three MRs, each shippable and each parity-tested; the first serves #466
on its own.

1. **Scope, derived properties, barrier, identity join** — `Scope::of`,
   the five properties (`effects` exists; `guaranteed` conservative per
   question 9), scope-local `applies_to_every_answer` and producer-keyed
   `keep_what_the_rules_proved`, scope-aware `Visible`, the two anchor
   changes, ops 3a/3b (the keyed-`Group` arm and the stops), 4 and 5,
   `Op::Relation` + `JoinKey`, contract 5, the four invariants, the
   recursive FROM item, both oracles. Serves a user-written sub-select
   (grouped or top-N) joined on an identity: #466, and the first, second,
   fifth and the three review rows of the table.
2. **The `OPTIONAL` body as a scope** — ops 1 and 2, and the absorb rules
   moved to the lowering's physical choice. Serves the two-read `OPTIONAL`
   over a reference and the lifted condition: the third and fourth rows.
3. **The element-held edge** — the `foreign_key_on` widening and the
   `BoundElement` edge spelling on the statement route, plus whatever
   question 6 decides. Serves #464.

# A body is a relation: lowering a grouped sub-select and an `OPTIONAL` body as one derived table

Status: **implemented** (revision 10; the human chose not to stage).
Items 1–4 of *Staging* are one body of work, in commits titled by item;
item 0 (the oxigraph backport) is consolidator issue #467 and not here.
Where building it showed this document wrong or silent, the sections
below say what changed and the text is amended in place; nothing
diverges silently. The consolidator side (contract check, the new
operator renderings, the `SELECT DISTINCT e.value` structure-dedup fix,
pin bump) shipped as a second MR after a release — built against the
branch unreleased, which is what revision 9 records. Revision 10 is
the last deferred piece, consolidator issue #471: op 3a widened from
the class to the other side's row tests, and the fold under it
finishing for a constant object on an element.

<details><summary>What revision 10 changed (consolidator #471): op 3a carries a row test, the fold takes a constant on an element</summary>

Two gaps revision 8 recorded as built, both closed here and both
amended below in place.

* **Op 3a carries the other side's row tests, not the class alone.**
  As built through revision 9, `RestrictScopeAtBoundary` placed
  `?v ∈ Identity(class)` and nothing else, so a body already typed in
  its own domain (#466's, #470's `?asset a CEA … GROUP BY ?asset`) got
  nothing from the outside and read the whole class where the outer
  scan read 130 tunnels. A `Restriction` now carries a `Predicate`:
  `Class(uri)`, as before, or `Condition(expr)` — a filter condition
  the other side applies to *every* row it hands the join
  (`conditions_on`: the record predicates of the filters on every path
  from a scan of `?v` up to the side's root, through the operators
  that pass their input's rows on — its table is in *Op 3a*) and that
  reads nothing but `?v`'s own record (`is_record_predicate`: `Column`
  readings of `?v`'s slots, literals, comparisons, `IN`, `AND`/`OR`/
  `NOT`; never a variable, a function, a bound element, a containment
  test or a pattern). The semi-join proof is the same sentence with one
  more clause: every `?v` the other side carries satisfies the test,
  and a test with one value per record says the same of `?v` on either
  side. The row-test arm fires only once the side already produces
  `?v` as `Identity(class)` with its scan visible at the insertion
  point — a test on a record needs the record's scan to land on, and
  the oracle reads it back through that scan — so the class arm and
  the row-test arm are one rule in two stages, and a body the user
  already restricted by hand gets no second copy (`applied` in the
  rule). One obligation per predicate, keyed by its text; the ledger
  stays append-only.
* **Op 3b brings a row test to rest and flips it.** A class
  restriction is read by the fold and never rendered; a row test is a
  filter like any other once it can move no further, and 3b — not
  `PushComparisonFilter`, which now leaves every restriction filter
  alone — flips it to `Sql` when the node below runs in SQL and the
  condition renders over the scans visible there. One rule owns the
  filter's position, which is what keeps the plan the plan's and not
  the schedule's: a row test above a keyed `Group` is passed below it
  before it is rendered, never rendered above it. At a `Join` a row
  test goes to the side whose scan of `?v` is visible (a class
  restriction still takes the first side that guarantees `?v`), and a
  filter moved below an `Sql` node is `Sql` on its new input or the
  step declines. `inner_join_groups` unites every restriction filter
  with its input, not only a class one, so a row test between the
  class restriction and the matches does not hide the matches from the
  fold.
* **A constant object on an unnested element is a `BoundElement`
  filter.** Revision 8 recorded "the restriction folds only with a
  variable-object read beneath" — #464's `?cs :isReference true` left
  the block on the engine as three islands. The fold was never the
  problem: `ConstantObjectBecomesFilter` resolved the element's site
  through `subject_site` and then declined it in `condition_for`,
  whose `class_at_path` refuses a collection hop. It now walks the
  prefix with `class_at_path_of` when the prefix crosses a collection
  (the site exists only when that collection's fan-out is below the
  join, so the element is a row there) and reads the slot as
  `BoundElement`, the reading a variable read of the same slot already
  had; the existence half is the nested path, single-valued. A
  containment test on an element's own collection (a multivalued slot
  off an element) still declines: the element is a row, and the third
  reading nothing renders is its array.
* **The oracle keyed a slot's binding by the star's name.** Two scopes
  scanning one variable are two scans, and a condition inside a
  sub-select that reused the outer scope's variable tested unbound; and
  a binding reused above a `GROUP BY` that dropped it did the same.
  `plan_to_algebra` now keys bindings by the scan node, and reuses a
  `Column` binding only where the variable is still in scope at the
  condition (a single-valued slot read again is the same triple joined
  again). The grammar gains the outer row test as a constant and as a
  `FILTER`, and the constant on an element alone and beside a read.

What this changes for the three user shapes, on the consolidator side:
#470's grouped sub-select under an outer tunnel restriction reads the
body over the restricted class, as a derived table on the statement
route and — through `keep_what_the_rules_proved`, which is local to the
naming domain and so admits a row test inside the sub-select's domain
— as a narrowed star on the fetch route; #464's `OPTIONAL` with
`isReference true` lowers as one derived table; the top-N
counter-example and every other `Rejected` row stay refused, each
still a test.

</details>

<details><summary>What the consolidator renderer changed (revision 9), two points</summary>

Building the renderer against this branch found two places where the
plan said less than the renderer needs, both fixed here and both
amended below in place.

* **A projection-less relation body gets the answering projection's
  column list.** Op 1 leaves *no* `Project` under the barrier it
  wraps (`enclose`; the absorb rules and the transparent-barrier
  elision match the bare shape), so the lowering of an enclosed
  `OPTIONAL` body had no projection to anchor `projected_columns` on
  and emitted a bare row set — scan, unnest, join — whose `Unnest` no
  binding discharged. The renderer derives its laterals from the
  bindings and cross-checks them against the unnests (it has to: a
  lateral is spliced where a binding is projected), so it declined the
  whole statement. `scope_columns` now builds the list from an anchor
  node, a variable list and a scope, and the `SubSelect` arm gives a
  body with neither a grouping nor a projection the projection a
  sub-select would have carried — the exports, then every fan-out, then
  every identity — anchored at the body root, in the body's own scope.
  The worked case's `n9` was the design's word for this; it is the
  lowering's, not the builder's.
* **A relation's measure column carries the term it is.** Every measure
  export was described as an integer literal (`measure_descriptor()`),
  so the outer statement serialised `AVG(?seq)` as
  `"1.5"^^xsd:integer` beside the engine's `xsd:decimal`. `TermOf::
  Measure` now carries a descriptor derived from the aggregate and its
  argument's term — `COUNT` an integer; `SUM` the argument's datatype;
  `AVG` the division's (`xsd:decimal`, or the argument's IEEE type);
  `MIN`/`MAX` the argument's descriptor as it stands, an IRI for an
  IRI-valued slot — and `ColumnKind::Measure { descriptor }` carries it
  to the relation column. The renderer serialises the column from that
  descriptor and nothing else.

What the renderer built, for the record (consolidator-server, the
`sparql` package): `plan_ops.PLAN_CONTRACT = 5` checked at the plan;
`row_source_from_ops` (stars, joins with `key` / `left_column` /
`right_column` / `right_reading`, relations) beside the fetch's
`stars_and_joins_from_ops`, which reads a relation *flat* — the body's
stars fetched whole under `<var>__<alias>`, the keyed join dropped, the
fetch bound voided — because an engine re-runs the query over the
fetch; `_from_join_where` with a relation as a FROM item (`( <body> )
AS q0`, the body rendered by the same renderer recursively), the three
keys, and laterals spliced after the item that introduces their star so
an element key and a `bound_element` edge can read them; one lateral
per collection hop shared by every binding that walks it; the occurrence
identifier as a JSON pointer over the holder's identity (every hop
written, single-valued ones too); `by_occurrence` as `WITH ORDINALITY`
and no `DISTINCT`; a required value read *off* an element as an `IS NOT
NULL` (the element's existence never covered it — a section without the
slot was an extra unbound solution, on the plain statement route too);
and records deduplicated **by identifier** across every fetch of a
plan, merging projections — a record boxed twice put two sets of blank
nodes in the store, and the engine leg counted every section twice.
Every table row is an oracle case against the engine over the same
rows, and the `Rejected` rows are endpoint tests naming `?s (in
sub-select 1)`.

</details>

<details><summary>What building it changed (revision 8), point by point</summary>

* **Op 1 is the builder's, not a rule.** Every `LeftJoin` right side and
  every `EXISTS` block is built already enclosed
  (`Builder::enclose`), and an off-spine `Slice`/`Distinct`/`Reduced`/
  `Project` builds its sub-query on a fresh spine wrapped whole in one
  `SubSelect`. `EncloseOptionalBody` as a rewrite would have had to
  find the body's edge after the fact; the builder has it. The pinned
  trees (test 9, `the_barrier_wraps_the_complete_sub_query`) are the
  contract. `PlanOp::SubSelect` carries `domain: Option<usize>` — the
  naming domain a user-written sub-select opened, `None` for the
  builder's own enclosure, which opens none.
* **The absorb rules stay refinement rules; the physical choice is a
  *transparent* barrier.** `AbsorbOptionalRead` / `AbsorbOptionalReference`
  match *through* the barrier and unwrap it; the lowering then elides a
  barrier of *transparent shape* (an optional body under a `LeftJoin`
  with a recorded reference edge) and renders today's SQL, and lowers
  every other barrier as a `Relation`. Moving absorption *into* the
  lowering would have meant two places that know the optional's shape.
  `PushLeftJoin` finds its edge only across transparent shapes
  (`feeds_through_transparent_shapes`).
* **`outputs(Scan)` includes an absorbed optional's bindings** (every
  slot with a variable, whatever its presence). The transition check
  fired on `AbsorbOptionalRead` otherwise: the export it kept had, by
  the old definition, no producer.
* **A barrier may export a variable nothing below produces** (a name
  the sub-select projects but never binds): the export is an unbound
  column, and *closure on producers* holds of the producers that exist.
* **`JoinKey::Cross` is raised only when one side is a scalar relation**
  (a keyless `Group`, or a `LIMIT` of at most one). A cross join of two
  bags with no key is correct and made a disconnected `OPTIONAL` lower
  as a product; the scoper's connectivity refusal is the contract there,
  so op 5 does not compete with it.
* **`applies_to_every_answer` is local to the naming domain and gains no
  keyless-`Group` arm.** A narrowing proved inside an `OPTIONAL` body
  reached the outer fetch through a shared star name in the materialise
  corpus; keying by domain closed it, and the keyless-`Group` arm the
  document proposed was what let *no spelling of an identity* through.
* **Structures are keys and count arguments when not serialising.**
  `key_is_readable` accepts a structure (`is_structure`) and a relation
  column; `PushGrouping` requires serialisable keys only in the root
  scope, where the key is an answer. `COUNT(?d)` over an element groups
  by its occurrence, as *What may cross a relation* promised.
* **Op 3a's preconditions, as built:** one side of the join is a
  barrier (a restriction inside one scope changed constant-object
  shapes and is never needed: the fold sees the class); `?v` is
  guaranteed on both sides; the other side has an `identity_class` for
  it; the restricted side binds `?v` only as `Computed` (read as a
  star, `reads_as_a_star`), does not type it itself (`types_itself`,
  which also needs `FoldMatchesIntoScan` to count classes per scope as
  a *set*, so two matches of one class on one star are not "two
  classes"), is not an absorb shape (`only_reference_reads &&
  absorb_reference_shape`, `single_read_body`: the rule would change
  the SQL of a query the absorb rules already serve), and the consumer
  the filter is inserted under is a scope root (`[E]`). *Revision 10:*
  those are the **class** arm's preconditions; the **row-test** arm
  (*What revision 10 changed*) fires once the side produces `?v` as
  the identity of that class, and needs none of them.
* **A restriction 3b cannot carry to a scan is an engine node**, not an
  `IN (SELECT …)` rendering: `Expr::InClass` becomes `EXISTS { ?v a
  <C> }` on the engine leg and the row stays whichever outcome the rest
  of the plan earns. The `IN (SELECT …)` spelling is not built.
* **The restriction folds only with a variable-object read beneath.**
  `FoldMatchesIntoScan` takes an `InClass` filter as the star's type
  source and reroutes the filter's consumers to its input; with only
  constant-object reads under it there is no slot to fold and the rule
  declines rather than orphan the join. *Revision 10:* this was read
  as the reason #464's `isReference true` block stayed on the engine;
  it was not — the body has a variable-object read (`hasCoveredSection
  ?cs`) and folds, and it was the constant on the *element* that
  declined, in `ConstantObjectBecomesFilter`. Closed there.
* **The 3b log is a `Display` without the path:** the rule-order test
  compares printouts, and two schedules that reach one plan by different
  transfers must print the same.
* **The scoper is keyed by an alpha-renaming:** `sparql_domains::qualify`
  renames every variable of a sub-select to `<name>__d<n>` with the
  builder's numbering, so `(naming domain, variable)` is one string
  through the scoper unchanged; `resolve` reads the plan's `Scan` by
  domain number and types an untyped star from it (`Scoping::Record`,
  then `resolve`, then `Scoping::Resolve(&hints)` with an agreement
  check). The `sparql_scope` linter binding still runs the scoper
  alone, so it refuses what `resolve` would type — the linter is a
  stricter check, deliberately, until the consolidator asks otherwise.
* **`Obligation::Boundary(Restriction)`** joins the append-only ledger;
  `refinement_never_changes_the_obligations` holds as a prefix
  property (boundary obligations are appended).
* **Contract 5's fields, beyond the document:** `Op::Join.right_reading`
  (`any_element` is the fetch's containment, `bound_element` the
  statement's own row), `Op::Unnest.dedup` (`by_value` / `by_occurrence`),
  `BindingSpec.relation` and `.occurrence`; `PushNotExists` declines an
  element-held key. On the Python side: `PlanOp.relation_alias` /
  `relation_body` / `relation_columns`, `join_key` + `join_key_left` /
  `join_key_right`, `right_reading`, `dedup`; `PushdownBinding.relation`
  / `.occurrence`; `JoinColumn`, `RelationColumn`.
* **`resolve_terms`: only identities beside slots resolve to the
  identity.** Two `Slot` terms of one variable stay distinct; two
  `Structure` terms agree only on `(holder class, path)`.
* **`Op::Join`/`LeftJoin` with a reference edge demand the edge's
  variables** (the oracle caught a barrier that had pruned the referenced
  star, lowering as a product), and an absorbed optional collection is
  translated as optional (the scan arm of `plan_to_algebra` skips a
  multivalued optional read, which its `Unnest` arm renders).

Test counts at the head: 536 library tests and 5 stub tests, of which
the per-rewrite oracle runs every rule over every grammar query, three
schedules, `MAX_ROUNDS = 1`, and every `Rejected` row of the table is a
test asserting the refusal.

</details>

**Revision 7**, after
[review round 6](https://github.com/Kapernikov/asset360-rust/pull/49#issuecomment-5734065400),
which accepted AC5 (the whole-path occurrence) and `demand` as the
prune's precondition, and found one gap: the post-state invariant
*demand is exported* cannot catch a prune that bypassed its match,
because `demand` is recomputed from the pruned plan and shrinks with
the interface it was meant to guard. Revision 7 replaces it with a
**transition check in the driver** — `refine` records what each
barrier's consumers demand *before* a rule edits the plan and refuses
the edit if a demanded export is gone after. Self-assessed against the
criteria (next section): all ten met as design contracts; round 7
decides AC4 (and AC9's test 3).

<details><summary>What revision 7 changed, point by point</summary>

* ***Demand is exported* is withdrawn as a state invariant.** Round 6's
  table: after an invalid prune of `?y` under `COUNT(DISTINCT *)` the
  `Group` demands `{?x}` and the barrier exports `{?x}`; the invariant
  holds of the wrong plan. Likewise `Distinct`, and a `Minus` whose only
  shared variable was pruned — the dependency leaves with the export.
* **Its replacement is a transition check, *no demanded export is
  dropped*** (*What may cross a relation*, part 4; *Evidence that
  survives rewriting* → *One check relates two plans*). Before
  `rule.apply`, `refine` records for every barrier *b* the set
  `kept(b) = demand_above(b) ∩ exports(b)` on the unmodified plan,
  keyed by *b*'s `NodeKey`; after, the node *b* resolves to (itself,
  or its successor through `retired`) must still output every name in
  `kept(b)`. It runs for whatever `apply` did, matcher or no matcher,
  and is a `RuleFailure` in every build, because the end-of-`refine`
  state check a release build relies on cannot see it. It is not a
  cached derived fact: it lives for one application.
* **Test 3's three bad prunes go through `refine`**, not the rule's
  method, and each additionally asserts that `Plan::check` *passes* on
  the pruned plan — the reason the check is a transition is itself
  pinned. The before/after oracle (test 2(d)) stays the semantic
  backstop.
* **Stated consequence:** a rule may not remove an observer and prune
  what it observed in one edit; that is two rewrites, each checked.
* **Alternative rejected:** an immutable observer contract written on
  the barrier at construction — the one stored property in a design
  where every other is recomputed, and one every rule that adds or
  removes an observer would have to maintain.
* **The acceptance table:** AC5 met (round 6 agreed); AC4 and AC9 name
  what revision 7 changed and wait for round 7.

</details>

<details><summary>What revision 6 changed, point by point</summary>

* **`demand(n)` is the sixth derived property** (*What a relational
  subtree derives*). Per operator it says which producers a node
  *observes*: named references for most; **every output of the input**
  for `Distinct`, `Reduced` and a `Group` carrying `COUNT(DISTINCT *)`;
  the shared variables for `Join`, `LeftJoin` and `Minus`; the
  substituted variables of an `EXISTS` body; and every output for any
  operator, expression or aggregate the analysis does not know. It is
  computed from the current plan when asked, never cached.
* **`PruneUnusedExports` matches on `demand`**, not on "no reference
  above resolves to the producer". Its equivalence is the
  bag-projection argument *plus* "no consumer observes the column",
  the second half being `demand`'s contract, and a new invariant,
  *demand is exported*, catches a prune that lied. Round 5's
  `COUNT(DISTINCT *)` over `VALUES (?x ?y)` is pinned in test 12 (the
  answer is `2`), with `DISTINCT`/`REDUCED` outside and a `MINUS` on an
  export beside it and in the grammar.
* **An occurrence is the whole path.** `Occurrence = (holder identity,
  hops)`, one hop per collection step from the record root to the
  element, each hop the slot and the position in it (a list ordinal, a
  mapping key). That is the walk on which the turtle writer mints the
  blank node, and the path `skolem: true` spells as an IRI, so
  `parts[0].children[1]` and `parts[1].children[1]` are two
  occurrences. Rendered as **one text column** composed from each
  lateral's ordinal — the renderer already emits one lateral per hop —
  so it crosses a relation, a join, a group and a nested `Unnest` as
  any scalar column does; a nested `Unnest` appends one hop.
  `JoinKey::Element` compares the whole identifier. Test 12 and the
  grammar gain the nested fixture.
* **`resolve` reads the domain's own scan whichever way it was typed**
  — by its local type match or by 3a/3b. The pipeline prose and
  question 10 said "only when 3a placed the restriction", which
  contradicted the accepted local-typing contract; both now say the
  same thing, and the scoper's own `Typed` record is an agreement
  check against the plan, not a second source.
* **The acceptance table** has one status column; AC4 and AC5 name what
  revision 6 added and wait for round 6.

</details>

<details><summary>What revision 5 changed, point by point</summary>

* **The acceptance criteria are the definition of done.** Round 4's
  ten criteria, verbatim, each with where in the document it is met.
* **The pipeline is one sequence with one interpretation of a star's
  class** ("The pipeline" section). Today the scoper refuses an untyped
  star before the naive plan exists, which would refuse the outer read
  3a exists to type. Now the scoper *records* an untyped star, the
  plan is refined, and a `resolve` step types the star from the refined
  plan's `Scan` for it or refuses — the refusal is final there and
  nowhere earlier. The scoper copies no rule: it reads a fact the plan
  derived. Four endpoint outcomes are named (`Statement`, `Fetch`,
  `Rejected`, `FetchDeclined`) and every table row says which.
* **Naming domain and evaluation unit are two things.** A sub-select
  makes a variable private; an `OPTIONAL` body or a `UNION` arm does
  not — its `?a` *is* the outer `?a`. Stars are keyed by naming domain;
  a class travels across a combining node only by that node's proof,
  spelled per operator in one table (`union_branches` is the precedent
  in the scoper).
* **A structure may cross a relation; it may not be an answer.**
  `ColumnKind::Structure` identified by *occurrence* (holder, path,
  ordinal) — the engine leg's turtle writer mints one blank node per
  occurrence, so that is bnode identity. Representable and
  serialisable are two properties; `key_is_readable` conflates them
  today. A new equivalence, `PruneUnusedExports`, drops an export
  nothing above reads (bag projection). Pinned: scalar projection over
  #464 is a statement, `SELECT *` is a fetch (an explicitly unsupported
  result form until elements have a term), an element joined outside is
  a join by occurrence. Questions 5 and 6 closed.
* **Found on the way: the unnest dedup is wrong for structures.**
  `SELECT DISTINCT e.value` is right for a scalar slot (a graph is a set
  of triples) and wrong for an inlined structure (two identical
  elements are two blank nodes). It affects today's flat `GROUP BY ?a
  COUNT(?cs)`. The `Unnest` contract states the rule per step kind, and
  MR1 fixes it.
* **Evidence holds keys, the proof is recomputed.** `Node` gains a
  stable `NodeKey`; the nine renumbering sites become one primitive;
  the ledger's `OutputSlot`/`JoinOccurrence`/steps hold keys; a removed
  node is *retired* with its successor. The chain-of-3b-arms invariant
  walks the *current* plan up the unique-consumer chain from the
  discharge to the origin; the stored path is a log asserted against
  it. "Already carries a restriction" is a canonical obligation key in
  the append-only ledger, not a position. Progress: a decreasing
  measure per rule; at the budget the plan is correct and lowers.
* **The matrix reconciled.** The top-N counter-example's outcome is
  `Rejected` (`query_unscoped`), not "correct fallback" — no fetch
  preserving the answer exists; test 7 asserts that and the oracle's
  answer, no SQL. The nested row derives its untyped outer read by 3a.
  `GROUP BY ?cs` in #466's key bullet lowers under the structure
  contract. Question 10 is closed by the `resolve` step.

</details>

<details><summary>What revision 4 changed, point by point</summary>

* **The scoper's refusal is a contract, not a layout.** Revision 3
  refused "an untyped read under a `Slice` or keyless `Group` in another
  scope", which catches the counter-example's shape and not its cause:
  `?s a :C . { SELECT ?x WHERE { ?s :p ?x } }` has neither operator and
  loses a row the same way, because the scoper keys stars by variable
  *name* across every `Project`. The contract is now **star inference is
  per scope**: a triple's subject is a star of the scope it is written
  in, a star acquires a class only from its own scope, and a private
  inner variable with an outer namesake is exactly the untyped subject
  the scoper already refuses. Alpha-renaming a private variable must
  not change the fetch, and that is the test.
* **The boundary wraps the complete sub-query, modifiers included.**
  spargebra gives `Slice → Distinct → Project → OrderBy → body`, and the
  builder turns only the `Project` into a `SubSelect`, so today the
  barrier's consumer is the sub-query's own `Slice`, not the join, and
  3a's "inside the boundary node" would have put the restriction *below*
  the limit. The builder now builds a sub-query on a spine of its own
  and wraps the whole of it in the barrier. Op 4, 3a, the tail walks and
  the scoper all read that one node; the normalised trees are pinned.
* **Transfer paths compose and branch.** The invariant validates each
  step against the scope a cursor is in and advances the cursor at a
  `Transfer`, so S → T → U is a chain, not a special case; a 3b arm that
  distributes (`Union`, both arms) *splits* the obligation into one per
  branch, each with its own path, and the ledger accounts for the parent;
  the `Join` arm pushes to one side in the 3a/3b MR.
* **Ownership is of the path, not the barrier.** Every node a boundary
  restriction passes or discharges at has exactly one consumer, or the
  rule declines (clone-at-the-first-shared-node is the widening); the
  plan root is exempt from "exactly one consumer".
* **3a restricts a join side, not only a scope.** Typing the MR1
  examples locally exposed that the outer read beside a typed sub-query
  (`… LIMIT 3 } ?s :hasName ?n`) has no type in its own scope, and 3a
  as written matched only an `Exporting` scope, so nothing would ever
  have typed it. The proof is about a join side; the match now says so,
  and that shape is the 3a/3b MR's, refused (correctly — today it is
  answered wrong) in MR1.
* **Consistency**: `Unnest`'s `outputs` row keeps its input's outputs;
  the MR1 examples carry explicit local types; the #464 walk-through
  names 3a/3b before folding the nested `OPTIONAL`'s scope, so staging
  item 4 depends on item 2; `plan_to_algebra`'s non-naive nodes get
  direct tests.

</details>

<details><summary>What revision 3 changed, point by point</summary>

* **#464's 17 s is an oxigraph optimizer bug, not the memory store, and
  it is fixed upstream on `main`** — [oxigraph PR
  1733](https://github.com/oxigraph/oxigraph/pull/1733), merged
  2026-06-04, absent from every 0.5.x release including 0.5.11. Measured
  on a synthetic copy of the shape: 7.9 s → 13.5 ms for 1 167 assets with
  the 30-line hunk applied to sparopt 0.3.7. The #464 section says so,
  and the runtime and staging sections no longer sell the element-held
  edge as the way to make that block fast: it is the way to make it
  *answer as a statement*, which is a capability, not a rescue. The
  spine is unchanged — #464 stays the second worked case of the same
  vocabulary.
* **Op 3's obligation crosses a scope by an explicit transfer**, not by
  breaking "obligations stay in their scope": `o_boundary` carries its
  path, each step a 3b arm, and an export step translates the slot. The
  invariant validates the path against **every** stop in the 3b table,
  not two of them.
* **The boundary filter lives inside the boundary node**, so the scope
  root stays the `SubSelect` and op 4's match is unaffected.
* **A contextual restriction needs the scope to have one consumer**:
  an `Exporting` scope has exactly one consumer, checked as an
  invariant; `o_boundary` names the join occurrence and side that
  justified it.
* **The top-N counter-example is a refusal, not a lowering** — and,
  found while checking it, **today's scoper answers it wrong on the
  fallback route**: it merges the sub-select's untyped `?s` into the
  outer `Signal` star, so the fetch is Signals only and the engine's
  `LIMIT 1` picks the first *Signal* where SPARQL picks the first
  record. That refusal is now the first MR's.
* **3a/3b leave the first MR.** #466 does not fire them (its inner type
  is explicit); they ship with their transfer and ownership contracts as
  their own MR.
* **Test 2(d) evaluates each rewrite**: a `plan_to_algebra` translation
  of the logical subset, evaluated on the in-memory oracle before and
  after every single rewrite, in the enclosing join context for 3a.
* **Four corrections**: `SUM`/`AVG` over an empty group are `0`, not
  unbound (`MIN`/`MAX` error); a unary `Unnest` keeps its input's
  outputs and guarantees; a grouped body does not multiply the outer row
  *when its whole key is the join key*, not because it has a `Group`;
  the #464 derivation admits the nested `OPTIONAL` recursively (op 1,
  op 4, op 5) and chooses absorption at lowering, instead of calling
  `AbsorbOptionalRead` during refinement.

</details>

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

## Acceptance criteria — the definition of done

Review round 4 stated these ten as the non-negotiable criteria for the
full design. "Met" meant the **design contract** is specified; at
revision 8 each row is also backed by the tests it names, on this
branch. Each row names where the contract lives;
a row that is not met says "not yet — see Q*n*". The criteria do not ask
for a particular optimizer framework, a cost model, an exact SQL
spelling, or every SPARQL construct; they ask that the chosen domain be
correct and composable and its boundaries honest.

| ID | Criterion | Where it is met | Status |
|---|---|---|---|
| **AC1** | **A relation boundary covers the complete logical unit.** Projection, grouping, `HAVING`, ordering, duplicate handling and slicing retain their algebraic order; construction does not depend on SQL admission. | "The fact: a scope" → *The boundary is the complete sub-query*; pinned trees, test 9. | Met (round 4 agreed). |
| **AC2** | **Binding identity and scope mean the same thing in every phase.** Private-name alpha-renaming cannot change inference or answers. Class restrictions cross boundaries only through justified interfaces; scoping cannot preempt a valid optimizer derivation. | "The pipeline": the scoper records, the plan derives, `resolve` decides, in that order; *Naming domain versus evaluation unit* gives the correspondence per operator for sub-selects, `OPTIONAL` bodies and `UNION` arms; tests 8 and 10. | Met (round 5 agreed). |
| **AC3** | **Rules consume sound operator properties.** Outputs, guaranteed bindings, term identity, correlation and effects have explicit transfer contracts. Unknown facts are handled conservatively; lexical name equality is never a substitute. | "What a relational subtree derives"; `term_of` now answers `Structure` for an element (*What may cross a relation*); Q9 keeps the conservative half; `demand` defaults to "every output" for an unknown operator. | Met (round 5 agreed). |
| **AC4** | **Every rewrite is an equivalence under explicit preconditions and in its stated context.** Preserve RDF-term equality, bags, unboundness, expression errors/effects and modifiers. Contextual restrictions preserve the enclosing join, not necessarily the restricted child. | Ops 1, 2, 3a, 3b, 4, 5 and `PruneUnusedExports`, each with *Match / Edit / Equivalence / caught by*; 3a is proved on the enclosing join; `PruneUnusedExports` matches on `demand` (*What may cross a relation*, part 4) and is caught by the transition check *no demanded export is dropped* (*Evidence that survives rewriting* → *One check relates two plans*); test 2(d) evaluates each rewrite alone. | **Revision 7.** Round 5: the precondition ignored whole-mapping observers. Round 6: precondition and equivalence accepted; the post-state invariant could not catch a prune that bypassed the match, since `demand` shrinks with the interface. Now: the check is a transition in `refine`, on the pre-edit demand, every build, and test 3 pins that a state check passes where it fails. Self-assessed met; round 7 decides. |
| **AC5** | **Logical interfaces and physical implementations compose.** Every supported exported value has a defined representation and valid consumers; recursive lowering and absorption preserve the same interface and multiplicity. | *What may cross a relation*: `ColumnKind` incl. `Structure` by occurrence — `(holder, hops)`, the whole path, one text column, composed per `Unnest` — its consumers and how it crosses a relation, a join, a group and a nested `Unnest`; demand pruning; pinned results for #464 and the nested fixture; the `Unnest` multiplicity contract (dedup by value for a scalar step, by occurrence for a structure step); Q5 and Q6 closed. | Met (round 6 agreed). Round 5 had found `(holder, path, ordinal)` collapsing `parts[0].children[1]` with `parts[1].children[1]`; the whole-path occurrence, transported as one scalar column, closed it. |
| **AC6** | **Contextual rewrites cannot affect another consumer, and their evidence survives composition.** Ownership is checked along mutated paths; transfers compose to arbitrary depth; splits account for every branch; later mutations cannot leave stale justification. | Op 3a's ownership rule; 3b's cursor and `Split`; *Evidence that survives rewriting*: `NodeKey`, one renumbering primitive, `retire`, the invariant recomputed from the current plan; test 11. | Met (round 5 agreed). |
| **AC7** | **The optimizer has a defined safe progress policy.** Repeated rules are idempotent or make bounded progress; property updates and proof validation follow mutations; rule scheduling affects cost/choice, not correctness. | *Evidence that survives rewriting* → *Progress*: a measure per rule, the canonical obligation key, derived facts recomputed on demand, behaviour at `MAX_ROUNDS`, schedules; tests 2(d) and 11. | Met (round 5 agreed). |
| **AC8** | **Lowering refusal never licenses an incorrect fallback.** The fallback fetch must preserve the original query's answers, with justification for any narrowing; otherwise report a deliberate refusal. A statement refusal, a fallback execution and a whole-query rejection must be distinguished. | "The pipeline" → *Outcomes*: `Statement` / `Fetch` / `Rejected` / `FetchDeclined`, and every capability row names its outcome; narrowing justification is the producer-keyed ledger plus `resolve`; the top-N row is a `Rejected`. | Met (round 5 agreed). |
| **AC9** | **Equivalence is tested independently of SQL admission and of the optimized fetch.** Evaluate before/after each rewrite, use a complete-fixture oracle, compare bags of RDF terms, cover empty/unbound/duplicate and nested cases, and test the translator's new nodes directly. | "Tests that would prove equivalence": `plan_to_algebra`, the in-memory oracle, bags, the grammar; tests 10 (full entry point), 11 (lifecycle) and 12 (structure interface, whole-mapping observers, nested occurrence). | Met as a test design (round 5 agreed); round 6's correction applied — test 3's three bad prunes run through `refine` so the transition check is what rejects them, and each asserts `Plan::check` passes on the pruned plan. Round 7 confirms. |
| **AC10** | **The complete supported domain is explicit and internally consistent.** Motivating examples follow from generic rules; unsupported cases have property-based reasons. Every claimed capability has a valid derivation and acceptance test. Staging does not excuse unresolved contracts. | "The same vocabulary" table with an outcome per row and 3a named where an outer read is untyped; "What stays refused" with the property each rests on; every open question either closed or marked not blocking with its assumption; staging delivers contracts this document already states; the "same vocabulary" table gains round 5's whole-mapping row. | Met (round 5 agreed). |

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
takes 15 ms per asset for the block.

**Where the 17 s goes — corrected in revision 3.** The py-spy profile
on #464 showed oxigraph's memory-store index walk, and revision 2 read
that as the store's cost. It is not: it is the *optimizer's* join order
inside the `OPTIONAL` body. oxigraph 0.5.11's `sparopt` reorders an
`OPTIONAL` body with the query's global bindings only, ignoring what the
preserved side binds, so the body starts at the pattern that looks most
selective in isolation — `?cs :isReference true`, a constant object —
and rescans every reference section of the whole fetched set **per
outer row**; `?a :hasCoveredSection ?cs`, the pattern that would have
bound `?cs` from `?a`, runs last. Quadratic, and the index walk is that
rescan. Reproduced on a synthetic copy of the shape (asset360-rust
branch `exp/464-oxigraph-1733-backport`, `examples/bench464.rs`): 300
assets, `explain` shows 90 000 leaf results for the `isReference`
pattern; 1 167 assets answer in 7.9 s, 300 in 0.5 s. Reading the store
through `oxrdf::Dataset` instead (no MVCC, `on_queryable_dataset`) is
*slower* (11.8 s), which settles that the store is not the cause.

Upstream has the fix: [oxigraph PR
1733](https://github.com/oxigraph/oxigraph/pull/1733), "fix quadratic
scaling of OPTIONAL in a JOIN on FK", merged to `main` on 2026-06-04 —
it reorders the body with the preserved side's bindings and costs a
lateral against a hash join. It is **not on the 0.5.x line**
(`v0.5.11` diverged from it; `sparopt 0.3.7` still reorders the right
side with `input_types`), and `main` is heading for 0.6 with a renamed
algebra (`GraphPattern` → `QueryExpression`), which this crate cannot
follow without moving `spargebra` too. Applying the PR's optimizer
hunk to `sparopt 0.3.7` under `[patch.crates-io]`: the same 1 167
assets answer in **13.5 ms**, 5 000 in 64 ms, the plan is a hash
`LeftJoin(keys = ?a)` with the body evaluated once, and this crate's
suite is green (515 + 5 tests). Related upstream threads:
[#1883](https://github.com/oxigraph/oxigraph/issues/1883) (the same
rescan under `GROUP BY`, answered "fixed in main with #1733"),
[#1951](https://github.com/oxigraph/oxigraph/issues/1951) and open PR
[#1955](https://github.com/oxigraph/oxigraph/pull/1955) (the same
class of bug for a generated `Lateral` inside `UNION`/filtered groups,
still open on `main`).

So the 17 s has a fix that is one dependency patch and no planner
change, and #464's place in this document is **not** speed. It is that
the block still runs on the engine at all: a fetch that materialises
every asset of the sub-zone and every track they reference, a result
that does not page by the statement (#457), a `SELECT` that cannot be
the answer. Those are what the element-held edge buys, and they are
worth what the staging section says they cost — no more. #464's thread
names why no rule takes the block, and both reasons are the *same*
missing fact:

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

One representational fact, six derived properties every relational
subtree carries, one pipeline that reads them in one order, one lowering
target with its column kinds, and six equivalence ops (one of them in
two halves). Five of the six are new rules (ops 1, 2, 3, 4 and the
small `PruneUnusedExports`); op 5 and the element-held edge are
widenings of rules that exist, each a precondition made precise rather
than a branch added. Nothing in the
list names a predicate, a class, or a query.

### The fact: a scope

```rust
/// Where a subtree is a unit of its own: evaluated to solutions, then
/// combined with the outside through its interface and nothing else.
///
/// Computed by `Scope::of(plan, root)`, never written by hand. A `SubSelect`
/// node's input — the *complete* sub-query, its own `Slice`/`Distinct`/
/// `Project`/`Sort` included — is one; so is the right side of a
/// `LeftJoin`, of an `AntiJoin`, of a `Minus`, and each arm of a `Union`.
/// `Plan::scope_of(node)` answers which scope a node is in, and
/// `Plan::scope_root(scope)` its root.
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

**The boundary is the complete sub-query, and the builder constructs
it.** This is the representation fix review round 3 asked for, and
revisions 1–3 all assumed it without having it. spargebra parses a
sub-query with modifiers as, outside to inside,

```text
Slice → Distinct → Project → OrderBy → (Filter → Group → …) → body
```

and `Builder::pattern` today turns only the `Project` into a
`SubSelect` — the first `Project` met off the spine — while `Slice` and
`Distinct` above it stay ordinary nodes of the *enclosing* scope. So for
`{ SELECT DISTINCT ?s WHERE { … } ORDER BY ?s LIMIT 1 }` the barrier's
consumer is the sub-query's own `Slice`, not the join; "the scope root is
the `SubSelect`" is false; and revision 3's 3a edit, `input :=
Filter(input, …)`, would have placed the restriction *below* the
limit — the round-1 counter-example, reintroduced by the representation.

The construction, in `Builder::pattern`: an off-spine pattern whose top
is a modifier (`Slice`, `Distinct`, `Reduced`, `Project` — spargebra
emits these only at a query's top, so meeting one off the spine *is*
meeting a sub-query) is built as a query of its own, **on a fresh
spine**: its `Project` becomes a `Project` node, its `Sort`/`Distinct`/
`Slice` sit where the algebra puts them, exactly as at the plan root.
The builder then wraps the chain's top in `SubSelect { input, vars }`
with `vars` the sub-query's projection. The barrier is therefore always
one node kind, always the direct input of the node that combines the
sub-query with the outside (a `Join`, a `LeftJoin`, the plan's own
spine), and always has the complete modifier subtree beneath it:

```
subselect [?s]                 ← the barrier; scope root; the join reads this
  slice   limit 1
    distinct
      project [?s]
        sort ?s
          match ?s :p ?x
```

Three consequences, each a rule that becomes simpler rather than one
that gains an arm. `grouping_tail` and `projection_tail` stop at the
barrier the way they stop at the plan root today, and accept the same
chain (`Bind`/`Filter`/`Sort`/`Distinct`/`Slice`/`Project`) below it,
so `PushGrouping`, `PushProjection` and the sort/slice rules treat a
sub-query as the query it is. Op 3a's `input := Filter(input, …)` now
sits *above* the sub-query's `Slice`, where the semi-join proof holds,
and 3b meets the `Slice` on the way down and stops. And op 4's match
needs no "no modifier above it inside the scope" clause: the barrier
is the scope root by construction, and the modifiers are nodes of its
input that must be `[S]` like any other. This normalisation is the
builder's, so it happens whether or not anything lowers, and
`plan_to_algebra` translates the barrier as the identity (its input is
the sub-query) and the `Project` as the sub-query's projection.

**Pinned trees.** The naive plan for a typed top-N (`ORDER BY … LIMIT`),
a grouped sub-query with `HAVING`, one with `DISTINCT`, and one with
`OFFSET` are golden printouts in `sparql_refine.rs`'s tests, asserting
the barrier at the top and each modifier beneath it in the algebra's
order — the claim "a `SubSelect` is always the scope root and the direct
join input" is then a test, not a sentence. A sub-query with no
modifier (`{ SELECT ?a (COUNT(?cs) AS ?n) … GROUP BY ?a }`, #466) gets
`subselect → project → bind → group`, one identity `Project` more than
today's tree; `PushProjection` inside the scope absorbs it the way it
absorbs the query's own.

### What a relational subtree derives

Every scope root, and every node, answers the same six questions, each an
analysis with a transfer function per operator. Rules consume the answers;
none of them re-walks the plan for descendants. That is the composability
claim in a form that can be tested: a new operator adds six arms to six
functions, and every rule that reads them applies to it without knowing it
exists — and an operator that adds none is treated by the sixth,
`demand`, as observing everything, which is the conservative answer.

| property | question | transfer, per operator |
|---|---|---|
| `outputs(n)` | which variables may be bound at `n`'s output | `Match`/`Scan`/`Values`/`Path`: what they bind. `Unnest`: **the input's outputs plus** the element (a unary operator, like its `guaranteed` row — revision 3 fixed one row and not the other). `Join`/`LeftJoin`: union of both sides. `Union`: union. `Minus`/`AntiJoin`: the left side. `Filter`/`Sort`/`Distinct`/`Reduced`/`Slice`: input. `Bind`: input + the bound variable. `Group`: keys + measures. `Project`/`SubSelect`: `vars`. |
| `guaranteed(n)` — *certainly bound* | which of `outputs(n)` are bound in **every** solution | `Match`/`Path`: all. `Scan`: identity + `Required` slots. `Unnest`: **the input's guarantees, plus** the element when the read is required (a unary operator keeps what came in — #466 needs `?a`'s guarantee to survive the unnest). `Values`: rows with no `UNDEF` in that column. `Join`: union of both sides. **`LeftJoin`: the left side only.** `Union`: the intersection of the arms. `Filter`/`Sort`/`Distinct`/`Reduced`/`Slice`: input. `Bind`: input, plus the variable iff the expression is total over guaranteed inputs (an arithmetic on a guaranteed variable; not a `Bind` that can error). **`Group`: a key iff it is a variable guaranteed by the input, or an expression total over guaranteed inputs; a measure iff its aggregate guarantees a value** — `COUNT` always; `MIN`/`MAX`/`SUM`/`AVG` only when the aggregated expression is guaranteed and cannot error over the group's rows; over an empty input a keyless `Group` yields `COUNT` = 0, **`SUM` = 0 and `AVG` = 0** (§18.5.1.2–3, the sum of nothing is the integer zero and `AVG` is defined as `0` when the group is empty), and `MIN`/`MAX` **error**, i.e. unbound — revision 2 wrote "the rest unbound", which is wrong for `SUM`/`AVG`; keeping them out of `guaranteed` in MR1 is the conservative analysis, not a description of their runtime value. `Project`/`SubSelect`: intersection with `vars`. |
| `term_of(n, ?v)` | what kind of term `?v` is, when bound | `Identity(class)` for a scan's star; a slot binding (path, reading, presence); `Measure`; `Structure` for an unnested element; a relation column carries its body's answer through a pushed barrier; `Ambiguous` when two producers disagree (a `Union` of two classes). This is today's `Visible::identity_of` / `slot_of` restated as a derived property with a transfer rule for the barrier. |
| `correlated_inputs(n)` | which outer variables the subtree reads | Empty everywhere except a `Testing` scope under `AntiJoin`, where it is the variables the right side shares with the left. A rule that lowers a scope as an independent relation requires it empty. |
| `effects(e)` for an expression | may `e` be evaluated in a different place, or a different number of times, and mean the same | `Expr::evaluates_the_same_out_of_context` today: pure → yes; `RAND`/`UUID`/`STRUUID`/`BNODE`/`NOW`/`IRI`, a custom function, an `Opaque` subplan → no. Any rule that moves an expression across an operator checks it. |
| `demand(n)` | which of `outputs(input)` the operator *observes*: a producer it reads by name, or one whose presence or value changes its answer with no name in sight. **Round 5's addition**: the fact a projection-shrinking rewrite needs, which "no reference resolves to it" is not. | `Project`/`SubSelect`: the producers of `vars` (the parser expands a root `SELECT *` to every in-scope variable, so it names them all). `Filter`: the condition's variables, **including every in-scope variable an `EXISTS`/`NOT EXISTS` body mentions** (§18.6 evaluates it by substitution). `Bind`: the expression's variables. `Sort`: the keys'. `Group`: the keys' and each aggregate expression's, and **every output of the input** when an aggregate is `COUNT(DISTINCT *)` (`AggregateExpression::CountSolutions { distinct: true }` counts distinct *mappings*; the non-distinct `COUNT(*)` counts rows and observes nothing). `Distinct`/`Reduced`: **every output of the input** — they compare whole mappings. `Join`/`LeftJoin`: `outputs(left) ∩ outputs(right)`, on both sides — compatibility is decided on the shared variables, so a shared variable no expression names is observed — plus the `LeftJoin` condition's variables. `Minus`: the same intersection (§18.5: `dom(μ) ∩ dom(μ′)` decides whether `μ` is removed). `AntiJoin`: its `correlated_inputs`. `Union`: nothing of its own; what is demanded of the union is demanded of each arm. `Slice`, `Values`, `Scan`, `Match`, `Unnest`: nothing beyond their own reads. **Any operator, expression or aggregate this table does not name, and any `Opaque` subplan: every output of the input** — the conservative default AC3 asks for. `demand_above(n)` is the union over `n`'s consumers of what each demands of `n`, and a consumer that passes a variable through (a `Project` keeping it, a `Filter`, a `Sort`) passes the demand on it through. Computed from the current plan when asked, never cached across a rewrite (*Evidence that survives rewriting*). |

Two of these are the ones the first revision got wrong: `guaranteed` was a
list of node kinds ("a group key or measure" was on it, and a `GROUP BY ?k`
over an optional `?k` keeps the unbound-key group, §18.5 Group allows an
error in a key); `term_of` and `guaranteed` were conflated, so a scan's
identity looked bound at every consumer, including the one below a
`LeftJoin` right side where it is not. **Term identity and binding
guarantee are two properties, and a rule that needs both asks both.**

Five things change to read the scope, and they are the whole "let existing
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
  becomes `current == plan.scope_root(scope)`, and the scope root of a
  sub-query is the barrier that wraps it whole (see "the boundary is the
  complete sub-query" above), so the chain the walk accepts below it is
  the chain it accepts below the plan root — the sub-query's own
  `Project`, `Sort`, `Distinct`, `Slice`, in the algebra's order.
  `PushGrouping` then takes a grouping inside a sub-select exactly as it
  takes the query's own. `PushProjection`'s blanket "any `SubSelect` in
  the plan" bail becomes "a `Group`/`Union`/`SubSelect` **in this
  scope**".
* **`Visible` stops at a scope boundary and reads the barrier's columns
  instead.** `Visible::below(plan, base)` today enumerates every `Sql`
  `Scan` that `feeds` `base`, through anything. With scopes: a scan inside a
  *pushed* barrier is invisible above it, and the barrier contributes one
  `Column` per export, typed by `term_of` at the scope root and flagged by
  `guaranteed` there — `Identity(class)` when the body's scan bound it, a
  slot binding (path, reading, presence), `Measure` for an aggregate,
  `Structure` for an unnested element (by occurrence).
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

* **The scoper infers per scope, and refuses what its scope does not
  type.** The fetch the fallback route runs over is the scoper's
  (`sparql_scoper.rs`), which reads the algebra rather than the plan,
  and it has the same defect the plan's rules had: `tag_triples_by_depth`
  walks through `Project`, `Distinct`, `Slice` and `Group` collecting
  triples, and star construction keys them by **subject-variable name**,
  so a triple written inside a sub-query joins the star of any outer
  variable spelled the same. Revision 3 caught one consequence (top-N)
  and wrote a refusal for that layout — untyped under a `Slice` or a
  keyless `Group` — which round 3 showed is the wrong kind of fix. The
  plain projection loses the same row with no such operator:

  ```sparql
  SELECT ?s ?x WHERE {
    ?s a :C .
    { SELECT ?x WHERE { ?s :p ?x } }
  }
  ```

  With `:a a :C ; :p "A"` and `:b a :Other ; :p "B"` the inner `?s` is
  private, the join is a cross join, and the answer is `(:a, "A")`,
  `(:a, "B")`. A fetch of `:C` records only — the inner `?s` merged
  into the outer star — answers one row. `SELECT ?x (COUNT(*) AS ?n)
  WHERE { ?s :p ?x } GROUP BY ?x` loses the `"B"` group the same way;
  a keyed `Group` is no safer than a keyless one. Adding `Project` to
  a list of forbidden operators, then the next one, is the shape-by-
  shape code this document exists to refuse.

  The contract, the same fact the plan carries: **a star is a
  `(naming domain, variable)`, not a variable.** A triple's subject is a
  star of the naming domain it is written in — each off-spine `Project`
  opens one, as it opens a `SubSelect` in the builder (an `OPTIONAL`
  body does not: see *Naming domain versus evaluation unit* in the
  pipeline section) — and a star acquires its class from its own domain
  only: an `rdf:type` triple there, or the reference-based inference
  `untyped_subject_refusal` already performs, over triples of that
  domain. A star that its own domain leaves untyped is the untyped
  subject the scoper **already refuses** (`Unscoped`, with the rewrite
  advice it already spells), whatever an outer variable of the same name
  is typed as — refused at the pipeline's `resolve` step, after the
  refined plan has had its one chance to type it (3a/3b), and not
  before. So the inner `?s` above is refused
  exactly as `?inner` would be, and the top-N counter-example is refused
  for the same reason rather than for having a `Slice`. Two stars in two
  domains that are typed alike and read alike still merge into one fetch
  star (`star_shape` is name-free already); two typed differently, or
  read differently, are two stars and the fetch is their union — wider
  than today's merged star, never narrower, which is the direction a
  fetch may err in.

  What may cross a boundary is then the same as for the plan: nothing in
  the first MR, and with the 3a/3b MR a restriction that rides an
  *exported* binding with the transfer proof of 3a/3b read downward — an
  outer class reaching an inner star only through an `Export` the join
  actually uses, never through a name, and read by the scoper from the
  refined plan's `Scan` rather than re-derived (the `resolve` step;
  question 10 is closed by it). The test is **alpha-renaming**:
  for every query in the property grammar, renaming each private inner
  variable to a fresh name must leave the scoper's fetch (`sparql_scope`'s
  stars and required fields) and the refined plan identical up to the
  names. A scoper that keys by name fails it on the first shadowed
  variable.

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
  outside (the `LeftJoin`'s `condition`, for a lifted one), never above —
  and never *below* into a nested scope either, with one sanctioned
  exception: a **transfer** recorded on the obligation itself (op 3b,
  "the obligation's path"), which the invariant validates step by step.
  Revision 1's "structurally unreachable" claim about the 18-vs-17 trap
  is withdrawn in favour of this, which is checkable.

### The pipeline: who decides what, and when a refusal is final

Round 4's first finding. `plan_query_refined_with_schema_graph`
(`sparql_plan.rs`) runs, in order: parse, `canonicalize`,
`obligations_of`, **the scoper**, `naive_plan`, `refine`,
`lower_refined`, `answers_alone`. The scoper refuses an untyped star on
the spot (`ScopeError::Unscoped` → 422 `query_unscoped`), before a
naive plan exists. Under the scope-local contract that refuses `{
SELECT ?s WHERE { ?s a :Signal } LIMIT 3 } ?s :hasName ?n` for its
untyped outer star — the read op 3a exists to type — before 3a has run.
Saying "the scoper will use the same transfer proof" named no producer
of that proof and no way for it to reach the scoper; and giving the
scoper a copy of 3a is two derivations of one fact, which is what 28d
removed a planner to stop.

**The contract: the refined plan is the one derivation of a star's
class across a boundary, and the scoper never crosses one.** A star its
own domain types is typed by the scoper as today; a star its domain
leaves untyped is *recorded*, and typed or refused only after the plan
has been refined. The phases, revised:

1. **Parse, `canonicalize`, `obligations_of`.** Unchanged.
2. **`scope_parsed` → `Scoping`.** The refusals for what the fetch
   cannot represent stay immediate and final — `UnsupportedConstruct`
   for `MINUS`, property paths, `LATERAL`, an `EXISTS` in an `ORDER
   BY` — they are the user's, as today. Star construction is keyed by
   `(naming domain, variable)`; every star carries `class: Typed(iri) |
   Untyped`. **An `Untyped` star is recorded, not refused.** The
   fetch-dependent results (`sql_limit`, `inexact`, required and
   optional fields) are computed at step 5, once every star has a
   class, by the code that computes them today.
3. **`naive_plan`.** The builder constructs the barriers (the
   normalised scope graph, *The boundary is the complete sub-query*);
   `Scope::of` reads it. **Correspondence invariant:** the scoper's
   naming domains and the plan's `SubSelect` barriers are in bijection,
   each pair binding the same variable set, and each scoper star has
   exactly one plan producer chain in its domain. A test over the
   property grammar asserts it (the scoper reads the algebra and the
   plan is built from the same algebra, so a disagreement is a bug in
   one of the two walks, and the test says which).
4. **`refine`.** Every rule, 3a/3b included. Boundary restrictions are
   derived here and only here.
5. **`resolve`** (new, `sparql_plan.rs`): for each star `(D, ?v)`,
   the refined plan is asked one question — *is the producer of `?v`
   at `D`'s root a `Scan` of some class `c`, in `D`?* The `Scan` is
   typed **when the domain's own scan is**, by either of two routes
   that `resolve` does not distinguish: the domain's own type match
   (`FoldMatchesIntoScan` folded `?v a c` into `scan c` — #466's `?a a
   :CEA` inside its sub-select; the scoper recorded `Typed(c)` for
   that star already, and `resolve` asserts the two agree — *the
   scoper and the plan agree*, an invariant, so the record is a check
   and the plan is the one source), or 3a/3b (3a placed the
   restriction, 3b carried it to the scan without meeting a stop, and
   the fold read it — the route an `Untyped` star has). If so the
   star is `Typed(c)` **from the plan**. If not — 3b stopped above a
   `Slice`, or nothing ever restricted the star — the star is refused:
   `ScopeError::Unscoped`, the same message as today, naming `?v` and
   its domain. **This is where an unscoped refusal is final, and the
   only place.** Then the fetch-dependent results are computed over the
   fully typed stars. In MR1, with no 3a/3b, `resolve` finds no
   plan-derived class for any untyped star and refuses exactly the
   stars round 3's contract refuses — so MR1 is this pipeline with
   step 5 typing nothing, not a different pipeline.
6. **`lower_refined`**, then `keep_what_the_rules_proved` on a refusal,
   merging producer-keyed narrowings into the *resolved* scoping.
7. **`answers_alone`** → the outcome.

**Why step 5's typing is sound for the fetch.** The class reached the
scan by a 3a/3b derivation whose invariant (*a boundary restriction's
path is a chain of 3b arms*) holds in the refined plan, so `Restrict`
at that scan preserves the enclosing join's answers. The engine leg
re-runs the original query over the fetched records, and 28h's
condition for a correct fallback is `eval(Q, fetched) = eval(Q,
complete)`; a record of `(D, ?v)` outside `c` contributes to no
solution of the enclosing join (that is what the chain proved), so
leaving it out of the fetch satisfies D1. That is question 10's
widening — a restriction crossing a barrier into the fallback fetch —
done once, in the plan, and *read* by the scoper. It ships with the
3a/3b MR, since without 3a/3b there is nothing to read.

**Naming domain versus evaluation unit.** Two things were called
"scope", and AC2 asks for the correspondence per operator rather than
the shared word. An *evaluation unit* (`Scope`) is any subtree SPARQL
evaluates to a multiset before combining it with the outside: a
sub-select, an `OPTIONAL` body, a `UNION` arm, a `NOT EXISTS`/`MINUS`
body. A *naming domain* is where a variable name denotes one variable.
SPARQL solution mappings are per query, and only a sub-select's
projection makes an inner variable private (§18.2.4, the projected
variables are the sub-query's interface): the `?a` inside an `OPTIONAL`
body **is** the outer `?a` — compatible mappings merge on it — and so is
the `?a` of a `UNION` arm. So stars are keyed by the innermost
enclosing **sub-select** (`Project` off the spine), not by every unit.
Whether a class *travels* between two mentions of one variable across
a combining node is that node's proof, the same in the plan (3a's
match) and in the scoper (its merging):

| combining node | class travels | proof |
|---|---|---|
| `Join`, one side → the other | yes, both ways | semi-join reduction on a join side — op 3a |
| `LeftJoin`, preserved side → body | yes | semi-join on the right side — op 3a |
| `LeftJoin`, body → preserved side | **no** | restricting the preserved side deletes rows it must keep |
| `Union`, arm ↔ arm | no | each arm is its own fetch and the fetch is their union — `union_branches` in the scoper today, for exactly this reason |
| `Minus`/`AntiJoin`, right ↔ left | no | a `Testing` unit; shrinking the right side widens the answer |
| `SubSelect`, exported `?v` ↔ outer `?v` | only through the export's producer | 3a at the barrier, 3b through it (a `Transfer`), read back by `resolve` |
| `SubSelect`, private `?v` ↔ outer namesake | never | two variables |

The scoper's `depth` tag and `type_depth` are its spelling of the
`LeftJoin` rows today: a triple at depth > 0 narrows nothing, and a
star whose only type is inside an `OPTIONAL` is fetched as optional.
Test 8 gains the third row's case (`?a :p ?x . OPTIONAL { ?a a :C }`,
untyped outside, typed inside) against the full-fixture oracle, so
what today's scoper does with it is measured rather than asserted here.

**Outcomes.** One enum the endpoint switches on, so a capability row
can say which and a test can assert it — AC8's three, plus the runtime
gate that already exists:

* **`Statement`** — SQL answers alone (`Refinement::UsedAlone`; today's
  `answers_alone` succeeds).
* **`Fetch { why }`** — the statement narrows, the engine finishes
  (`Refinement::Used(Some(why))`); `why` names the node that stopped
  lowering. Correct by D1–D3: every narrowing in the fetch has its
  justification in the ledger — the scan it was proved on, keyed by
  producer, or step 5's resolution.
* **`Rejected(ScopeError)`** — no plan: `unsupported_construct`,
  `query_unscoped` (decided at step 5), `incomplete_plan`. 422, the
  endpoint's existing vocabulary (`views.py::plan_or_error`).
* **`FetchDeclined`** — a fetch too large to materialise
  (`planning.fallback_can_answer`, the runtime 422 #466 hits today as
  `aggregate_not_pushable`). Unchanged; named so a row can say "this is
  a fetch, and on a big class the gate declines it".

A `Fetch` is never a wrong answer by construction, and a shape for
which no correct fetch exists is a `Rejected`, never a `Fetch` — the
top-N counter-example's row in the table now says so.

**Tests (test 10, full entry point).** Through `plan_query_refined`,
not by invoking a rule: the untyped outer read — MR1 →
`Rejected(query_unscoped)` naming the outer domain's `?s`; 3a/3b MR →
`Statement`; #464's nested `OPTIONAL` — MR1 → `Fetch`, after items 2–4
→ `Statement`; the top-N counter-example → `Rejected(query_unscoped)`
in every MR; the plain projection → `Rejected`; each alpha-renamed
variant → the identical outcome.

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
/// cannot see into. `descriptor` is the term the column *is*: an identity's
/// IRI, a slot's own, and — revision 9 — a measure's from its aggregate
/// (`COUNT` an integer, `SUM` the argument's datatype, `AVG` the
/// division's, `MIN`/`MAX` the argument's descriptor); `None` for a
/// structure.
pub struct RelationColumn { pub var: String, pub kind: ColumnKind, pub descriptor: Option<TermDescriptor>, pub guaranteed: bool }
pub enum ColumnKind {
    Identity { class_uri: String },
    Slot(BindingSpec),
    Measure { descriptor: TermDescriptor },
    /// An inlined element, identified by its occurrence — see "What may
    /// cross a relation". Representable, never serialisable (`descriptor`
    /// is `None`).
    Structure { holder: ColumnRef, path: Vec<String>, class_uri: String },
}

/// `Op::Join` gains a key beside the reference edge it has today.
pub enum JoinKey {
    /// `holder.object_data->>'slot' = referenced.asset360_uri`: today's edge.
    Reference(JoinEdge),
    /// `left.<col> = right.<col>`: two identities of the same class, or a
    /// relation column against a star's identity.
    Identity { left: ColumnRef, right: ColumnRef },
    /// Two `Structure` columns of one `(holder class, path)`: equal iff
    /// the whole occurrence identifier — holder identity and every
    /// collection hop's position — agrees.
    Element { left: ColumnRef, right: ColumnRef },
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

### What may cross a relation: the column kinds, structures included

Round 4's second finding. Op 1 exports every body variable, `?cs`
included; op 4 required every export to be *renderable*; question 5
said `SELECT *` therefore fails op 4 while `SELECT ?name ?kpFromMeter
…` lowers — although the user's `SELECT` list does not change S1's
interface — and the #464 walk-through pushed `SELECT *` anyway.
`ColumnKind` had no `Structure`. Contradictory, and AC5 asks for the
general contract, not a #464 exception. Six parts.

**1. Representable and serialisable are two properties.**
*Representable*: the value can be a column of a derived table, with an
equality the outside may use. *Serialisable*: the final projection can
emit it as an RDF term. Every `ColumnKind` is representable;
`Structure` is not serialisable. Today `key_is_readable` (and
`column_is_readable`, which delegates to it) asks `resolve_column` —
the *term shape* — of a grouping key, which is the two conflated: an
element could never be a column of anything, whether or not anyone
wanted to print it. The split: `representable(n, ?v)` is
`term_of(n, ?v) ≠ Ambiguous`; `serialisable(n, ?v)` is `representable`
and the kind is not `Structure`. Op 4 and op 5 and `PushGrouping`'s key
test ask the first; only the root `PushProjection` asks the second.

**2. `ColumnKind::Structure` is an occurrence, not a value.** An
inlined element is identified by **`Occurrence = (holder identity,
hops)`**, where `hops` has one entry per collection step on the path
from the record root to the element, each entry the slot and the
element's position in that collection — a list's ordinal, a mapping's
key. `parts[0].children[1]` and `parts[1].children[1]` are two
occurrences of one `(holder, path parts/children)`. Round 5 found that
revision 5's `(holder identity, path, ordinal)` kept the last hop only
and collapsed them.

Why occurrence, and why the whole path: the engine leg materialises
records with `TurtleOptions { skolem: false }` (`sparql_executor.rs`),
and the turtle writer (`rust-linkml-core`, `turtle.rs`) mints one fresh
blank node per inlined structure it *walks into* — `State::next_subject`,
a counter, called for each element of a list or a mapping and again for
each nested element inside it. A blank node's identity within a graph
is that walk: two identical elements in one array are two blank nodes,
two triples, two solutions, and so are two elements at the same index
of two sibling arrays. With `skolem: true` the same writer spells the
walk as an IRI — `identifier_node`: `<parent>/<slot>/<member>`, the
member being the list index or mapping key (or the element's own key
slot, when its class has one) — so the occurrence *is* the skolem path
with the index in place of a key. The statement uses the index because
the engine leg runs without skolemisation, where two elements sharing
a key value are still two nodes.

**Representation in SQL: one text column.** The renderer already emits
**one lateral per collection hop** (`_binding_expr` in `sql_builder.py`:
`u0`, `u1`, …, each a `jsonb_array_elements` or a `jsonb_each`), so
every hop's position is in the same `FROM`. Each list lateral gains
`WITH ORDINALITY`; a mapping lateral has `e.key` already. A `Structure`
column is the holder's `asset360_uri` concatenated with the hops as a
JSON pointer — `t1.asset360_uri || '#/hasCoveredSection/' ||
(u0.ordinal - 1)`, and for a nested element `… || '/children/' ||
(u1.ordinal - 1)` — **one scalar value**, so a `Relation` exports it, a
join compares it, a `GROUP BY` groups by it and a nested `Unnest`
carries it exactly as any other column, with no vector to transport
across any boundary. Equality of two `Structure` columns is text
equality of the whole identifier on one `(holder class, path)`, which
is blank-node equality in the graph the engine leg answers over, stated
in the statement. **A nested `Unnest` composes the identifier**: the
`Unnest` node's contract is `occurrence(element) = occurrence(what it
unnests from — the holder, or the enclosing element) ++ (slot,
position)`, one hop per `Unnest`, so an element's identity is a
function of the chain of unnests below it and no `Unnest` knows its
depth. `term_of` answers `Structure { class_at_path, path }` for an
unnested element and for a relation column carrying one; `guaranteed`
treats it as any slot read (a required unnest binds it in every row).

**3. Its consumers**, each a place a `Structure` column may legally be
read, and the equality each rests on:

* a slot read *through* it — `e.value->>'hasEntryPointM'`
  (`BindingSpec.containers` marks the `List` step; the `Unnest` below
  binds it; `SlotReading::BoundElement`). This is how `?kpFromMeter`,
  `?track` and the rest are columns of `q0` in #464;
* op 5's join key, when **both** sides are `Structure` of one `(holder
  class, path)` — `JoinKey::Element`, equality on the whole occurrence
  identifier. SPARQL joins on the blank node, which is the same
  occurrence, so the two agree.
  Admitted, with its own test; it is what an element used in an
  enclosing join needs;
* a `GROUP BY ?cs` key — by the identifier. `COUNT(?cs)` counts
  occurrences, `COUNT(DISTINCT ?cs)` distinct identifiers;
* `BOUND(?cs)`, and a `LeftJoin` whose right side exports it —
  presence is a `NULL` test on the holder column, as for any relation
  column.

Not a consumer: the root `Project` (no term to emit), a value
comparison (a blank node compares equal to nothing but itself, and the
comparison rule has no arm for it — refused, as today), `ORDER BY ?cs`
(blank-node order is undefined in SPARQL — refused, as today).

**4. Demand: an export nothing observes leaves the interface.** A new
equivalence, `PruneUnusedExports`. **Match:** a `SubSelect { vars }`
and a `?v ∈ vars` such that `?v`'s producer is **not in
`demand_above(barrier)`** — the sixth derived property (*What a
relational subtree derives*). Round 5 showed that is the fact this
rule needs and revision 5's "no reference above resolves to the
producer" is not: `COUNT(DISTINCT *)`, `DISTINCT` and `REDUCED` observe
every column of the mapping and name none, and a `Join`, `LeftJoin` or
`MINUS` on a shared variable observes it with no reference anywhere.
`demand` enumerates the operators that observe the whole mapping and
answers "every output" for one it does not know, so a future operator
is retained until someone writes its arm. Scope closure still ties a
named reference to a producer slot, so "the producer" is a fact of the
closure analysis, not a name search. **Edit:** `vars := vars \ {?v}`.
**Equivalence:** two halves. SPARQL `Project` is a bag projection
(§18.5, `Project` keeps multiplicity), so removing a column changes no
row count and no other column's value; *and* by `demand`'s contract no
consumer's answer depends on that column's presence or value. The
second half is what round 5's `SELECT (COUNT(DISTINCT *) AS ?n) WHERE
{ { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1 20) } } } }`
needs: the `Group` demands every output of its input, `?y` stays, and
the answer stays `2`. **Caught by:** a **transition check in the
driver**, *no demanded export is dropped* — not a state invariant.
Revision 6 wrote one (*demand is exported*: every producer in
`demand_above(barrier)` is in `vars`) and round 6 showed it cannot
work: `demand` is recomputed from the current plan, and the operators
that observe the whole mapping demand *every output of the input* —
so after a bad prune of `?y` under `COUNT(DISTINCT *)` the `Group`
demands `{?x}`, the barrier exports `{?x}`, and the invariant holds of
the wrong plan. The same under `Distinct`, and for a `Minus` whose only
shared variable was pruned: the dependency leaves with the export, and
nothing dangles for closure to see. A property that shrinks with the
interface it guards cannot check that interface after the fact. So
`refine` checks the *edit*: before `rule.apply`, for every barrier *b*
it records `kept(b) = demand_above(b) ∩ exports(b)` on the unmodified
plan — the variable names, each with its producer's `NodeKey` for the
printout — keyed by *b*'s `NodeKey`; after `apply`, the node *b*
resolves to (itself, or its successor through `retired`) must have
every name in `kept(b)` among its `outputs`, and a retirement with no
successor fails (*Evidence that survives rewriting* → *One check
relates two plans*). It fails at the rule in every build, naming the
rule and the variable, and it runs for whatever `apply` did, matcher or
no matcher — which is what test 3's three bad prunes need, and why they
are applied through `refine` and not by calling the rule. Two
consequences are stated rather than left to be found: a rule may not
remove an observer and prune what it observed in one edit — that is
two rewrites, each checked, and a rule that wants both writes two — and
the before/after oracle of test 2(d) stays the semantic backstop; this
is a structural check on one class of edit. The prune runs before op 4,
so op 4's "every export representable" is asked of the exports the
query *demands*. This is demand-driven pruning; it is not "change the
user's `SELECT` list", which changes the query.

**Pinned results** (test 12), each through the full entry point:

* `SELECT ?name ?kpFromMeter ?track ?trackName ?trackCode ?trackDiscr`
  over #464: `?cs` is referenced by nothing above S1 → pruned → every
  remaining export representable and serialisable → op 4, op 5 → the
  root `PushProjection` → **`Statement`**.
* `SELECT *` over #464: the root `Project` references `?cs` → not
  pruned → op 4 admits it (representable) → the root `PushProjection`
  declines (`?cs` not serialisable) → **`Fetch`** (`why`: the root
  projection of a `Structure`), and the engine emits the blank node.
  An explicitly unsupported *result form*: a statement cannot answer a
  query that asks for a blank node. The widening is a term for
  elements — the turtle writer has `skolem: true`, which mints
  `<parent>/<part>` IRIs, and a statement can spell that from the pair
  — but that changes what the endpoint answers for every element and
  is the datamodel's decision, not the planner's (question 5, closed
  that way: not blocking, the outcome is pinned).
* an element in an enclosing join — `?a :hasCoveredSection ?cs . {
  SELECT ?cs (COUNT(?x) AS ?n) WHERE { ?a a :CEA ; :hasCoveredSection
  ?cs . ?cs :x ?x } GROUP BY ?cs }` with `?cs` not projected at the
  root: `Structure` on both sides of the join, one `(CEA,
  hasCoveredSection)` → `JoinKey::Element` → **`Statement`** (MR4,
  with the element-held edge); projected at the root → **`Fetch`**.
* round 5's whole-mapping observer: `SELECT (COUNT(DISTINCT *) AS ?n)
  WHERE { { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1 20) } } } }`
  → `demand` at the `Group` is every output → `?y` is not pruned → the
  refined plan answers `2` on the oracle; the outcome is **`Fetch`**
  (`PushGrouping` declines `COUNT(DISTINCT *)`, as today). Beside it:
  `SELECT DISTINCT ?x` and `SELECT REDUCED ?x` over the same sub-select
  (`?y` *is* pruned — the `Project` under the `Distinct` does not pass
  it — and the answers are unchanged), and `{ SELECT ?x ?y … } MINUS {
  ?z :p ?y }` (`?y` demanded by the `Minus`, not pruned).
* the nested occurrence: a holder whose `parts[0].children[1]` and
  `parts[1].children[1]` are identical structures. `SELECT (COUNT(?c)
  AS ?n) WHERE { ?h a :H ; :parts ?p . ?p :children ?c }` answers `2`
  (two hops, two identifiers), and `?c` joined outside its sub-select
  on `JoinKey::Element` joins each child to itself only — one row per
  occurrence, not two. Revision 5's key would have answered `1` and
  joined each to both.

**5. Found while writing this: the unnest dedup is wrong for
structures.** `_from_join_where` (`sql_builder.py`) renders every
unnest as `SELECT DISTINCT e.value` — "one solution per *distinct*
element", which is right for a scalar or IRI slot (an RDF graph is a
set of triples: `["Freight", "Freight"]` is one triple) and **wrong for
an inlined structure**: two identical elements are two blank nodes
(point 2), two triples, two solutions, and the engine leg counts two
where the statement counts one. It affects today's flat `GROUP BY ?a
COUNT(?cs)`, the form #466's statement reuses. The `Unnest` contract
states the rule per step kind: **dedup by value for a scalar step,
by occurrence (no dedup, `WITH ORDINALITY`) for a structure step.**
`plan_to_algebra`'s direct `Unnest` test (test 2) seeds a duplicated
structure beside the duplicated scalar and asserts both multiplicities
against the oracle. It goes in MR1: it is the `Unnest` node's contract,
the same file, and #466 counts elements.

**6. Question 6, closed.** `ScanSlot.path` stays rooted at the star
(`[hasCoveredSection, isReference]`); "relative to the element" is the
renderer's reading, which `BindingSpec.containers` already spells per
step and `SlotReading::BoundElement` already names. What the plan must
guarantee is that the element exists as a row where such a read
happens: invariant **every `BoundElement` read has its unnest** — a
read of path `p` with reading `BoundElement` has, below it in the same
evaluation unit, an `Unnest` of `p`'s collection prefix (a sibling of
`fanout_restored`, which walks the same pair the other way). A read
with no such unnest is `AnyElement` or ill-formed.

### Op 1 — `EncloseOptionalBody` *(new rule; an identity projection)*

> **As built (revision 9).** The builder's `enclose` wraps the body in a
> `SubSelect` and leaves *no* `Project` under it; the identity projection
> the worked cases write as `n9` is supplied by the *lowering*, which gives
> a body with neither a grouping nor a projection the answering
> projection's column list (`scope_columns`: exports, fan-outs,
> identities). A body's `unnest` is thereby a binding the renderer can
> discharge, which the bare row set was not.

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

**Match.** A subtree *S* that is a side of a `Join`, or the **right**
side of a `LeftJoin`, with `?v ∈ on`; `?v ∈ guaranteed(S.root)` and
`?v ∈ guaranteed(other side)`; `term_of(other side, ?v) =
Identity(class)`; **that `Join`/`LeftJoin` is *S*'s only consumer**;
and a *predicate* of `?v` the other side proves and *S* does not yet
carry. Two predicates, one rule (revision 10):

* **the class**, `?v ∈ Identity(class)`, when
  `restriction_is_implied(S, ?v, class)` is false — no `o_boundary`
  with this canonical key in the ledger, and *S*'s root does not
  already produce `?v` as `Identity(class)`, guaranteed (see *Evidence
  that survives rewriting*);
* **a row test**, once *S* produces `?v` as `Identity(class)` with a
  scan of it visible at the insertion point: each condition *c* in
  `conditions_on(other side, ?v)` — the record predicates of the
  filters on every path from a scan of `?v` up to the other side's
  root, through the operators that pass every input row on (the table
  below) — that *S* does not already apply (`conditions_on(S, ?v)`)
  and the ledger does not hold. A **record predicate** reads nothing
  but `?v`'s own record: `Column` readings of `?v`'s slots (a
  single-valued path from the record root), literals, comparisons,
  `IN`, `AND`/`OR`/`NOT`. Not a variable (not a column), not a
  function (its arguments' presence is not audited), not a
  `BoundElement` (a row of the fan-out, not the record), not a
  containment test (`AnyElement`, which the oracle cannot yet spell
  back), not a pattern, and not a class restriction (its own arm).

  | operator on the way from the scan of `?v` to the other side's root | every row above satisfies the filters below it? |
  |---|---|
  | `Filter`, `Sort`, `Distinct`, `Reduced`, `Slice`, `Unnest` | yes: a subset, an order, a dedup, a fan-out |
  | `Bind` not binding `?v` | yes |
  | `Project`, `SubSelect` keeping `?v` | yes |
  | `Group` with `?v` among its keys | yes: every group is rows of one `?v` |
  | `Join` | yes, from each side that guarantees `?v` |
  | `LeftJoin`, `Minus`, `AntiJoin` | from the left side only |
  | `Union`, a `Bind` of `?v`, a keyless `Group`, anything else | **stop**: the rows above are not all rows below |

  A row test inside the other side's own `OPTIONAL` or in one arm of
  its `UNION` therefore never crosses (it decides a binding, not which
  `?v` reach the join), and the same table read from *S*'s side is
  what says a test is already applied. *S* is usually an `Exporting` scope — the sub-query beside
a typed outer read — but the proof is about a join side, not a scope
root, so it is also the **other** direction: the outer `?s :hasName
?n` beside `{ SELECT ?s WHERE { ?s a :Signal } … LIMIT 3 }` is a join
side whose partner's `?s` is `Identity(Signal)` (a relation column,
guaranteed), and the same rule types it. Revisions 1–3 wrote the match
for a scope only, which would have left that outer read untyped in its
own scope, i.e. refused — a gap round 3's typing remark exposed. In
the enclosing scope no transfer is involved: the obligation is raised
and discharged in the scope *S* is in.

The last clause is the ownership rule the equivalence needs and revision
2 left implicit. What 3a proves is `Join(L, R) ≡ Join(L, Restrict(R))`;
it does not prove `R ≡ Restrict(R)`. If *R* had a second consumer — a
DAG spelling of `Union(Join(C, R), R)` — rewriting *R* in place would
change that consumer's answer. Two contracts would do; this document
takes the first for the barrier and, since round 3, **for every node the
restriction touches**: a plan is a tree as the builder writes it, but
`fold` already makes two nodes read one scan (`consumers`' own doc
comment says so), so one consumer of the barrier is not ownership of
the body — two barriers with one consumer each can share a scan, and
pushing a restriction into it rewrites both. The rule is therefore:
**a contextual rewrite may edit a node only if every node on the path
from the justifying edge to that node, the node included, has exactly
one consumer** (`consumers(plan, id).len() == 1`, the function in
`sparql_rules.rs`); 3a checks it for the barrier, 3b checks it at each
step and declines the step at the first shared node, leaving the
restriction above it (still correct). The invariant *a boundary
restriction's path is a chain of 3b arms* re-checks it for every node
on the recorded path. The plan root is exempt — it has no consumer —
and is never the root of an `Exporting` scope anyway. (The second
contract — clone the subtree below the first shared node and redirect
the one edge, what `PushComparisonFilter`'s "privately consumed" test
approximates today — is the widening if a shared node ever proves to be
where the restriction is worth having.)

**Edit.** When `S.root` is a barrier, the restriction goes **inside
the boundary node**, not above it: `S.root` is a `SubSelect { input }`
(every `Exporting` scope root is one after op 1 and by construction for
a user-written sub-query — the construction that wraps the *complete*
sub-query, modifiers included), and the edit is `input := Filter(input,
?v ∈ Identity(class))`; when *S* is a plain subtree of the enclosing
scope, `S := Filter(S, ?v ∈ Identity(class))` in that scope. The
scope root is still the `SubSelect`, so op 4's match is unaffected and
a scope boundary stays identifiable by its node kind rather than by
whatever operator happens to be on top; and because the barrier wraps
the modifiers, the filter lands *above* the sub-query's `Slice` and
`Distinct`, where the semi-join proof is made, and 3b meets the `Slice`
going down. A row test is inserted `Sql` where the frontier already is
(the barrier is pushed, so the filter must render there and does, over
the same class) and the engine's elsewhere; 3b flips it where it comes
to rest. Revision 2 wrote `S.root := Filter(S.root, …)`, which would
have put a filter above the barrier and broken op 4's match; revision 3
wrote this edit against today's tree, where the barrier is the bare
projection and the same edit lands *below* the limit — round 3's
finding, fixed in the representation rather than in the rule. The filter kind renders as `<col> IN
(SELECT asset360_uri FROM golden_records WHERE asset_type = 'class')` —
or, when 3b carries it to a scan, as the scan's own `asset_type`
condition — and raises a new obligation, `o_boundary(S, ?v, class,
at: JoinOccurrence { node: NodeKey, side })`, in *S* (a key, not an
index — *Evidence that survives rewriting*). The obligation is the
provenance the review asked for in place of `narrowed_by_interface:
true`: it names the join occurrence and side that justified it, so it
stays checkable when that occurrence is itself rewritten (op 5 flips
the node to `[S]` and records a key; the occurrence is the same node),
and whoever discharges it names the rule and the boundary.

**Equivalence.** A semi-join reduction on the completed relation: for a
join on `?v`, rows of *S* whose `?v` no row of the other side carries
contribute nothing to `Join`, and nothing to `LeftJoin` when *S* is the
**right** side — a left row with no partner is kept unchanged either way,
so restricting the *right* relation cannot delete or alter a left row,
while restricting the *left* relation would delete preserved rows. Every
`?v` the other side carries is an IRI of `class` (`term_of`), bound in
every row (`guaranteed`); records of one URI in two classes do not exist
(the identifier column is the table's key). And every `?v` the other
side carries satisfies each row test on the way to its root (the table
above: every operator passed keeps only rows that passed the filter);
a record predicate has one value per record, read off the same JSON
row by either scan, so it says the same of `?v` on *S*'s side. So the
filter — the class, or the test — removes only rows that joined
nothing. This is `ValuesNarrowTheJoinedScan`'s argument
with a scan in place of the `VALUES` block, and it is made at the root of
*S*, where the relation is complete.

**What it declines.** The left side of a `LeftJoin`; `Minus` and
`AntiJoin` (a `MINUS` keeps rows with *no* partner, so shrinking the right
side widens the answer); a `Union` arm as a side (the arm is not the
relation the join sees); a `?v` either side binds optionally; a row test
the other side applies conditionally, on another star, on an element,
or through a function; and a row test for a side whose `?v` is a
relation column of a nested barrier with no scan of its own visible
(the nested case of the table below) — the class still crosses there,
the test does not, and the nested barriers' own joins carry nothing
across because neither side filters.

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
| `Join` | yes, to **each** side that has `?v ∈ guaranteed(side)`; a side where `?v` is not guaranteed keeps rows whose `?v` is unbound, which the filter must not test. A row test goes to the side whose scan of `?v` is visible — on a side that binds `?v` as a slot (the reference edge's other end) it would never render — and stops above the join when neither has one |
| `LeftJoin` | to the **left** side only, and only if `?v ∈ guaranteed(left)`; never to the right (that deletes bindings, not rows) |
| `Union` | to both arms, each on its own guarantee |
| `Minus`, `AntiJoin` | to the left side only |
| `Group` **with `?v` among its keys** | yes: rows are partitioned by `?v`, so a test on `?v` removes whole groups and changes no surviving group's aggregate |
| `Group` **without `?v` as a key** (a keyless aggregate, or `?v` only inside a measure) | **stop.** The aggregate's value depends on the rows removed |
| `Slice` | **stop.** Which rows the offset/limit keep depends on the rows removed — the counter-example above |
| `Path`, `Service`, `Opaque` | stop |

**Where a row test comes to rest it becomes SQL** (revision 10). A class
restriction is read by the fold and never rendered. A row test is a
filter like any other once it can move no further, and 3b — not
`PushComparisonFilter`, which leaves every restriction filter alone —
flips it to `Sql` when the node below runs in SQL and the condition
renders over the scans visible there. One rule owns the filter's
position: taken by the comparison rule it would land wherever the
frontier happened to be when that rule ran, above a keyed `Group` the
walk would have passed, and the plan would be the schedule's. The same
holds on the way down: a filter moved below an `Sql` node is `Sql` on
its new input (it renders there or the step declines, which for a class
restriction — that nothing renders — means it stays above the frontier),
and below an engine node it stays the engine's.

The walk records each step as the rule that took it, **on the obligation**:
`o_boundary` carries a `path: Vec<Step>`, one `Step { rule, node:
NodeKey }` per operator it passed — a log the invariant checks against
the chain it recomputes from the plan, not the proof itself (below). Where the walk stops, the filter stays: still correct
(the boundary proof holds at any point above the stop, because every step
preserved the multiset), just not folded into a scan.

**The transfer step.** The `Project`/`SubSelect` row is the one that
crosses a scope: pushing through a `SubSelect` exporting `?v` moves the
filter from scope *S* into the nested scope *T* whose barrier that
`SubSelect` is. Revision 2 let 3b do it and let *obligations stay in
their scope* forbid it, which the review caught. The reconciliation is
not to re-home the obligation (that loses the provenance the invariant
exists to keep) but to make the crossing explicit: the step is a
`Step::Transfer { via: NodeId, from: OutputSlot, to: OutputSlot }` — the
export's producer slot on *T*'s side, resolved through
`Export.producer`, so `?v` in *S* becomes the slot that produces it in
*T* — and the obligation keeps its origin (*S*, the join occurrence, the
class). *Obligations stay in their scope* reads the path **with a
cursor**: it starts in the origin scope *S*; every ordinary step must
name a node of the scope the cursor is in; a `Transfer` must name a
barrier of that scope and moves the cursor into the scope it wraps; the
discharge must be in the scope the cursor ends in. Revision 3 wrote
"legal iff its path holds a `Transfer` into *T* and every step before it
is in *S*", which reads S → T and rejects S → T → U; the cursor makes
depth irrelevant, and the two-barriers-down test (test 2's positive
transfer case) is the case that exercises it. That is the "explicit
obligation-transfer operation" the review asked for, and it is also the
composition the nested cases in the table need.

**A step that branches splits the obligation.** `path: Vec<Step>` is
one route, and two of the 3b arms lead to more than one input. For
`Union` the filter *must* reach both arms (dropping it from one arm
changes the answer), so the step is `Step::Split { at: NodeId }`: the
parent obligation is discharged at the `Union` by rule 3b, and one
**derived obligation** per arm is raised, each a copy of the parent
(origin, join occurrence, class) with the parent's path plus its own
branch, so `ledger_balances` sees one discharge and *k* new obligations
and each child is checked on its own path from then on. For `Join` the
filter *may* go to either side that guarantees `?v` and need not go to
both — `Join(Filter(L), R) ≡ Filter(Join(L, R))` when `?v ∈
guaranteed(L)` — so the 3a/3b MR pushes to **one** side (the first that
guarantees `?v`, in input order; a deterministic choice the fixpoint
test can check) and does not split. Pushing to both sides is a later
widening using the same `Split` step, if a measured plan ever wants the
restriction on two scans. A single historical path is a proof for one
descendant; the split is what makes it a proof for each.

In the counter-example the filter stops above `Slice`, and what happens
next is a **refusal, not a lowering** — see the table row below and the
scoper contract it uncovered.

**Would a bad application be caught?** A new invariant, **a boundary
restriction's path is a chain of 3b arms**: for each `o_boundary`, walk
its `path` from the origin scope root to the discharge with the cursor
above and require every step to be a "yes" arm of the table *for the
node it names* — no `Slice`, no keyless `Group`, no keyed `Group` whose
keys omit `?v`, no `LeftJoin` right side, no `Minus`/`AntiJoin` right
side, no `Join` side where `?v` is not guaranteed, no
`Path`/`Service`/`Opaque`, every `Transfer` via a `SubSelect` of the
cursor's scope that exports the slot it names, every `Split` at a
`Union` with one child obligation per arm, and **every node on the path
with exactly one consumer** (the ownership rule of 3a, re-checked where
the restriction actually went). Revision 2 checked two of those stops;
the table has nine, and the invariant reads the table rather than
restating part of it. The obligation is what makes the check local:
without it, the invariant could not tell a user-written filter (which
may sit anywhere the user put it) from a derived one.

#### Evidence that survives rewriting, and the progress policy

Round 4's third finding. `OutputSlot`, `JoinOccurrence`, `Transfer`
and every `Step` above hold a `NodeId`, and a `NodeId` is an index
into `plan.nodes`: every removal renumbers — `remove_nodes`, `fold`,
and seven more sites in `sparql_rules.rs` each build a `remap` and
reassign `plan.nodes` — so a path validated when it was written names
the wrong nodes after `PushProjection` absorbs the `Project` one of
its steps passed. A check made once at creation is not the invariant
the section above promises. Three decisions close it, and a fourth
states progress.

**Identity is a key; position is an address.** `Node` gains `key:
NodeKey`, allocated from a counter on the `Plan` and never reused;
`NodeId` stays the index `PlanOp::inputs()` uses. Everything the ledger
records about a node — `OutputSlot.node`, `JoinOccurrence.node`,
`Transfer.via`, `Split.at`, a `Step.node` — is a `NodeKey`, and
`Plan::node(key) -> Option<NodeId>` resolves it. The nine renumbering
sites become one primitive, `Plan::rebuild(kept, remap)`, which
re-indexes inputs (what each of them does by hand today) and touches
no evidence, because evidence holds keys. A rule that removes a node
says what took over its work — `Plan::retire(key, successor:
Option<NodeKey>)`, the fact every existing `remap` already computes
(`remove_nodes` maps a removed node to its input, `fold` to the scan)
— and the plan keeps `retired: BTreeMap<NodeKey, Option<NodeKey>>`.
New invariant, **evidence resolves**: every key the ledger names is
live, or retired with a successor; a rule that retires a key an
obligation still names, with no successor, fails at the rule.

**The proof is recomputed; the stored path is a log.** *A boundary
restriction's path is a chain of 3b arms* reads the **plan**, not the
recorded `path`. For each `o_boundary` with origin `(join: NodeKey,
side, ?v, class)` and discharge site *d* — the node whose `discharges`
holds it, which the ledger already records — walk **up** from *d*
through consumers: the walk is a chain because the ownership rule
gives every node on it exactly one consumer, and a node with two is
the failure. Each node passed must be a "yes" arm of the 3b table
*for the node as it is now*; passing a barrier that exports `?v`'s
producer is a `Transfer`; passing a `Union` is a `Split`, and the
ledger must show the parent discharged there with one child per arm;
the walk must reach the origin side's root (the barrier of an
`Exporting` scope, or the side subtree's root), resolved through
`retired` when a later rule merged joins (the successor must be a
join with the same side, or the check fails). The recorded `path:
Vec<Step>` is written for the printout and `RefineLog` and, in a debug
build, asserted equal to the recomputed chain — which is how a rule
that records a step it did not take is caught. This is what makes it
an invariant after every pass: `push → fold → remove → reanalyse →
lower` is checked at each arrow against the current plan, by
`Plan::check`, as every other invariant is.

**Derived facts are recomputed, never cached across a rewrite.**
`outputs`, `guaranteed`, `term_of`, `correlated_inputs`, `Scope::of`,
`consumers` are functions of the plan, computed on demand by the rule
that asks — as `Visible::below` is today — and `Plan::check`
recomputes them. No rule stores a property on a node except what the
*lowering* renders (`RelationColumn.guaranteed` is written after
refinement has ended). There is therefore nothing to invalidate, and
"property updates follow mutations" is true by construction rather
than by discipline. The cost is linear per ask; the plans are small
and the grammar test measures it.

**One check relates two plans.** Every invariant in this document is a
closure property of one plan, checked by `Plan::check` after each
application and on the result. Round 6 found a lie no such check can
see: a demanded export pruned by a rule that skipped its own match,
where `demand` recomputed on the smaller plan agrees with the smaller
interface (*What may cross a relation*, part 4). The driver therefore
holds one fact across a single application and no further. `refine`
becomes: for each rule, `let kept = plan.kept_exports();` —
`BTreeMap<NodeKey, BTreeSet<(Variable, NodeKey)>>`, one entry per
barrier, `demand_above(b) ∩ exports(b)` on the plan as it is — then
`rule.apply(plan)`, then `plan.check_transition(&kept)?`, then the
existing `plan.check()`. `check_transition` resolves each key through
`retired` exactly as *a boundary restriction's path* resolves its nodes,
and fails when a kept name is not in the resolved node's `outputs` or
the key resolved to nothing. Three things it is not. It is not a cached
derived fact: `kept` is dead the moment the check has run and the next
application recomputes it, so "recomputed, never cached across a
rewrite" stands. (A rule that declines edits nothing, so the record
taken before it still describes the plan the next rule sees; the
driver re-takes it after every *edit* rather than before every *try*
— the same fact, derived once per plan state. Pepibru GitLab issue
#468 measured the per-try form at most of the planner's time.) It is not a `debug_assert!`: it is a `RuleFailure` in
every build, because the end-of-`refine` state check a release build
relies on cannot see a transition defect — there is no post-state that
witnesses it. And it is not the equivalence proof: test 2(d)'s
before/after oracle remains the semantic backstop; this catches one
class of structural lie at the rule instead of in a result. Cost: one
`demand_above` per barrier per application, over one `outputs` table
and one `demand_above` memo shared by the barriers of that one
derivation (a chain of `n` joins asked per barrier is `n` walks, asked
once is one), linear like everything else here, measured by the
grammar test and pinned by the wide-`OPTIONAL` timing test. The alternative round 6
offered — an immutable observer contract written on the barrier at
construction — is rejected: it would be the one stored property in a
design where every other is recomputed, and every rule that adds or
removes an observer would have to maintain it, which is the discipline
this section exists to avoid.

**"Already carries a restriction" is a key, not a position.** 3a's
last-but-one clause is decided by `restriction_is_implied(side, ?v,
class)`, which is true when either (a) the ledger holds an
`o_boundary` with the **canonical key** `(origin join key, side, ?v,
class)` — the ledger is append-only, an obligation is never removed,
only discharged, so the answer does not depend on where 3b has since
moved the filter; or (b) `term_of(side root, ?v) = Identity(class)`
and `?v ∈ guaranteed(side root)` — the restriction is implied by the
side's own scan (#466's case). Two obligations with one canonical key
is a ledger defect, checked.

**Progress.** Every rule has a measure that strictly decreases, or a
fact it makes true once and matches against:

| rule | why it fires finitely often |
|---|---|
| 3a | at most once per canonical key; keys are bounded by joins × variables × classes in the plan |
| 3b | each step moves one filter one node down — its depth strictly increases in a finite tree; a `Split` replaces one obligation by *k* whose remaining depth is each strictly smaller |
| op 1 | once per `LeftJoin`: the match excludes a right side that is already a `SubSelect` |
| op 2 | removes a `condition`; there are finitely many |
| ops 4, 5 | flip `[E]` → `[S]`, never back |
| `PruneUnusedExports` | strictly shrinks a `vars` list |
| the existing rules | keep their own: `fold` removes nodes, the push rules flip executors, the tail rules absorb a chain |

So every schedule terminates in bounded steps, and `MAX_ROUNDS` (64,
`sparql_rules.rs`) stays as the guard it is. **At the budget** the plan
is correct: every applied rewrite was an equivalence and `Plan::check`
held after each, so `refine` returns the plan as it is, lowering
proceeds, and the outcome is a `Statement` or a `Fetch` — never a
wrong answer, only a less refined one. Hitting it is a bug
(`debug_assert!` today), and the grammar test asserts a fixpoint for
every generated query. **Schedules:** two legal schedules may produce
two plans (3b's `Join` arm takes the first side in input order; an
earlier fold may change which stop is met first) and both must
preserve answers; no rule's precondition is stated in terms of a later
rule repairing its result, and the bad-rule tests check each rule
alone. Confluence is not claimed and identical SQL is not required.

**Tests (test 11, lifecycle).** (i) 3a, then 3b through a `Project`,
then `PushProjection` absorbs that `Project`: the step's key resolves
through `retired`, the recomputed chain holds, the plan lowers; (ii)
3a offered again after 3b moved the filter: declines on the canonical
key, no duplicate obligation; (iii) a `Split` at a `Union`, then a fold
in one arm that removes the node that arm's step named; (iv) a
deliberately bad rule retires a node an obligation names, with no
successor — *evidence resolves* fails at the rule; (v) `MAX_ROUNDS`
forced to 1 over the grammar: every plan passes `Plan::check`, lowers,
and agrees with the oracle.

**Why the body scans the class again rather than reading the outer row**
is unchanged from revision 1: inside the derived table, `?a`'s record is a
fresh `Scan` joined back to the outer `t0` on identity, a self-join
Postgres answers by index; a `LATERAL` rendering reading `t0` directly is
an equivalent statement and a renderer choice (open question 3).

### Op 4 — `PushBarrier` *(new rule; the projection rule, at a scope root)*

**Match.** An `Exporting` `SubSelect` that is `[E]`, whose `input` is
`[S]`, whose `correlated_inputs` is empty, and every export's producer
resolves through the body's `Visible` to a **representable** column
(`term_of` not `Ambiguous`; a `Structure` qualifies — *What may cross a
relation*, part 1; `key_is_readable` is that test once its term-shape
half moves to the root `PushProjection`). `PruneUnusedExports` has run
before, so the exports asked about are the ones the query demands. The barrier is the
scope root by construction (it wraps the complete sub-query), so there
is no "no modifier above it" clause to state: the sub-query's own
`Project`, `Sort`, `Distinct`, `Slice` are nodes of `input`, and `input
[S]` means the rules anchored at the barrier (`PushProjection`,
`PushGrouping`, the sort and slice rules) took every one of them, or the
barrier does not match. A `Testing` scope never matches: it is not a
relation the outside joins.

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

* `on = [?v]`, **both** sides `term_of(side, ?v) = Structure` of one
  `(holder class, path)`, `?v ∈ guaranteed(side)` on both:
  `JoinKey::Element`, equality on the whole occurrence identifier
  (holder identity and every hop) — the blank node's occurrence, which
  is what SPARQL's term equality on a blank node is within one graph
  (*What may cross a relation*, parts 2 and 3). MR4.

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
which is the SPARQL multiset. A grouped body has one row per **key
tuple**, so a grouped sub-select does not multiply the outer row **when
its whole grouping key is the join key** — `GROUP BY ?a` joined on `?a`,
which is #466: that is the "one count beside the row" it asks for,
obtained from the semantics rather than assumed. `GROUP BY ?a ?x` joined
on `?a` alone yields one row per `(?a, ?x)` and *does* multiply, exactly
as SPARQL does. "Has a `Group`" is not a uniqueness property; "the join
key is the complete key" is, and it is what a consumer that needs
at-most-one-row (none in this document) would have to check.

**Would a bad application be caught?** A new invariant, the sibling of
`reference_joins_agree`: **join keys agree** — a `JoinKey::Identity`'s two
columns are each the identity of a scan or a relation column of kind
`Identity`, on the side the key says, of one class, **each in
`guaranteed` of its side**, and `on` is that variable alone; a
`JoinKey::Element`'s two columns are each a `Structure` of one `(holder
class, path)`, guaranteed on both sides; a `JoinKey::Cross` has `on =
[]`.

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
n9  project [?a, ?trackQty]                             [E]   the sub-query's own projection
n10 subselect [?a, ?trackQty]                           [E]   ← scope S1 root: the barrier
n11 join    n3, n10 on ?a                               [E]
n12 project ?name ?trackQty                             [E]
```

(`n9` is the identity projection the wrapping construction leaves under
the barrier; today's builder emits `n10` alone, as the projection.)

After the existing rules — the fold now counting types per scope, so each
`?a` gets its scan; the constant-object filter; `PushGrouping` anchored
at the barrier, which is the only change it needs; and `PushProjection`
inside S1 taking `n9` as it takes the query's own:

```
n0  scan    :CEA as ?a, requires [identification]        [S]
n1  filter  ?a.hasCivilEngineeringAssetType = <Tunnel>   [S]
n2  scan    :CEA as ?a, requires [hasCoveredSection]     [S]   (scope S1)
n3  unnest  ?a.hasCoveredSection as ?cs                  [S]   (scope S1)
n4  group   keys=[?a] count(?cs) as ?trackQty            [S]   (scope S1)
n5  project [?a, ?trackQty]                              [S]   (scope S1)
n6  subselect [?a, ?trackQty]                            [E]   ← op 4 fires
n7  join    n1, n6  on ?a                                [E]   ← op 5 fires
n8  project ?name ?trackQty                              [E]   ← PushProjection
```

Op 4: `n5` is `[S]`, `?a` resolves to `Identity(CEA)` inside S1 (`term_of`)
and is guaranteed there (a scan identity, through a keyed `Group` whose
key it is, through a projection that keeps it), `?trackQty` to a `COUNT`
measure, guaranteed → `n6 [S]`.
Op 5: `n1`'s `term_of(?a) = Identity(CEA)`, guaranteed; `n6`'s column says
the same; one class → `n7 [S]`, key recorded. (Op 3a's class arm does
not fire: S1 already scans `?a` as CEA, so a restriction to CEA at its
root adds nothing — `restriction_is_implied`, arm (b). Its **row-test
arm** does, from revision 10: `n1`'s `hasCivilEngineeringAssetType =
<Tunnel>` is a record predicate on `?a` that every outer row satisfies,
so `?a.hasCivilEngineeringAssetType = <Tunnel>` is placed at S1's root,
3b passes it through the projection and the keyed `Group` to rest above
`n3`, and the derived table below reads 130 tunnels' sections, not
20 517 assets' — consolidator #470.) `PushProjection`: no `Group` **in the outer
scope** (the one in S1 is behind a pushed barrier), body `[S]`, both
projected variables readable (`?name` a column of `n0`, `?trackQty` a
relation column) → `n8 [S]`. Every node `[S]`, one island per scope,
`answers_alone` holds — the statement is the answer:

```sql
SELECT t0.object_data->>'identification' AS name, q0.track_qty
FROM golden_records t0
JOIN (
  SELECT t1.asset360_uri AS a, count(*) AS track_qty
  FROM golden_records t1
  CROSS JOIN LATERAL jsonb_array_elements(t1.object_data->'hasCoveredSection') WITH ORDINALITY AS e(value, ordinal)
  WHERE t1.asset_type = 'CivilEngineeringAsset'
    AND t1.object_data->>'hasCivilEngineeringAssetType' = '…Tunnel'
  GROUP BY t1.asset360_uri
) q0 ON q0.a = t0.asset360_uri
WHERE t0.asset_type = 'CivilEngineeringAsset'
  AND t0.object_data->>'hasCivilEngineeringAssetType' = '…Tunnel'
  AND t0.object_data ? 'identification'
```

(The inner statement is what `PushGrouping` + the renderer already emit for
the flat `GROUP BY ?a` form, with one correction: the unnest of an
inlined *structure* is `WITH ORDINALITY` and **not** `SELECT DISTINCT
e.value` — *What may cross a relation*, part 5 — because each element
is its own blank node in the graph the engine leg answers over.
`count(*)` over the fanned-out element is `COUNT(?cs)` by the existing
unbound rule, since a required element is never `NULL`.)

**Equivalence, the cases that matter:**

* **A tunnel with no covered section.** The inner `?a :hasCoveredSection ?cs`
  is mandatory, so the group has no row for it, and SPARQL's `Join` drops
  the tunnel: the query as written answers **no row**, not `0`. The inner
  join to `q0` does the same. Wanting `0` is a different query —
  `OPTIONAL { { SELECT … } }` — and the same ops give it: op 5 on the
  `LeftJoin`, `?trackQty` unbound for that tunnel. `BIND(COALESCE(?trackQty,
  0))` on top is a `Bind` the engine keeps — a fetch, not the statement —
  unless `COALESCE` becomes an `Expr::to_sql` case, which is orthogonal.
* **A duplicate entry in the array.** Two cases, and they differ. A
  duplicated *scalar* (`["Freight", "Freight"]`) is one triple — a
  graph is a set — and the existing `SELECT DISTINCT e.value` dedup is
  right. A duplicated *structure* (two identical covered sections) is
  two blank nodes and two triples, so `COUNT(?cs)` is 2 on the engine
  leg; the statement must count 2 too, and today's flat form counts 1.
  The `Unnest` contract (part 5) dedups by value for a scalar step and
  by occurrence for a structure step; the seed carries one of each.
* **JSON `null` / absent key.** A required read excludes both; the element
  unnest of a missing array yields no rows. Unchanged.
* **Multi-valued key.** `GROUP BY ?a` on an identity: one row per record.
  `GROUP BY ?cs` (an element) is a `Structure` key, grouped by
  occurrence (*What may cross a relation*, part 3); a join above on
  `?cs` is `JoinKey::Element` when the other side is the same
  `(CEA, hasCoveredSection)` structure, and the outcome is a
  `Statement` (MR4) — or a `Fetch` when `?cs` reaches the root
  projection. Revisions 1–4 said op 5 declines here because `?cs` is
  not an identity; it declines only when the other side is not the
  same structure.
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
   makes `isReference = true` a `BoundElement` filter above the unnest
   (as built from revision 10: through revision 9 its `condition_for`
   refused the collection hop and the block stayed on the engine as
   three islands — consolidator #464, #471). With the outer
   `belongsToSubZone <Z>` beside it, op 3a's row-test arm then places
   `?a.belongsToSubZone = <Z>` at S1's root too, and 3b brings it to
   rest above the body's scan.
3. **The element-held edge** lets `PushReferenceJoin` push `n8`'s join on
   `?track` between the element and the Track scan.
4. **The nested `OPTIONAL`, recursively.** Op 1 has already wrapped
   `n9` as `subselect [?track ?trackDiscr]` — scope S2, nested in S1,
   exporting `?track` (guaranteed: `n9` reads it as a subject) and
   `?trackDiscr`. S2's one match, `?track :hasTrackType ?trackDiscr`,
   carries **no type of its own** — round 3's point — so nothing folds
   it until **op 3a** places `?track ∈ Identity(Track)` at S2's root
   (the inner `LeftJoin`'s left side has `term_of(?track) =
   Identity(Track)` from S1's Track scan, guaranteed; S2 is its right
   side and its only consumer) and **op 3b** walks it through S2's
   identity `Project` to the match, with no stop in between. Only then
   does `FoldMatchesIntoScan` give S2 its own `scan :Track as ?track` (a
   star per scope), reading `hasTrackType`, discharging that
   `o_boundary`. This is why staging item 4 (#464 as a statement) sits
   after item 2 (3a/3b): a nested `OPTIONAL` that names no class is the
   normal spelling, and it is typed only by the restriction family.
   **Op 4** flips
   S2's barrier (one `[S]` scan, no correlated inputs, both exports
   columns). **Op 5** pushes `n10`, the inner `LeftJoin`, on `?track`:
   `Identity(Track)` and guaranteed on both sides (the Track scan of
   S1 on the left, S2's relation column on the right), `condition:
   None`. Nothing here is `AbsorbOptionalRead`: revision 2 called that
   rule during refinement, which contradicts its own decision to move
   absorption to lowering. What *lowering* then does with S2 is the
   physical choice of op 1's third bullet: the body is one `Scan`
   reading one single-valued slot of the preserved star, so it renders
   as the nullable column `t2.object_data->>'hasTrackType'` rather than
   as a derived table — the same SQL as before, reached through a
   barrier, which is the phase separation composing on a real case
   rather than being asserted.
5. **`PruneUnusedExports`, then op 4.** The query as #464 wrote it is
   `SELECT *`, so the root `Project` references `?cs` and nothing is
   pruned. Op 4 flips S1's barrier anyway: every node in S1 is `[S]`
   (S2's barrier included, a leaf of S1's island), no correlated
   inputs, every export **representable** — `?cs` a `Structure` of
   `(CEA, hasCoveredSection)` by occurrence; `?a` and `?track`
   identities; the rest slots; `?trackDiscr` S2's nullable column, not
   guaranteed, which the column records.
6. **Op 5** pushes `n11` as a `LeftJoin` on `?a`: `Identity(CEA)` on both
   sides, `?a ∈ guaranteed` on both — on the *outer* side because it is
   `n0`'s scan identity on the preserved side, on S1 because the derived
   property says so at S1's root; "an identity is always bound", which
   revision 1 wrote here, is not a rule — `condition: None`.
7. **The root `PushProjection`** asks *serialisable* of every projected
   variable. For `SELECT *` that includes `?cs`, a `Structure` with no
   term: it declines, and the outcome is a **`Fetch`** — the outer scan
   left-joined to `q0` is the fetch, the engine finishes and emits the
   blank node. Revisions 1–4 wrote "takes `SELECT *`" here, which
   contradicted question 5; the statement below is the outcome for the
   **scalar projection** `SELECT ?name ?kpFromMeter ?track ?trackName
   ?trackCode ?trackDiscr`, for which step 5 prunes `?cs` from S1's
   exports and the root projection is all serialisable columns —
   **`Statement`**. Both are pinned (test 12).

```sql
SELECT t0.object_data->>'identification' AS name, q0.kp_from_meter, q0.track, q0.track_name, q0.track_code, q0.track_discr
FROM golden_records t0
LEFT JOIN (
  SELECT t1.asset360_uri AS a,
         e.value->>'hasEntryPointM' AS kp_from_meter,
         t2.asset360_uri AS track, t2.object_data->>'name' AS track_name,
         t2.object_data->>'longname' AS track_code,
         t2.object_data->>'hasTrackType' AS track_discr
  FROM golden_records t1
  CROSS JOIN LATERAL jsonb_array_elements(t1.object_data->'hasCoveredSection') WITH ORDINALITY AS e(value, ordinal)
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
| `{ SELECT ?s WHERE { ?s a :Signal } ORDER BY ?s LIMIT 3 } ?s a :Signal ; :hasName ?n` — top-N, then read, **typed in both scopes** | `PushProjection` and the sort/slice rules in the scope, 4, 5 | `JOIN (SELECT … ORDER BY … LIMIT 3) q ON q.s = t0.asset360_uri` — **`Statement`** (MR1) |
| the same with the outer read **untyped** (`… } ?s :hasName ?n`) | 3a on the *outer* side (the relation column is `Identity(Signal)`, guaranteed), 3b through nothing, fold; then 4, 5 | the same statement — **`Statement`** in the 3a/3b MR. In MR1 the outer domain leaves `?s` untyped, `resolve` finds no `Scan` for it, and the outcome is **`Rejected`** (`query_unscoped`, naming the outer `?s`); today's fallback answers it **wrong** (the scoper merges the outer read into the inner star, so the fetch is Signals *with a name* and the top 3 are ranked over the wrong universe). Test 10 |
| `OPTIONAL { { SELECT ?a (COUNT(?cs) AS ?n) … GROUP BY ?a } }` — #466 with zeros | `PushGrouping` in the scope, 4, 5 (left) | `LEFT JOIN (grouped) q`, `?n` unbound for a tunnel with none — **`Statement`** (MR1) |
| `?a a :CEA ; :hasCivilEngineeringAssetType <Tunnel> . { SELECT ?a (COUNT(?cs) AS ?n) WHERE { ?a a :CEA ; :hasCoveredSection ?cs } GROUP BY ?a }` — #470: the body typed in its own domain, the **outer row test** on the shared identity; also under `OPTIONAL { { … } }`, and the mirror `{ SELECT ?s WHERE { ?s a :Signal ; :length ?l FILTER(?l > 2) } } ?s :name ?nm` where the body's test crosses **out** to the untyped outer read after its class | 3a's row-test arm (revision 10), 3b through the projection and the keyed `Group` to rest above the scan, then 4, 5 | the derived table's `WHERE` carries the test; the body reads the restricted class — **`Statement`**; on the fetch route the sub-select's star is narrowed the same way (`keep_what_the_rules_proved` is local to the naming domain). A test the outer side applies under its own `OPTIONAL`, in one `UNION` arm, on another star or on an element stays where it is, and a body that already spells it gets no copy — each a test |
| `OPTIONAL { ?s :ref ?t . ?t a T ; :x ?p ; :y ?q }` — 28d's declined two-read over a reference | 1, 3a/3b (the body's `?s` has no local type; typed from the preserved side), `PushReferenceJoin`, 4, 5 | `LEFT JOIN (SELECT … FROM t1 JOIN t2 …) q` — **`Statement`** after MR3; **`Fetch`** before (as today) |
| `OPTIONAL { ?s :ref ?t . ?t a T . FILTER(?t.x > 5) }` — the lifted condition | 1, 2, then as above | condition in the derived table's `WHERE` — **`Statement`** after MR3; **`Fetch`** before |
| `{ SELECT (COUNT(*) AS ?total) WHERE { ?s a :Signal } } ?s a :Signal ; :hasName ?n` — a scalar beside every row | `PushGrouping` in the scope (no keys), 4; the outer join has `on = []` | `CROSS JOIN (SELECT count(*) …) q` — op 5 with an empty key is a cross join, which is what a natural join on no variables is; worth its own row in the tests — **`Statement`** (MR1) |
| `{ SELECT ?s (COUNT(?x) AS ?nx) WHERE { ?s a :Signal ; :x ?x } GROUP BY ?s } { SELECT ?s (COUNT(?y) AS ?ny) WHERE { ?s a :Signal ; :y ?y } GROUP BY ?s } ?s a :Signal` — two grouped sub-selects joined on one identity, **each body typed in its own scope** (an untyped body is the 3a/3b MR's) | `PushGrouping` in each scope, 4 twice, 5 twice | two derived tables joined on `s` — **`Statement`** (MR1) |
| `{ SELECT ?s ?nx ?ny WHERE { { SELECT ?s (COUNT(?x) AS ?nx) WHERE { ?s a :Signal ; :x ?x } GROUP BY ?s } { SELECT ?s (COUNT(?y) AS ?ny) WHERE { ?s a :Signal ; :y ?y } GROUP BY ?s } } } ?s :hasName ?n` — the review's case: the two joined **inside** a third sub-select, each body typed locally, **the outer read `?s :hasName ?n` untyped in its own domain** | the row above inside S0, then 4 and 5 for S0 — the derived properties are recursive, so the outer barrier sees `?s` as `Identity`, guaranteed, and `?nx`/`?ny` as measures, without a descendant search; then **3a on the outer side** (S0's relation column is `Identity(Signal)`, guaranteed; the outer read is a `Join` side), 3b through nothing, fold, and 5 on the outer join | a derived table whose body joins two derived tables — **`Statement`** in the 3a/3b MR; **`Rejected`** (`query_unscoped` on the outer `?s`) in MR1, for the same reason as the second row |
| `?s a :Signal . { SELECT ?s WHERE { ?s :p ?x } ORDER BY ?s LIMIT 1 }` — the review's op-3 counter-example, **untyped** below the slice | 3a at the root (above the `Slice`, since the barrier wraps it), 3b **stops at `Slice`**; then **nothing** — the match below the slice is untyped and no rule in this document turns an unrestricted match into a scan, so op 4 has no `[S]` input and does not fire | **`Rejected`** (`query_unscoped`, naming the inner domain's `?s`) — in MR1 and after 3a/3b alike: `resolve` finds no `Scan` for `(inner, ?s)`. Not a fallback, correct or otherwise: the inner universe is every record with `:p`, and no fetch the scoper can spell preserves the answer without fetching that universe — the property this row rests on. Revision 4 wrote "refused — correct fallback", which implied a fetch exists. *And today's fallback answers it wrong*: the scoper keys stars by variable name across the `Project`, so the inner `?s` joins the outer `Signal` star with `name` required, the fetch holds Signals only, and the engine's `LIMIT 1` picks the first *Signal* where SPARQL picks the first record of any class (verified on `main` with `sparql_scope`: one star, `Signal`, `required_fields: [name]`). Revision 2's row promised SQL for this, which had regained admission by importing the outer class below the slice; revision 3 refused it *for having a `Slice`*, which round 3 showed is the layout and not the cause. Under the scope-local scoper contract the inner `?s` is a star of its own scope with no class there, and is refused as any untyped subject is — the `Slice` is incidental. The already-typed top-N (first row) is the positive test; this row asserts the `Rejected` outcome plus the oracle's answer (empty), and no SQL |
| `?s a :C . { SELECT ?x WHERE { ?s :p ?x } }` — round 3's plain projection: the inner `?s` is **private**, the join is a cross join, and the answer keeps every `:p` value of every record; also its keyed-`Group` twin `{ SELECT ?x (COUNT(*) AS ?n) WHERE { ?s :p ?x } GROUP BY ?x }` | nothing: no `Slice`, no keyless `Group`, and still no type in the inner domain | **`Rejected`** (`query_unscoped`) by the same contract, for the same reason, with the same message as if the variable were spelled `?inner` — and the alpha-renamed spelling must produce the identical fetch and plan. Today's scoper merges the private `?s` into the `:C` star and loses the row; that is the bug MR1 fixes, by the contract rather than by naming this row |
| `SELECT (COUNT(DISTINCT *) AS ?n) WHERE { { SELECT ?x ?y WHERE { … } } }` — round 5's whole-mapping observer over a sub-select exporting a column nothing names; also `SELECT DISTINCT ?x` / `REDUCED` outside, and `{ SELECT ?x ?y … } MINUS { ?z :p ?y }` | `demand` at the `Group` (every output of its input), at the `Minus` (the shared `?y`): `PruneUnusedExports` does not match, `?y` stays. Under `SELECT DISTINCT ?x` the `Project` below the `Distinct` does not pass `?y`, so it *is* pruned, harmlessly. `PushGrouping` declines `COUNT(DISTINCT *)` as today | **`Fetch`** for the count (the oracle's `2` is pinned, test 12; a prune would have answered `1`, and the transition check *no demanded export is dropped* is what fails in `refine` if a rule ever tries); **`Statement`** for the `DISTINCT ?x` form over a typed body |
| `?s a :Signal . FILTER NOT EXISTS { ?s :ref ?t . ?t a T ; :x ?p ; :y ?q }` | a `Testing` scope: op 1 does not apply (it is not a `LeftJoin`), op 4 does not match (`correlated_inputs` non-empty, exports nothing) — the correlated `NOT EXISTS (SELECT …)` is its own op | **`Fetch`** today and throughout this document; a widening of `PushNotExists` from "exactly one scan" to "a lowered testing scope", with its own proof, is the follow-up |

What does **not** appear: a rule that mentions `hasCoveredSection`, a count,
an array, a tunnel, or "sub-select joined to the driving variable".

## What stays refused, and says so

Each is a precondition of one op, and the plan printout names the node.

* **A join on a value column** (`?a :code ?c . { SELECT ?c … }`): op 5,
  identity or element only. Reason: term fidelity of two stored texts.
* **A structure in the answer** (`SELECT *` over #464, `SELECT ?cs`):
  the root `PushProjection`, *serialisable*. Reason: a blank node has
  no term a statement can emit; the outcome is a `Fetch` and the engine
  emits it. Widening: a term for elements (skolem), the datamodel's
  call.
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
  not narrowed. Correct, and the printout shows where it stopped. When
  the scan below is typed in its own domain the outcome is a
  `Statement` (the typed top-N row); when it is not, `resolve` refuses
  the query (`Rejected`) — a stop never leaves an untyped star to a
  fetch.
* **A boundary restriction whose path meets a shared node**: op 3b
  declines the step (ownership along the path); the restriction stays
  above the shared node, correct, and the printout names it.
* **A row test that is not a record predicate, or not applied to every
  row** (revision 10): a test under the other side's `OPTIONAL` or in
  one `UNION` arm, on an element of its fan-out, on another star,
  through a function, or a containment test on a collection: op 3a's
  row-test arm does not carry it. The class still crosses; the body
  reads the whole class under the test it did not get, which is what it
  did before. Widening: `AnyElement` once the oracle can spell it back.
* **A sub-query whose star has no class in its own domain** (a private
  inner variable, whatever its name; an exported one that only the
  outside types, until the 3a/3b MR's transfer proof reaches its scan):
  the scoper's existing `Unscoped` refusal, per domain, decided at the
  pipeline's `resolve` step — `Rejected`. On the statement route the
  same body has no scan and op 4 does not match.
* **A `Join` inside a scope, on 3b's way down**: pushed to one side
  that guarantees `?v`; the other side's scan is not narrowed. Correct
  and deliberately narrow until a measurement asks for the split.
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
  `Testing` and is not. A restriction reaching a `Union` from *above*
  it (3b) splits into one obligation per arm. Unchanged in effect.
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
   scalar sub-select cross join; the top-N-then-read; (revision 10) the
   outer row test crossing into a typed body, mandatory and under
   `OPTIONAL`, and out to an untyped outer read; the four tests that do
   not cross; the constant on an unnested element as a `Statement` on
   the real schema.
2. **Property-style**, in the spirit of `the_rule_order_does_not_decide_the_fixpoint`:
   generate bodies from a small grammar — a driving class; 0–2 reads of it;
   an optional or mandatory array hop with 1–3 element reads; an optional
   or mandatory reference hop with 0–2 reads and an optional nested read;
   a `GROUP BY` on the identity **or on an optionally-bound variable, or
   no key**, with `COUNT`/`MIN`/`MAX` or none; an optional `ORDER BY` +
   `LIMIT`/`OFFSET` on the body; a body variable that **shadows** an
   outer name without exporting it; **one or two** shared variables with
   the outside; **an exported variable the outside never names**,
   under an outer `SELECT DISTINCT`, `REDUCED`, `COUNT(DISTINCT *)`, a
   `MINUS` that binds it, or nothing (round 5: the whole-mapping
   observers, each a `demand` arm); **a two-level array hop**
   (`parts/children`) with 1–2 element reads; **nesting** one generated
   body inside another, one level; placed as a mandatory sub-select, an
   `OPTIONAL` body, or `OPTIONAL { { SELECT … } }`; (revision 10) an
   outer row test on the shared identity as a constant and as a
   `FILTER`, and a constant on the unnested element alone and beside a
   read. The seed has an
   empty class, a record with a duplicate array entry, a record with
   identical elements at the same index of two sibling nested arrays
   (`parts[0].children[1]` = `parts[1].children[1]`), and two records
   identical in every read slot (duplicate mappings). For each generated query assert (a) `refine` reaches a
   fixpoint with every invariant holding, (b) the plan lowers or names the
   node that stopped it, (c) where it lowers, the statement's answer equals
   both oracles as a bag, and **(d) each logical rewrite, taken alone, is
   answer-preserving** — *evaluated*, not inferred from whichever plans
   happen to lower. Revision 2 truncated the rule list and compared the
   plans that reached SQL, which skips every intermediate plan, i.e.
   exactly the ones 3a/3b and a nested boundary produce. Revision 3
   adds the instrument: **`plan_to_algebra(plan, node) ->
   spargebra::GraphPattern`**, a back-translation of the logical subset
   — `Match` to its triple, `Scan` to `?v a C` plus one triple per read
   (a `Required` read is the triple, an optional read is `OPTIONAL`),
   `Unnest` to the multivalued slot's triple, `Filter`/`Bind`/`Sort`/
   `Distinct`/`Slice`/`Group`/`Project`/`SubSelect`/`Join`/`LeftJoin`/
   `Union`/`Values` to their algebra, a `Relation` to a sub-select, the
   barrier (`SubSelect`) to the identity — its input is the sub-query,
   modifiers and projection included — and the boundary filter to
   `FILTER(?v IN (…))` over `?v a C`. The naive plan is the algebra
   faithfully (`Builder::pattern`), so the translation of the naive plan
   must be the query it came from, which is the translation's own test
   — for the nodes the naive plan has. The nodes it does not have —
   `Scan`, `Unnest`, `Relation`, the boundary filter — get **direct
   tests** of their own, since the round trip never exercises them: a
   `Scan` with a `Required` read and one with an optional read, each
   evaluated against the matches it replaced; an `Unnest` over an array
   with a duplicate entry and over an absent key, asserting
   **multiplicity** against the triple it stands for — one row per
   distinct *value* for a scalar slot, one row per *occurrence* for an
   inlined structure (two identical structures are two blank nodes),
   none for a missing array; a nested `Unnest` over `parts/children`
   asserting the composed identifier tells `parts[0].children[1]` from
   `parts[1].children[1]` and that each equals itself across a
   `Relation` boundary; a `Relation` against the `SubSelect` it
   lowers. Then for every generated query and
   every rule application in `refine`'s trace: translate the plan
   *before* and *after*, evaluate both on the in-memory oracle
   (oxigraph over the full fixture), compare as bags. For 3a the unit
   compared is the **enclosing join** (`Join(L, R)` against `Join(L,
   Restrict(R))`), not the restricted child. Plus the three legal
   schedules (the list, the list reversed, one random permutation under
   a fixed seed) end to end, and a **positive** case for a permitted
   cross-scope transfer (a restriction that legally reaches a scan two
   barriers down, asserted to have moved and to agree). A rewrite that
   is only right in combination with a later one is a rewrite whose
   precondition lies. The grammar is small enough to enumerate
   exhaustively rather than sample.
3. **Invariants driven by a bad rule**, as 28d does for the frontier: a
   barrier that exports a variable its body does not bind; a join key on a
   slot column; an identity join with the key recorded on the wrong side;
   an identity join on a key `term_of` accepts and `guaranteed` does not;
   a lifted condition sunk although it reads an outer variable, and one
   sunk although it calls `RAND()`; an optional-side filter reparented to
   the outer scope (*obligations stay in their scope*); a boundary
   restriction pushed below a `Slice`; an export pruned under a
   `COUNT(DISTINCT *)`, under a `Distinct`, and one shared with a
   `Minus` right side — these three applied **through `refine`**, the
   bad rule's `apply` pruning with no match, so that *no demanded
   export is dropped* is what rejects them, and each also asserts that
   `Plan::check` *passes* on the pruned plan, pinning why the check is
   a transition (round 6); a `Structure` join key
   that compares the last hop only (*join keys agree*, since `term_of`
   gives the path and the key must cover every hop of it). Each must
   fail at the rule, not in a result.
6. **The fallback with a hidden variable**: the review's COUNT case, run
   with the SQL lowering *deliberately refused* (`tests/support.py`'s
   sibling of `force_engine_leg` that fails `lower_refined` by name) so
   the plan is refined, the fetch reconstructed by
   `keep_what_the_rules_proved`, and the engine answers over it. Both
   `:C` records must come back with `?n = 1`. Then the same with the
   inner and outer variable spelled the *same* and the inner one
   exported — the case where the narrowing *is* valid, through the
   boundary proof — asserting the fetch was narrowed.
7. **The review's op-3 counter-example**, through the full entry point:
   the outcome is `Rejected(query_unscoped)` naming the inner domain's
   `?s`, in MR1 and after the 3a/3b MR alike — no SQL is asserted,
   because none is produced. The in-memory oracle's answer (empty) is
   recorded beside it, so the day a widening admits the shape the test
   already says what it must answer. After the 3a/3b MR the *refined
   plan printout* additionally shows the restriction above the `Slice`
   and the scan below it untyped.
8. **Scope-local scoping**, each against the in-memory oracle over the
   complete fixture, since the C-only fetch is exactly what a wrong
   scoper produces and the engine leg would agree with it: the plain
   projection (`?s a :C . { SELECT ?x WHERE { ?s :p ?x } }`, two rows);
   its keyed-`Group` twin (the `"B"` group survives); the top-N
   counter-example; and each of them **alpha-renamed** (`?s` → `?inner`
   in the sub-query), asserting the scoper's stars, required fields and
   the refined plan are identical up to the name. In MR1 every one of
   these is a refusal, and the test asserts the refusal *and* that the
   refusal names the inner scope's untyped subject, not an operator.
9. **The boundary wraps the sub-query** (pinned trees): the naive plan
   for a typed top-N with `ORDER BY … LIMIT`, a grouped sub-query with
   `HAVING`, one with `DISTINCT`, one with `OFFSET`, each a golden
   printout with `subselect` on top and the modifiers beneath in the
   algebra's order; and for each, that the join's input *is* the
   barrier (`consumers(barrier) == [join]`). In the 3a/3b MR: the
   boundary filter on the typed top-N lands above the `Slice` in the
   printout, and the S → T → U transfer and a `Union` split each
   validate against the chain-of-arms invariant and evaluate equal on
   the oracle.
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
10. **The full entry point** (`plan_query_refined`, not a rule called by
    hand), asserting the *outcome* of the pipeline section: the untyped
    outer read beside a typed sub-query (`Rejected` in MR1, `Statement`
    after 3a/3b), #464's nested `OPTIONAL` (`Fetch`, then `Statement`),
    the top-N counter-example and the plain projection (`Rejected`
    throughout), each alpha-renamed to the identical outcome; and the
    correspondence invariant — the scoper's naming domains and the
    plan's barriers in bijection — over the property grammar.
11. **Rewrite lifecycle** (the *Evidence that survives rewriting*
    section): 3a → 3b through a `Project` → `PushProjection` absorbs it
    → the key resolves through `retired`, the recomputed chain holds,
    the plan lowers; 3a re-offered after 3b moved the filter declines
    on the canonical key; a `Split` then a fold that removes a node an
    arm's step named; a bad rule that retires a named node with no
    successor fails *evidence resolves* at the rule; `MAX_ROUNDS = 1`
    over the grammar still passes `Plan::check`, lowers and agrees with
    the oracle.
12. **The structure interface** (*What may cross a relation*): #464 as
    a scalar projection (`Statement`, `?cs` pruned), as `SELECT *`
    (`Fetch`, the engine emits the blank node), and an element joined
    in an enclosing scope (`JoinKey::Element`, `Statement`); each
    against the oracle; `COUNT(?cs)` over a duplicated structure equals
    the oracle's 2. **Round 5's two fixtures**: `SELECT (COUNT(DISTINCT
    *) AS ?n) WHERE { { SELECT ?x ?y WHERE { VALUES (?x ?y) { (1 10) (1
    20) } } } }` answers `2` after `refine` (the `Group`'s `demand`
    keeps `?y`; the outcome is `Fetch`), with its `DISTINCT ?x`,
    `REDUCED ?x` and `MINUS` siblings; and the nested-array duplicate —
    `parts[0].children[1]` identical to `parts[1].children[1]` —
    counted `2` and joined to itself only, through a `Relation`, a
    `JoinKey::Element` and a `GROUP BY` on the element.

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
unnest, on the order of the 0.4 s the fetch already takes — against
**the engine leg after the oxigraph fix, not against 17 s**: with
oxigraph PR 1733 applied the block costs milliseconds on the engine
(see the #464 section), so what the statement buys is no materialised
fetch and paging by the statement (#457, pepibru GitLab), not a
rescue. Both numbers are predictions to be measured on the review app,
not promises.

**Code.** One computed `Scope` with `Scope::of`; the six derived
properties as functions over the plan (`outputs`, `guaranteed`, `term_of`
absorbing today's `Visible::identity_of`/`slot_of`, `correlated_inputs`,
`demand`, and `effects` which exists); `Visible` reading barrier columns;
`applies_to_every_answer` scope-local with a keyless-`Group` arm;
`keep_what_the_rules_proved` keyed by producer; the builder's sub-query
construction (a fresh spine, wrapped whole) and its pinned trees; two
one-line anchor changes in the two tail walks; new rules ops 1, 2, 3a,
4; op 3b as a per-operator table that mostly restates
`PushComparisonFilter`'s placement, with `Transfer` and `Split` steps
and ownership along the path; the three absorb rules moved from
`tier_one_rules` to `lower_refined`'s choice of physical form; one
widened rule (op 5, and the element-held edge in `foreign_key_on`);
`Op::Relation`, `JoinKey` (`Identity`, `Element`, `Cross`),
`ColumnKind::Structure` as one text column composed per hop, the
unnest's `WITH ORDINALITY` (with the scalar-versus-structure dedup
fix), `PruneUnusedExports` on `demand`, `PLAN_CONTRACT` 5; `NodeKey`
on every node, `Plan::rebuild` replacing nine hand-written
renumberings, `Plan::retire`; nine invariants (closure on producers,
obligations stay in their scope with the cursor over transfers, join
keys agree, an `Exporting` scope has one consumer, a boundary
restriction's path is a chain of 3b arms over singly-consumed nodes —
recomputed from the plan, evidence resolves, every `BoundElement` read
has its unnest, the scoper and the plan agree) plus the one transition
check, no demanded export is dropped, in `refine`; **the scoper keyed by
`(naming domain, variable)` with per-domain class inference**, an
`Untyped` star recorded rather than refused, and the `resolve` step in
`sparql_plan.rs` with the outcome enum — a change in
`sparql_scoper.rs`'s star construction and one new phase, not a new
refusal kind; `plan_to_algebra` for test 2(d) with direct tests for
its non-naive nodes; the recursive FROM item in `sql_builder.py` and
its `_OpRelation` in `plan_ops.py`; the in-memory oracle in
`tests/support.py`. One
asset360-rust release per MR, one pin bump each. More than revision 1
said — the derived properties and the fallback provenance are the first
MR's cost and were missing — and about twice #463 (pepibru GitLab, N and
nested reference `OPTIONAL`s) for the first MR now that 3a/3b are out of
it, then about #463 each for the other three.

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
   moving three rules from refinement to lowering is the `OPTIONAL`-body
   MR's largest item (staging item 3). Blocking for that MR, not the first.*
3. **`LATERAL` versus self-join rendering of a relation whose scan is the
   outer star.** Assume self-join first, `LATERAL` measured afterwards on
   the review app. *Not blocking.*
4. **Op 3 on a user-written sub-select — closed by review round 1,
   re-scoped by round 2.** It was a correctness question, not a
   widening: the boundary restriction (3a) applies to any `Exporting`
   scope on a `Join` side or a `LeftJoin` right side, and whether it
   reaches the scan is 3b's per-operator walk. Revision 2 then put the
   whole 3a/3b family in the first MR "because #466's body has a
   `Group`" — but the #466 walk-through says 3a *does not fire* for
   #466 (its inner type is explicit, the "already carries" clause), so
   the keyed-`Group` arm is never exercised by it. The family moves to
   its own MR with its transfer and ownership contracts. *What is open
   is whether 3b's table is complete — a reviewer who can name an
   operator it commutes through wrongly reopens it. Not blocking for
   the first MR; blocking for the 3a/3b MR.* Round 3 added two details
   to that MR's contract, both now in the text: the path is validated
   with a cursor so it composes to any depth, and it splits at a
   `Union`; and ownership is checked along the path, not at the
   barrier. The `Join` arm pushing to one side rather than both is a
   deliberate narrowing, not an open question.
11. **Is the wrapped barrier's identity `Project` worth eliding?** The
    construction leaves `subselect → project` for every sub-query, one
    node more than today for the modifier-free case. Assume it stays: a
    barrier that is always the same node over the same chain is what
    the pinned trees, op 4's match and `plan_to_algebra`'s identity
    translation rely on, and `PushProjection` absorbs it for free. *Not
    blocking.*
12. **How the scoper represents a naming domain — closed by round 4.**
    A domain id threaded through `tag_triples_by_depth` beside `depth`,
    opened at each off-spine `Project` (the scoper's existing
    structure, one more tag). The class of a star the domain leaves
    untyped comes from the refined plan at `resolve`, never from a
    second walk. The correspondence to the plan's barriers is a tested
    bijection (test 10). *Closed.*
5. **An element as an interface variable — closed by round 4.**
   `ColumnKind::Structure`, identified by occurrence — `(holder, hops)`,
   the whole path from the record root, one text column (revision 6,
   round 5's nested-array finding) — representable and not
   serialisable (*What may cross a relation*). `PruneUnusedExports` removes it when nothing reads it;
   `SELECT *` over it is a pinned `Fetch`, a scalar projection a pinned
   `Statement`, an element joined outside a pinned `Statement` by
   `JoinKey::Element`. A *term* for elements (skolem IRIs, which the
   turtle writer can already mint) would make `SELECT *` a statement
   and is the datamodel's decision — question 13. *Closed; nothing
   blocks.*
6. **Reads of an element's slots as scan paths — closed by round 4.**
   `ScanSlot.path` stays rooted at the star; the element step is what
   `BindingSpec.containers` and `SlotReading::BoundElement` already
   say; the new invariant *every `BoundElement` read has its unnest*
   is what the plan guarantees. *Closed.*
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
10. **Carrying a restriction across a barrier into the fallback fetch —
    closed by round 4.** It is the pipeline's `resolve` step: a star its
    domain leaves untyped takes the class of the refined plan's `Scan`
    for it, which is typed when the domain's own scan is — by its local
    type match (then the scoper agreed already) or by 3a placing the
    restriction and 3b carrying it to the scan with no stop between
    (round 5's consistency point: revision 5 said "only when 3a placed
    it", which contradicted local typing); the fetch of that
    star is then narrowed by a proof the plan holds, read once. MR1 has
    no 3a/3b, so `resolve` types nothing and refuses what round 3
    refused; the 3a/3b MR brings the widening and test 6's second half
    with it. *Closed.*
13. **A term for elements.** The engine leg's turtle writer mints a
    blank node per inlined structure (`skolem: false`); with `skolem:
    true` it would mint `<parent>/<slot>/<member>` IRIs, and a statement
    can spell the same from the occurrence identifier — it *is* that
    path, with the index for the member — which would make
    `SELECT *` over #464 a `Statement` and change what every element
    answers as, on every route. Assume blank nodes stay. *Not blocking;
    the datamodel's call, and the outcomes are pinned either way.*

## Staging

Four MRs, each shippable and each parity-tested; the first serves #466
on its own and is narrower than revision 2's, as the review asked.
*Built as one body of work on this branch, per the human's call
("i'd not do staging"); the items survive as the commit subjects and as
the grouping below.*

0. **Not in this document, and first:** the oxigraph optimizer fix for
   #464's engine cost — PR 1733's hunk on `sparopt 0.3.7` under
   `[patch.crates-io]` (a fork branch, dropped the day a 0.5.x release
   carries it; asking upstream for that backport costs one issue), or
   the 0.6 line when this crate moves its algebra. One dependency
   change, one release; the measurement is on branch
   `exp/464-oxigraph-1733-backport`. It is the human's call and it is
   not a planner change, which is why it is item 0.
1. **Scope, derived properties, barrier, identity join** — `Scope::of`,
   the six properties (`effects` exists; `guaranteed` conservative per
   question 9; `demand` with its conservative default), scope-local
   `applies_to_every_answer` and producer-keyed
   `keep_what_the_rules_proved`, scope-aware `Visible`, the two anchor
   changes, ops 4 and 5 and `PruneUnusedExports` on `demand`,
   `Op::Relation` + `JoinKey` (`Identity`, `Cross`) +
   `ColumnKind::Structure` as the composed occurrence column with the
   unnest's ordinality and the structure dedup fix, contract 5,
   `NodeKey`/`Plan::rebuild`/`retire`, the invariants *closure on
   producers*, *obligations stay in their scope*, *join keys agree*,
   *an `Exporting` scope has one consumer*, *evidence resolves*,
   *every `BoundElement` read has its unnest* and *the scoper and the
   plan agree*, the transition check *no demanded export is dropped* in
   `refine` (every build), the recursive FROM item,
   the builder's sub-query construction with its pinned trees,
   `plan_to_algebra` (with direct tests for `Scan`/`Unnest`) and both
   oracles, **the pipeline**: the scoper keyed by `(naming domain,
   variable)`, a class inferred from a star's own domain only, an
   untyped star recorded and refused at `resolve` (which types nothing
   in this MR), the outcome enum, tested by alpha-renaming and through
   the full entry point. Serves a user-written sub-select **typed in
   its own domain** (grouped or top-N) joined on an identity: #466, and
   the table's typed top-N, #466-with-zeros, scalar cross join and
   two-grouped-sub-selects rows. The nested row and the untyped outer
   read are `Rejected` here and served by item 2.
2. **Restriction at a boundary** — ops 3a and 3b with the obligation
   path (cursor-validated transfers, `Split` at a `Union`, one side at a
   `Join`), ownership along the path, the canonical obligation key, the
   *chain-of-3b-arms* invariant recomputed from the plan, and
   `resolve` reading a plan-derived class for a star its domain left
   untyped (question 10, the fallback narrowing across a barrier, with
   test 6's second half); the lifecycle tests. Serves the untyped-body
   rows, the nested row and the untyped outer read; the top-N
   counter-example stays `Rejected`, now with the restriction visible
   above the `Slice` in the printout.
3. **The `OPTIONAL` body as a scope** — ops 1 and 2, and the absorb rules
   moved to the lowering's physical choice. Serves the two-read `OPTIONAL`
   over a reference and the lifted condition: the table's two `OPTIONAL`
   rows.
4. **The element-held edge** — the `foreign_key_on` widening, the
   `BoundElement` edge spelling on the statement route, and
   `JoinKey::Element`. **Depends on item 2**: the nested `OPTIONAL`'s
   scope in #464 names no class and is typed by 3a/3b (step 4 of the
   walk-through). Serves #464 *as a statement* for its scalar
   projection (`SELECT *` is a pinned `Fetch`, question 13) and the
   element-joined-outside row; its speed is item 0's.

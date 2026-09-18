# A body is a relation: lowering a grouped sub-select and an `OPTIONAL` body as one derived table

Status: **design, nothing built.** Draft PR for review; the two issues it
answers stay parked until the design is agreed.

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

One representational fact, one lowering target, and five equivalence ops.
Three of the five are new rules; two are widenings of rules that exist,
each a precondition made precise rather than a branch added. Nothing in the
list names a predicate, a class, or a query.

### The fact: a scope

```rust
/// Where a subtree is a unit of its own: evaluated to solutions, then joined
/// to the outside on its interface variables and nothing else.
///
/// Computed, not stored. A `SubSelect` node's input is one; so is the right
/// side of a `LeftJoin`, of an `AntiJoin`, of a `Minus`, and each arm of a
/// `Union`. `Plan::scope_of(node)` answers which scope a node is in, and
/// `Plan::scope_root(scope)` its root.
pub struct Scope { pub root: NodeId, pub interface: Vec<String> }
```

`interface` is the variables the outside may see: a `SubSelect`'s `vars`;
for a `LeftJoin`/`AntiJoin`/`Minus` right side, every variable the body
binds (SPARQL projects nothing away there). A scope's root is its topmost
node; the plan root is the root of the outermost scope.

Four things change to read it, and they are the whole "let existing rules
run inside" claim:

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
  `Column` per interface variable, typed as what the body bound it to —
  `Identity(class)` when the body's `Visible::identity_of` says so, a slot
  binding (path, reading, presence) when a scan bound it, `Measure` for an
  aggregate. `identity_of(?a)` above a pushed sub-select therefore answers
  the inner star's class, which is what lets a join key on it, and
  `slot_of(?a)` stays ambiguous, so a filter on it still declines. A scan
  inside an *unpushed* barrier stays invisible too: nothing outside may
  address it, which is what "barrier" meant all along.
* **`applies_to_every_answer` is unchanged**, and that is worth saying:
  a `SubSelect` on the mandatory side of a `Join` passes its rows through,
  so a constraint under it still decides which answers there are. It is
  transparent for the *answers* and opaque for the *addresses*, and the
  two questions are asked by different functions.

The invariant that makes it local, added to `Plan::check()`:

* **Scope closure.** No node outside a scope reads a variable the scope
  does not export (`Expr::vars()` of every filter, sort term, key, measure,
  and `on` list, checked against `interface`). The naive builder already
  produces plans that satisfy it (the sub-select's `vars` set is what the
  outside sees); a rule that resolves an inner scan's column from outside
  fails here rather than rendering `t1.object_data->>'x'` for a row that
  is not `t1`'s.

### The lowering target: a relation

```rust
/// A derived table: the body rendered as a statement of its own, joined by
/// column.
Op::Relation {
    body: OpTree,                  // rendered recursively, own alias space
    alias: String,                 // q0, q1, …
    columns: Vec<RelationColumn>,  // one per interface variable
}
pub struct RelationColumn { pub var: String, pub kind: ColumnKind, pub descriptor: TermDescriptor }
pub enum ColumnKind { Identity { class_uri: String }, Slot(BindingSpec), Measure }

/// `Op::Join` gains a key beside the reference edge it has today.
pub enum JoinKey {
    /// `holder.object_data->>'slot' = referenced.asset360_uri`: today's edge.
    Reference(JoinEdge),
    /// `left.<col> = right.<col>`: two identities of the same class, or a
    /// relation column against a star's identity.
    Identity { left: ColumnRef, right: ColumnRef },
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

**What it changes.** Nothing that answers; it makes op 4 applicable to an
optional body. It runs *after* `AbsorbOptionalRead`, `AbsorbOptionalReference`
and `PushLeftJoin` in `tier_one_rules`, so the shapes those already serve
keep the cheaper statement they have (a nullable column beats a derived
table), and the barrier is what a body they declined gets. Rule order buys
a cheaper plan, never a different answer — the order-independence test
(`the_rule_order_does_not_decide_the_fixpoint`) is extended to assert that
reversing the order yields a plan that *lowers to a statement* either way,
and the oracle that its answers agree.

**Would a bad application be caught?** A barrier with the wrong `vars` fails
*scope closure* the moment anything above reads a dropped variable.

### Op 2 — `SinkLiftedCondition` *(new rule; a filter moves into the unit)*

**Match.** `LeftJoin { right: SubSelect(body), condition: Some(c) }` where
every variable of `c` is **certainly bound** in `body`.

**Edit.** `condition = None`; `body := Filter(body, c)`; the obligation
moves from the left join to the new filter.

**Equivalence.** SPARQL §18.5: `LeftJoin(Ω1, Ω2, c) = { merge(μ1, μ2) |
compatible, c(merge) } ∪ { μ1 | no compatible μ2 with c(merge) }`. When
`c` reads only variables Ω2 binds in every solution, `c(merge(μ1, μ2)) =
c(μ2)`, so both sets equal those of `LeftJoin(Ω1, Filter(c, Ω2))`. The
precondition is exactly where this fails: a `c` reading a left-only
variable, or a variable the body binds *optionally* (unbound in some μ2 —
then `c(merge)` may read μ1's binding), is not sinkable.

**Certainly bound** is the one new predicate, and it is reused by op 5:
`Plan::certainly_binds(scope, var)` — bound by a `Required` scan slot, a
scan's identity, an `Unnest` under a required read, a `Values` row with no
`UNDEF`, a group key or measure; not by anything reached through a
`LeftJoin.right`, a `Union` arm, or an `Optional` slot. It is
`mandatorily_feeds` asked of a variable instead of a node.

**What it closes.** 28d "an `OPTIONAL` whose condition is lifted into the
join — served as a fetch": the condition was given up to the residual
because the renderer had no `ON` to put it in. Inside a derived table it is
a `WHERE`, and the ledger claims it honestly. What stays declined is the
case with a real correctness argument against it.

### Op 3 — `NarrowScopeByInterface` *(widening of `FoldMatchesIntoScan`)*

**Match.** Inside a scope that is one side of a `Join` or `LeftJoin` with
`?v ∈ on`, a `match` whose subject is `?v`, where the *other* side's
`Visible::identity_of(?v)` is `Some(class)` and `?v` is certainly bound
there, and the scope has no `rdf:type` match on `?v`.

**Edit.** Fold the matches on `?v` into `Scan { star_var: ?v, class_uri:
class, … }` exactly as `FoldMatchesIntoScan` does when the type is written;
the scan records `narrowed_by_interface: true`.

**Equivalence.** A semi-join reduction: for a natural join on `?v`, rows of
one side whose `?v` no row of the other side carries contribute nothing to
`Join`, and nothing to `LeftJoin` when the restricted side is the *right*
one (a left row with no partner is kept unchanged either way). Restricting
the body to records of `class` drops only rows whose `?v` is not an IRI of
that class — and every `?v` the other side carries is one. Records of the
same URI in another class do not exist (the identifier column is the
table's key), so "identity of `class`" is a set the body's `?v` is either
in or joins nothing. This is the same argument `ValuesNarrowTheJoinedScan`
already makes with a `VALUES` block as the other side; the other side is
now a scan.

**What it declines.** `Minus` and `Union` (a `MINUS` keeps rows with *no*
partner, so shrinking the right side widens the answer); a `?v` the other
side binds optionally; a scope with its own type on `?v` (then the ordinary
fold applies and this rule has nothing to add — and if the two classes
differ, the join is empty in SPARQL and stays empty: the fold does not
unify them).

**Why the body scans the class again rather than reading the outer row.**
Inside the derived table, `?a`'s record is a fresh `Scan` joined back to
the outer `t0` on identity. That is the correct self-join, and Postgres
answers it by index on `asset360_uri`. Rendering the body as `LEFT JOIN
LATERAL (… jsonb_array_elements(t0.object_data->'hasCoveredSection') …)`
instead — reading the outer row directly — is an equivalent statement
(identity is a key, so the self-join is 1:1) and a *renderer* choice the
plan does not have to know about. See cost, and open question 3.

### Op 4 — `PushBarrier` *(new rule; the projection rule, at a scope root)*

**Match.** A `SubSelect` that is `[E]`, whose scope root is itself (no
modifier above it inside the scope — `PushProjection`/`PushGrouping`
inside the scope have already absorbed any), whose `input` is `[S]`, and
every interface variable resolves through the body's `Visible` to a
renderable column (`key_is_readable`, the grouping rule's own test, which
`PushProjection::column_is_readable` already reuses).

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

**Match.** An `[E]` `Join` or `LeftJoin` whose sides are both `[S]`, whose
`on` is one variable `?v`, and where **both** sides resolve `?v` as an
identity — `Visible::identity_of(?v)` is `Some` on each, a scan's or a
relation column's — of the same class, and `?v` is certainly bound on
both.

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

**Certainly bound, again.** A SPARQL join is on *compatible* solutions, and
an unbound `?v` is compatible with everything: a group whose key is unbound
(a key read from an `OPTIONAL`) joins every outer row. SQL's `NULL = x` is
never true. So the rule requires `?v` certainly bound on both sides, and
declines otherwise — the missing-value bucket stays an engine shape when
its key is a join key.

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
`Identity`, on the side the key says, of one class, and `on` is that
variable alone.

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

Op 4: `n4` is `[S]`, `?a` resolves to `identity_of = CEA` inside S1,
`?trackQty` to a measure → `n5 [S]`. Op 5: `n1`'s `Visible` says `?a` is
CEA's identity; `n5`'s columns say `?a` is `Identity(CEA)`; both certainly
bound → `n6 [S]`, key recorded. `PushProjection`: no `Group` **in the outer
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

1. **Op 1** wraps `n10` in `subselect [?cs ?track ?kpFromMeter ?trackName
   ?trackCode ?trackDiscr]` — scope S1, interface everything the body binds.
2. **Op 3** folds `n1`–`n4` into `scan :CEA as ?a` inside S1 (the outer side
   of `n11` binds `?a` as CEA's identity, certainly), with `unnest
   hasCoveredSection as ?cs`, `?cs`'s three reads as element paths
   (`hasCoveredSection[each].isReference` etc.). `FoldMatchesIntoScan` folds
   `n5`–`n7` into `scan :Track as ?track`. `ConstantObjectBecomesFilter`
   makes `isReference = true` a `BoundElement` filter above the unnest.
3. **The element-held edge** lets `PushReferenceJoin` push `n8`'s join on
   `?track` between the element and the Track scan.
4. `AbsorbOptionalRead` absorbs `n9`/`n10` — one match, one single-valued
   slot of `?track`, the existing rule, now inside S1.
5. **Op 4** flips the barrier: every node in S1 is `[S]`, every interface
   variable a column (`?cs` — see open question 5; `?track` an identity;
   the rest slots).
6. **Op 5** pushes `n11` as a `LeftJoin` on `?a`: identity on both sides,
   certainly bound on both (an identity is always bound), `condition:
   None`.
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
  the key. A condition that must not delete the left row cannot reach the
  outer `WHERE`, because scope closure forbids anything outside S1 from
  reading `?cs`. The trap is structurally unreachable rather than avoided
  by placement.
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
| `?s a :Signal . FILTER NOT EXISTS { ?s :ref ?t . ?t a T ; :x ?p ; :y ?q }` | today's `AntiJoin` with the block wrapped by 1 and pushed by 4 — the correlated `NOT EXISTS (SELECT … )` renders the derived table as a subquery | a widening of `PushNotExists` from "exactly one scan" to "a pushed barrier", left as a follow-up row |

What does **not** appear: a rule that mentions `hasCoveredSection`, a count,
an array, a tunnel, or "sub-select joined to the driving variable".

## What stays refused, and says so

Each is a precondition of one op, and the plan printout names the node.

* **A join on a value column** (`?a :code ?c . { SELECT ?c … }`): op 5,
  identity only. Reason: term fidelity of two stored texts.
* **A join key the body binds optionally** (`{ SELECT ?k … WHERE { OPTIONAL
  { ?s :key ?k } } GROUP BY ?k }`): op 5, certainly bound. Reason: an
  unbound key joins everything in SPARQL and nothing in SQL.
* **A lifted condition reading an outer variable** (`OPTIONAL { ?t :len ?l
  FILTER(?l > ?outer) }`): op 2 declines, `PushLeftJoin`/op 5 refuse a
  `LeftJoin` with a condition. As today.
* **A body with an engine node** (a `REGEX` filter inside the sub-select,
  a property path, `GROUP_CONCAT`, `COUNT(DISTINCT *)`, a float `SUM`):
  op 4 does not match; the barrier stays `[E]`, the outer scan is a fetch,
  the engine answers — and for an aggregate the 422 gate stays exactly
  where it is. The message improves for free: the printout shows *which*
  node inside the scope stayed `[E]`.
* **A scope on the right of `MINUS`, or a `UNION` arm**: op 3 declines
  (semi-join reduction is unsound there), op 4 may still fire (a barrier is
  a barrier), and nothing today joins a `Minus` in SQL. Unchanged.
* **A sub-select whose body restricts an identifier slot** (`?s :id "X"`):
  `IdentityIsNotATriple`, inherited.
* **`!BOUND(?relationColumn)`** above a pushed left join: refused as today.

## Tests that would prove equivalence

The oracle is the engine leg (`tests/support.py::force_engine_leg`), which
re-runs the original query over the materialised fetch — the same oracle
every path-B capability carries. Each case asserts its admission
(`used_alone`) so a comparison cannot become one route against itself.

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
   a `GROUP BY` on the identity with `COUNT`/`MIN`/`MAX` or none; placed
   as a mandatory sub-select, an `OPTIONAL` body, or `OPTIONAL { { SELECT
   … } }` — and for each generated query assert (a) `refine` reaches a
   fixpoint with every invariant holding, (b) the plan lowers or names the
   node that stopped it, (c) where it lowers, the statement's answer equals
   the engine's over the seed, row for row under a total order. The grammar
   is small enough to enumerate exhaustively rather than sample.
3. **Invariants driven by a bad rule**, as 28d does for the frontier: a
   barrier that exports a variable its body does not bind; a join key on a
   slot column; an identity join with the key recorded on the wrong side;
   a lifted condition sunk although it reads an outer variable. Each must
   fail at the rule, not in a result.
4. **Both routes agree on the shapes that had a cheaper statement**: every
   `OPTIONAL` the absorb rules already serve keeps its plan (the frozen
   `planner_baseline.json` — 114 shapes — is the reviewable diff, and the
   expected diff is *zero* route changes plus the new capabilities named in
   `PATH_B_CAPABILITIES`). Reversing the rule order must still lower to a
   statement with the same answer, which is the composability claim as a
   test.
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

**Code.** One computed `Scope`; `Visible` reading barrier columns; two
one-line anchor changes in the two tail walks; two new rules (ops 1, 2), one
new rule (op 4), one widened rule (op 5, and the element-held edge in
`foreign_key_on`), one widened fold (op 3); `Op::Relation`, `JoinKey`,
`PLAN_CONTRACT` 5; two invariants; the recursive FROM item in
`sql_builder.py` and its `_OpRelation` in `plan_ops.py`. One asset360-rust
release, one pin bump. Roughly the effort of #463 (pepibru GitLab, N and nested reference `OPTIONAL`s) for each of the two shapes,
sharing the first four items.

**Risk.** The renderer recursion is the part with no precedent in this
codebase, and it is where a parameter-order or alias-collision bug would
live; the property suite's row-for-row comparison is what would find it.
The second risk is the one the human named — an op that is really a shape
in disguise — and the "same vocabulary" table above is the standing test
for it: a new row that needs a new op is a reason to reopen this document.

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
2. **Where op 1 sits in `tier_one_rules`.** Assume after `AbsorbOptionalRead`,
   `AbsorbOptionalReference`, `PushLeftJoin`, so served shapes keep their
   cheaper statements and the baseline diff is zero route changes. If the
   reviewer prefers "every `OPTIONAL` body is a barrier, always" for
   uniformity, the cost is a derived table on shapes that had a nullable
   column, and the baseline diff records it. *Blocking for the second MR,
   not the first.*
3. **`LATERAL` versus self-join rendering of a relation whose scan is the
   outer star.** Assume self-join first, `LATERAL` measured afterwards on
   the review app. *Not blocking.*
4. **Does op 3 (narrow the body by the interface class) apply to a
   user-written sub-select without a type?** Assume yes for `Join` and
   `LeftJoin` — the semi-join argument is the same as for an `OPTIONAL`
   body. *Not blocking; it only widens what folds.*
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

## Staging

Three MRs, each shippable and each parity-tested; the first serves #466
on its own.

1. **Scope, barrier, identity join** — `Scope`, scope-aware `Visible`, the
   two anchor changes, ops 4 and 5, `Op::Relation` + `JoinKey`, contract 5,
   the two invariants, the recursive FROM item. Serves a user-written
   sub-select (grouped or top-N) joined on an identity: #466, and the first,
   second and fifth rows of the table.
2. **The `OPTIONAL` body as a scope** — ops 1, 2, 3. Serves the two-read
   `OPTIONAL` over a reference and the lifted condition: the third and
   fourth rows.
3. **The element-held edge** — the `foreign_key_on` widening and the
   `BoundElement` edge spelling on the statement route, plus whatever
   question 6 decides. Serves #464.

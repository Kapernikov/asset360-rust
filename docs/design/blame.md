Blame/Provenance Design
=======================

1. Goal
-------
- Provide provenance ("blame") for every node in a LinkMLValue tree after applying a sequence of change stages.
- Each stage consists of metadata (asset360-specific) and a list of deltas to apply.
- The final result should enable, in Python, a natural API to query provenance directly from a value view (e.g., `value.provenance()`).

2. Design Considerations
------------------------
- Keep rust-linkml-core neutral and reusable:
  - Core adds stable NodeIds to all LinkMLValue nodes.
  - Core changes patch/apply to always return a trace (added/deleted/updated NodeIds).
  - Core does NOT know about asset360 metadata.
- Implement provenance in asset360:
  - asset360 maps NodeId → Asset360ChangeMeta (“last writer wins”).
  - Provide a simple Python helper `get_blame_info(value, blame_map)` that looks up the current node’s NodeId in the blame map and returns the stage metadata (or None).
- NodeId generation must be cheap and predictable:
  - Use a global atomic counter (u64) to assign NodeIds to created nodes.
  - Preserve NodeIds for unchanged nodes; assign fresh NodeIds for newly created/replaced nodes.
- Large subtree replacements are common and acceptable:
  - When a delta creates a whole subtree, the trace will contain all NodeIds for that subtree in `added`.
  - Assign the stage’s metadata to all of them (O(size_of_subtree)).
- Python ergonomics:
  - LinkMLValue exposes a read-only `node_id` integer (u64 in Rust, int in Python).
  - Blame is carried separately and attached via a thin view that keeps navigation idiomatic.

3. High-Level Flow
-------------------
1) Start from a base LinkMLValue (or empty/new).
2) For each stage in order: apply the stage’s deltas using core’s patch/apply.
3) The core returns `(new_value, PatchTrace)` where `PatchTrace = { added, deleted, updated }` NodeIds (ints).
4) Asset360 updates its blame map:
   - For every NodeId in `added ∪ updated`, set `blame[node_id] = stage.meta` (last-writer-wins).
   - `deleted` is informational; those NodeIds are gone.
5) Return the final value and blame map; in Python, call `get_blame_info(value, blame_map)` on any LinkMLValue to retrieve that node's provenance.

4. Core Changes (rust-linkml-core)
----------------------------------
- NodeIds
  - Add `NodeId(u64)` to every LinkMLValue node (Scalar/List/Mapping/Object).
  - Assign NodeIds during load/construct and when materializing new nodes in patch.
  - Preserve NodeIds for unchanged nodes; assign fresh ones for created/replaced nodes.
- Cheap generator
  - `static NEXT_ID: AtomicU64 = AtomicU64::new(1);`
  - `fn new_node_id() -> NodeId { NodeId(NEXT_ID.fetch_add(1, Ordering::Relaxed)) }`
- Patch trace
  - Define `PatchTrace { added: Vec<u64>, deleted: Vec<u64>, updated: Vec<u64> }` (NodeIds are ints in Python).
  - Change existing `patch/apply` to return `(LinkMLValue, PatchTrace)`.
  - Implementation:
    - Pre-snapshot: traverse to collect `pre_ids` (HashSet<u64>).
    - Apply deltas (assign NodeIds for any new nodes).
    - Post-snapshot: traverse to collect `post_ids` (HashSet<u64>).
    - `added = post_ids - pre_ids`, `deleted = pre_ids - post_ids`.
    - `updated`: NodeIds that persist across patch and whose content/structure mutated in place (e.g., scalar set, container change). Full subtree replacements should not report `updated` for replaced children; only the parent container (if structurally changed) may be `updated`.
- Python bindings
  - Expose a read-only `node_id` property on LinkMLValue as an int.
  - Change `py_patch/py_apply` to return `(LinkMLValue, {"added": [int], "deleted": [int], "updated": [int]})`.

5. Asset360 Changes (this crate)
---------------------------------
- Metadata and stage types
  - `pub struct Asset360ChangeMeta { /* author, timestamp, ticket, etc. */ }`
  - `pub struct ChangeStage<M> { pub meta: M, pub deltas: Vec<Delta> }`
- Apply with blame
  - `pub fn apply_changes_with_blame(base: Option<LinkMLValue>, stages: Vec<ChangeStage<Asset360ChangeMeta>>, sv: &SchemaView) -> (LinkMLValue, HashMap<u64 /*NodeId*/, Asset360ChangeMeta>)`
  - For each stage:
    - `(value2, trace) = core::patch(value, &stage.deltas, sv)`
    - For every id in `trace.added` and `trace.updated`: `blame.insert(id, stage.meta.clone())`
    - `value = value2`
- Python API
  - Provide `apply_changes_with_blame(stages, sv, base=None) -> (value, blame_map)`.
  - Provide `get_blame_info(value, blame_map) -> dict | None` that returns the stage metadata dict for `value.node_id` (an int), or `None` if absent.
  - Metadata dicts are created by serializing the Rust struct via `serde_json` into Python objects.

6. Semantics and Edge Cases
---------------------------
- Last-writer-wins: if multiple stages write the same node, the later stage’s metadata overwrites prior blame.
- Full subtree replacement: children of the replaced subtree appear in `added`; all those NodeIds get the stage’s metadata. The old subtree’s NodeIds appear in `deleted` and are discarded. The parent container may be listed in `updated` if its structure changed.
- In-place container change (list append/pop, object add/remove field): container NodeId appears in `updated`.
- Pure scalar change: target scalar NodeId appears in `updated`.

7. Performance Notes
--------------------
- NodeId assignment is a single relaxed atomic increment per created node (very cheap).
- Trace computation uses two traversals per patch call (pre and post), linear in tree size.
- If needed later, we can optimize updated detection per-delta or offer a compact blame mode (record only at subtree roots with a parent map), without changing the Python-facing API.
 - NodeIds are ints in Python, avoiding string formatting/allocations on hot paths.

8. Alternatives Considered
--------------------------
- Type parameter on `LinkMLValue<M>`: strong typing but cascades generics through core and complicates Python; rejected for simplicity.
- Trait-object metadata in core: leaks object-safety / PyO3 concerns into core; better to keep metadata mapping in asset360.
- Path-only blame: workable short-term but NodeIds are more robust for lists/moves.

9. Migration & Compatibility
----------------------------
- Core: `patch/apply` return type changes; Python binding changes accordingly.
- asset360: implements new `apply_changes_with_blame` and `get_blame_info`; callers adapt to the new return values.

---
This document captures the agreed architecture to deliver provenance with minimal core impact, high performance, and an idiomatic Python API.

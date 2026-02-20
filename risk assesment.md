# StateFabric Risk Assessment and Mitigation Plan

Last updated: 2026-02-19

## Scope
This document maps the 20 identified risks to concrete fixes, with immediate mitigations and structural changes required for production safety.

## Priority Gates
- `P0 (must fix before any production use)`: 1, 2, 3, 4, 5, 6, 9, 10, 15
- `P1 (must fix before multi-node GA)`: 7, 8, 11, 12, 13, 14, 16, 17, 19
- `P2 (must fix before enterprise rollout)`: 18, 20

## Critical Risks (P0)

### 1. Data Loss Window (Durability Gap)
- Immediate fix:
  - Make write acknowledgement dependent on durable append (`appendOperation` must complete before success is returned).
  - Add crash-safe write-ahead log segments (JSONL or binary) with checksum + atomic segment manifest update.
- Structural fix:
  - Batch writes by size/time with explicit `flush()` and durability SLA (`max_unflushed_ops`, `max_unflushed_ms`).
  - On startup, replay all committed segments since last snapshot.
- Acceptance criteria:
  - Kill-process and timeout fault tests show zero acknowledged-write loss.

### 2. PN-Counter Can Go Negative
- Immediate fix:
  - Do not use eventual PN-counter for balances/inventory; mark these as `strong` only.
  - Remove or hard-warning all examples that imply safe non-negative decrements under eventual consistency.
- Structural fix:
  - Implement strong-consistency counters (Raft or equivalent) for bounded resources.
  - If eventual is required, use escrow/token-reservation CRDT design with explicit oversell bounds.
- Acceptance criteria:
  - Concurrency tests prove no negative inventory/balance in strong mode.

### 3. Unbounded Tombstone Growth in OR-Set
- Immediate fix:
  - Add tombstone budget limits and compaction trigger thresholds.
  - Snapshot compaction must rewrite set state and drop causally stable tombstones.
- Structural fix:
  - Introduce causal stability frontier (version vectors or equivalent) and safe tombstone GC.
  - Add telemetry for `tombstone_count`, `gc_cycles`, `gc_reclaimed`.
- Acceptance criteria:
  - Long-run soak tests show bounded memory growth for churn-heavy sets.

### 4. Misleading Transaction Semantics
- Immediate fix:
  - Rename eventual `transaction` API to `batch` or `bestEffortBatch`.
  - Add runtime warning/exception for unsafe patterns like read-check-write in eventual namespaces.
- Structural fix:
  - Expose two explicit APIs:
    - `eventual.*` (no atomicity guarantees)
    - `strong.*` (linearizable; coordination required)
  - Provide safe financial/inventory examples only on strong APIs.
- Acceptance criteria:
  - Docs and SDK types cannot be misread as ACID for eventual operations.

## Serious Risks (P0/P1)

### 5. HLC Clock Skew Vulnerability (P0)
- Immediate fix:
  - Reject/quarantine timestamps beyond a configured future skew bound (for example 2-5 minutes).
  - Log and meter skew violations per node.
- Structural fix:
  - Require authenticated node identity on replication messages.
  - Add monotonic correction logic and drift alarms.
- Acceptance criteria:
  - Poisoned timestamp simulation cannot permanently bias conflict resolution.

### 6. S3 Operation Log Cost and Latency (P0)
- Immediate fix:
  - Batch many operations into one object (segment files), not one PUT per op.
  - Parallelize recovery reads with bounded concurrency.
- Structural fix:
  - Use rolling snapshots + segment retention policy + compaction.
  - Optionally support lower-latency append backends (Kinesis, DynamoDB streams, Kafka) behind adapter interface.
- Acceptance criteria:
  - Cost and recovery latency benchmarks meet target at projected write rates.

### 7. Gossip Without Membership Management (P1)
- Immediate fix:
  - Disable P2P gossip in serverless mode; use shared durable log synchronization first.
- Structural fix:
  - Add membership service (heartbeats, TTL expiry, join/leave, failure detection) using DynamoDB/Redis/etcd.
  - Define peer discovery bootstrap contract.
- Acceptance criteria:
  - New ephemeral instances converge without static peer configuration.

### 8. No Conflict Visibility (P1)
- Immediate fix:
  - Emit structured conflict events with key, local state, remote state, resolved state, reason.
- Structural fix:
  - Add sampling, tracing correlation IDs, and conflict dashboards.
  - Provide optional callback hooks per CRDT type.
- Acceptance criteria:
  - Operators can inspect what conflict happened and why a value won.

## Moderate Risks (P0/P1)

### 9. CRDTMap Merge Bug (P0)
- Immediate fix:
  - Always merge child CRDT values when both sides have the same key.
  - Never overwrite a newer/older child CRDT solely by parent timestamp.
- Structural fix:
  - Add property-based tests for commutativity, associativity, idempotence.
  - Add regression tests for concurrent nested-map updates.
- Acceptance criteria:
  - Concurrent nested updates converge without data loss.

### 10. OR-Set Merge Bug (P0)
- Immediate fix:
  - Merge tombstones from both replicas first, then filter element tags.
  - Add delete-resurrection regression tests.
- Structural fix:
  - Add invariant checks in CI for OR-Set semantics.
- Acceptance criteria:
  - Deleted elements do not reappear after any merge ordering.

### 11. Rate Limiter Example Broken (P1)
- Immediate fix:
  - Replace global growing counter example with time-windowed key buckets plus TTL/GC.
- Structural fix:
  - Provide production-safe patterns:
    - eventual approximate limiter (windowed CRDT),
    - strict limiter on strong key.
- Acceptance criteria:
  - Counter state remains bounded and limiter recovers each window.

### 12. Memory Budget Unrealistic (P1)
- Immediate fix:
  - Enforce runtime memory budget with working-set loading and LRU eviction.
  - Add per-CRDT memory accounting.
- Structural fix:
  - Lazy namespace loading, hot/cold tiering, optional compressed cold segments.
- Acceptance criteria:
  - 256MB and 512MB profiles run without OOM at target workload.

### 13. No Schema Evolution Strategy (P1)
- Immediate fix:
  - Add schema version metadata and compatibility policy to snapshot/log formats.
- Structural fix:
  - Implement migration registry and rolling upgrade tooling (read-old/write-new).
- Acceptance criteria:
  - N-1 to N upgrades succeed without manual data rewrite.

### 14. Security Model Incomplete (P1)
- Immediate fix:
  - Default-deny namespace policy and signed identity requirement.
  - Enforce SDK-side authorization hooks for every read/write API.
- Structural fix:
  - Integrate IAM/OIDC/JWT validation, policy engine, audit logs.
- Acceptance criteria:
  - Unauthorized namespace access is blocked and auditable.

### 15. `compareAndSwap` Requires Coordination (P0)
- Immediate fix:
  - Remove CAS from eventual register interface.
  - Keep CAS only on strong/linearizable register APIs.
- Structural fix:
  - Type-level API split to prevent accidental use (`EventualRegister` vs `StrongRegister`).
- Acceptance criteria:
  - No eventual API surface implies linearizable semantics.

## Minor and Design Risks (P1/P2)

### 16. No Backpressure on Gossip (P1)
- Immediate fix:
  - Add max payload size, max in-flight bytes, and retry backoff.
- Structural fix:
  - Credit-based flow control and chunked delta transfer.
- Acceptance criteria:
  - Lagging nodes do not cause memory spikes or retransmit storms.

### 17. Dual Ordering (UUIDv7 vs HLC) (P1)
- Immediate fix:
  - Define one canonical ordering for conflict resolution (HLC + node ID + counter).
  - Use UUIDv7 only as unique identifier, not ordering source.
- Structural fix:
  - Add ordering consistency checks in replay and merge tests.
- Acceptance criteria:
  - Replays are deterministic under clock skew and concurrent writes.

### 18. No Encryption at Rest (P2)
- Immediate fix:
  - Enable SSE-KMS by default for all S3 objects.
- Structural fix:
  - Add optional client-side envelope encryption for high-sensitivity namespaces.
- Acceptance criteria:
  - All persisted artifacts are encrypted and key rotation is documented/tested.

### 19. `process.exit(0)` in Lifecycle Hooks (P1)
- Immediate fix:
  - Replace with graceful async shutdown (`await flush(); await close(); return`).
- Structural fix:
  - Use `AbortSignal` deadlines and idempotent shutdown handlers.
- Acceptance criteria:
  - Runtime exits without truncating writes or bypassing platform cleanup.

### 20. 15-Month MVP Roadmap Risk (P2)
- Immediate fix:
  - Narrow MVP scope to safe subset:
    - eventual CRDTs with explicit warnings,
    - no misleading transaction/CAS APIs,
    - fixed merge correctness and durable logging.
- Structural fix:
  - Rebaseline roadmap into value slices with kill criteria and competitive checkpoints.
- Acceptance criteria:
  - MVP ships with clear guarantees and no known unsafe default behavior.

## Recommended Execution Sequence
1. Correctness blockers: 9, 10, 15, 4
2. Durability and ordering safety: 1, 5, 6
3. Resource safety: 3, 11, 12, 19
4. Distributed operability: 7, 8, 16, 17
5. Governance and enterprise readiness: 13, 14, 18, 20

## Release Gate Checklist (Go/No-Go)
- All P0 items closed with tests.
- Fault-injection suite passes (crash, timeout, skew, partition, replay).
- Documentation clearly separates eventual vs strong guarantees.
- Cost and memory benchmarks published for target serverless profiles.

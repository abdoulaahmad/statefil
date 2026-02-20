# StateFabric Full Architecture Blueprint

Last updated: 2026-02-19
Status: implementation-ready reference architecture

## 1. Purpose
Define the complete target architecture for StateFabric with explicit safety boundaries, consistency contracts, and production controls.

This architecture is aligned with:
- `StateFabric-Technical-Whitepaper.md`
- `EXECUTION-PLAYBOOK.md`
- `risk assesment.md`

## 2. Architecture Principles
1. Local-first state access for hot-path latency.
2. Durable-before-acknowledge writes.
3. Explicit consistency domains: eventual vs strong.
4. Deterministic convergence for eventual CRDT state.
5. Production safety first: correctness, durability, observability, security.

## 3. System Context

```
Application Function
  -> StateFabric SDK
      -> Local State Engine (CRDT + cache + query)
      -> Durable Log Adapter (segment append + snapshots)
      -> Replication Engine (mode-specific)
      -> Optional Strong Consistency Engine (Raft/consensus keys)
      -> Security + Policy + Audit
      -> Observability (metrics/logs/traces/events)
```

## 4. Top-Level Logical Architecture

```
+-----------------------------------------------------------------------+
|                            StateFabric Runtime                        |
+-----------------------------------------------------------------------+
| SDK/API Layer                                                         |
| - Namespace + CRDT APIs                                               |
| - Eventual API and Strong API                                         |
+-----------------------------------------------------------------------+
| State Engine                                                          |
| - Namespace registry                                                   |
| - CRDT instances                                                       |
| - Merge engine                                                         |
| - Working-set manager (memory budget + eviction)                       |
+-----------------------------------------------------------------------+
| Write Pipeline                                                        |
| - Operation builder + canonical ordering key                           |
| - Validation + authz                                                   |
| - Durable segment append (sync ack)                                    |
| - In-memory apply                                                      |
| - Replication enqueue                                                  |
+-----------------------------------------------------------------------+
| Persistence                                                           |
| - Segment log store                                                    |
| - Snapshot manager                                                     |
| - Compaction + retention                                               |
| - Schema version + migration registry                                  |
+-----------------------------------------------------------------------+
| Replication                                                           |
| - S3/object-log sync mode (serverless default)                         |
| - Gossip mode (cluster mode, requires membership)                      |
| - Conflict event emitter                                               |
+-----------------------------------------------------------------------+
| Strong Consistency (optional but required for critical keys)          |
| - Consensus group                                                      |
| - Strong key registry                                                  |
| - CAS/transactions for strong namespaces                               |
+-----------------------------------------------------------------------+
| Security + Operations                                                 |
| - Identity/authn/authz                                                 |
| - Encryption at rest/in transit                                        |
| - Metrics, tracing, audit                                              |
+-----------------------------------------------------------------------+
```

## 5. Consistency Model (Hard API Boundary)

### 5.1 Eventual API
- CRDT-based.
- Convergent but non-linearizable.
- No CAS.
- No ACID transaction claims.
- Suitable for carts, preferences, collaborative metadata, analytics counters.

### 5.2 Strong API
- Consensus-backed.
- Linearizable per strong namespace/key group.
- Supports CAS and strict transaction semantics.
- Required for balances, inventory decrements, uniqueness constraints.

### 5.3 API Surface Split

```ts
interface StateFabric {
  eventual: {
    counter(ns: string, key: string): EventualCounter;
    set<T>(ns: string, key: string): EventualSet<T>;
    register<T>(ns: string, key: string): EventualRegister<T>;
    map(ns: string, key: string): EventualMap;
    batch<T>(fn: () => T | Promise<T>): Promise<T>;
  };

  strong: {
    counter(ns: string, key: string): StrongCounter;
    register<T>(ns: string, key: string): StrongRegister<T>;
    map(ns: string, key: string): StrongMap;
    transaction<T>(fn: () => T | Promise<T>): Promise<T>;
  };
}
```

## 6. Data Architecture

### 6.1 Canonical Operation Envelope

```ts
interface OperationEnvelope {
  opId: string; // UUIDv7 only for identity
  actor: { nodeId: string; processId: string };
  hlc: { physical: number; logical: number; nodeId: string }; // canonical ordering
  sequence: number; // per-node monotonic counter
  namespace: string;
  key: string;
  crdtType: string;
  operation: { type: string; payload: unknown };
  dependencies?: string[];
  schemaVersion: string;
  signature?: string;
}
```

Ordering rule:
- Conflict and replay ordering use `(hlc, nodeId, sequence)`.
- `opId` is never the source of ordering truth.

### 6.2 Snapshot Format

```ts
interface SnapshotV1 {
  version: "1.0";
  snapshotId: string;
  createdAt: string;
  checkpoint: {
    maxHlc: { physical: number; logical: number; nodeId: string };
    vectorClock: Record<string, number>;
    lastSegmentId: string;
  };
  state: Record<string, unknown>;
  metadata: {
    compression: "none" | "zstd";
    checksum: string;
    encrypted: boolean;
    kmsKeyId?: string;
  };
}
```

### 6.3 Schema Evolution
- Every persisted artifact carries version.
- Migration registry supports read-old/write-new.
- Upgrade policy: runtime N can read N and N-1.

## 7. Write and Read Pipelines

### 7.1 Write Pipeline (durable-before-ack)
1. Build operation envelope.
2. Validate API constraints by consistency domain.
3. Authorize namespace/key access.
4. Append to durable segment log.
5. Apply to local in-memory CRDT.
6. Emit conflict/merge events if applicable.
7. Enqueue replication.
8. Return success.

### 7.2 Read Pipeline
1. Read from working-set memory.
2. If missing, lazy-load namespace from snapshot + segments.
3. Apply pending deltas.
4. Return value and staleness metadata.

## 8. Replication Architecture

### 8.1 Mode A: Serverless Default (object-log synchronization)
- No direct peer gossip requirement.
- Writers append to shared durable segments.
- Readers rebuild from latest snapshot + subsequent segments.
- Bounded parallel fetch on recovery.

### 8.2 Mode B: Cluster Replication (gossip + membership)
Required modules:
- Membership registry (join/leave/heartbeat/TTL).
- Peer discovery bootstrap.
- Anti-entropy sync using vector clocks.
- Delta exchange with backpressure.

Backpressure controls:
- `maxDeltaBytes`
- `maxInFlightBytes`
- `maxConcurrentSyncs`
- retry with exponential backoff + jitter

## 9. CRDT Architecture and Invariants

### 9.1 Supported CRDTs
- G-Counter
- PN-Counter (eventual use only for non-critical values)
- OR-Set with tombstone GC
- LWW-Register
- CRDT-Map (recursive child merge)

### 9.2 Mandatory invariants
- Commutative merge.
- Associative merge.
- Idempotent merge.
- No delete resurrection for OR-Set.
- Nested CRDT updates converge for CRDT-Map.

### 9.3 OR-Set tombstone management
- Track causal stability frontier.
- GC only tombstones below stable frontier.
- Compaction rewrite on snapshot.
- Alert on tombstone budget pressure.

## 10. Strong Consistency Subsystem

### 10.1 Strong key registry
Namespace or key patterns marked as `strong`:
- `payment:*`
- `inventory:*`
- `identity:unique:*`

### 10.2 Guarantees
- Linearizable reads/writes.
- CAS correctness.
- Transaction atomicity only in strong domain.

### 10.3 Failure behavior
- If consensus unavailable, strong operations fail fast.
- Eventual fallback is forbidden for strong keys.

## 11. Durability and Recovery

### 11.1 Segment log model
- Append-only segment objects.
- Segment manifest atomically advanced after checksum verification.
- Retention policy: active + recent + compacted archive.

### 11.2 Snapshot and compaction
- Trigger by operation count, elapsed time, and memory pressure.
- Snapshot includes checkpoint and schema version.
- Compaction drops obsolete segment history after safe checkpoint.

### 11.3 Recovery algorithm
1. Load latest valid snapshot.
2. Verify checksum/decrypt/decompress.
3. Load segments newer than snapshot checkpoint.
4. Replay in canonical order.
5. Start replication and mark ready.

## 12. Memory Architecture

Working-set model for serverless:
- Strict memory budget configuration.
- Lazy namespace loading.
- LRU eviction to cold storage.
- Per-CRDT and per-namespace memory accounting.

Target profiles:
- 256MB runtime profile.
- 512MB runtime profile.
- 1024MB runtime profile.

## 13. Security Architecture

### 13.1 Identity and transport
- mTLS for node-to-node cluster traffic.
- Signed replication messages.
- OIDC/JWT or IAM-derived principal identity for SDK calls.

### 13.2 Authorization
- Default-deny policy model.
- Namespace/key level allow rules.
- Mandatory authorization check on every read/write/admin path.

### 13.3 Encryption
- In transit: TLS.
- At rest: SSE-KMS by default.
- Optional client-side envelope encryption for sensitive namespaces.

### 13.4 Auditability
- Append immutable audit event per policy decision and write.
- Include principal, action, namespace/key, result, requestId.

## 14. Observability Architecture

### 14.1 Core metrics
- `write_ack_latency_ms`
- `durable_append_latency_ms`
- `recovery_time_ms`
- `replication_lag_ops`
- `state_memory_bytes`
- `tombstone_count`
- `conflict_events_total`
- `clock_skew_reject_total`

### 14.2 Structured events
- Conflict resolution events with before/after values.
- HLC skew rejection events.
- Compaction events with reclaimed bytes.

### 14.3 Trace spans
- `state.write`
- `state.persist.append`
- `state.recover`
- `state.replicate.sync`
- `state.strong.transaction`

## 15. Reliability and Fault Model

Covered failures:
- Process crash.
- Function timeout.
- OOM kill.
- Network partitions.
- Delayed/duplicated replication messages.
- Clock skew and poisoned timestamps.

Resilience strategies:
- Durable-before-ack write path.
- Replay-safe idempotent operations.
- Quarantine out-of-bounds timestamps.
- Retry with jitter and bounded concurrency.

## 16. Deployment Architecture

### 16.1 Single-region serverless (recommended initial production)
- Object storage as source of truth.
- No gossip dependency.
- Optional strong subsystem for critical namespaces only.

### 16.2 Cluster mode
- Stateful nodes with membership service.
- Gossip replication enabled.
- Strong subsystem colocated or external consensus service.

### 16.3 Multi-region
- Region-local snapshots/segments.
- Inter-region replication policies by namespace.
- Explicit RPO/RTO per namespace class.

## 17. Implementation Work Packages

Execution method:
- All work packages must use SF-TDD as defined in `SF-TDD-WORKFLOW.md`.

### WP1: Correctness core (P0)
- Fix CRDT-Map recursive merge.
- Fix OR-Set tombstone merge ordering.
- Remove CAS from eventual register.
- Rename eventual transaction to batch.

### WP2: Durable write path (P0)
- Segment append before ack.
- Crash recovery replay tests.
- Snapshot checkpoint + retention.

### WP3: Time and ordering safety (P0)
- HLC drift bounds and reject telemetry.
- Canonical ordering enforcement in replay.

### WP4: Cost and scale baseline (P0/P1)
- Batched segment writes.
- Parallel bounded reads on recovery.
- Memory budget and lazy load.

### WP5: Operability and visibility (P1)
- Conflict event stream.
- Backpressure controls.
- Full metrics/traces.

### WP6: Security and governance (P1/P2)
- IAM/OIDC integration.
- Policy engine.
- SSE-KMS defaults and audit logs.

### WP7: Strong consistency subsystem (P1/P2)
- Consensus integration.
- Strong key registry.
- Strong transactions and CAS.

## 18. Architecture Decision Records (Initial)

ADR-001:
- Decision: split API into explicit `eventual` and `strong`.
- Why: avoid misleading semantics and unsafe usage.

ADR-002:
- Decision: canonical ordering uses HLC + nodeId + sequence.
- Why: prevent UUID-order ambiguity.

ADR-003:
- Decision: serverless default uses object-log sync, not gossip.
- Why: avoids membership/discovery dependency for ephemeral compute.

ADR-004:
- Decision: durable append required before acknowledging writes.
- Why: closes acknowledged-write loss window.

## 19. Release Gates

Gate A (alpha):
- WP1 complete with regression/property tests.
- Durable write path implemented and fault-tested.

Gate B (beta):
- Recovery performance targets met.
- Memory profiles validated on 256MB and 512MB.
- Conflict visibility and skew protections active.

Gate C (GA):
- Security controls enforced end-to-end.
- P0/P1 checklist in `risk assesment.md` closed.
- Strong consistency subsystem available for critical namespaces.

## 20. Open Architecture Questions
1. Should strong subsystem be embedded or external managed consensus?
2. What default replication mode should be auto-selected per deployment target?
3. Which namespace classes require cross-region active-active vs active-passive?
4. What is the target RPO/RTO per namespace class?

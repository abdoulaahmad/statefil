# StateFabric Technical Implementation Details

Last updated: 2026-02-19
Source baseline: `FULL-ARCHITECTURE.md`
Roadmap reference: `IMPLEMENTATION-ROADMAP.md`

## 1. Runtime Components

### 1.1 Core modules
- `Engine`: namespace registry, CRDT instances, in-memory apply/query.
- `DurabilityManager`: durable append, segment manifest, snapshot/compaction.
- `ReplicationManager`: serverless sync mode and cluster gossip mode.
- `PolicyManager`: authn/authz checks for all API calls.
- `ObservabilityManager`: metrics, traces, structured events.
- `StrongConsistencyManager`: consensus-backed operations for strong namespaces.

### 1.2 Package layout
```text
packages/
  core/
    src/engine/
    src/crdt/
    src/log/
    src/persistence/
    src/replication/
    src/security/
    src/observability/
  sdk/
    src/client.ts
    src/types/
  persistence-s3/
    src/adapter.ts
  persistence-local/
    src/adapter.ts
```

## 2. Public Interfaces

### 2.1 Main runtime
```ts
export interface StateFabricRuntime {
  eventual: EventualApi;
  strong: StrongApi;
  admin: AdminApi;
  start(): Promise<void>;
  stop(signal?: AbortSignal): Promise<void>;
}
```

### 2.2 Eventual API
```ts
export interface EventualApi {
  counter(ns: string, key: string): EventualCounter;
  set<T>(ns: string, key: string): EventualSet<T>;
  register<T>(ns: string, key: string): EventualRegister<T>;
  map(ns: string, key: string): EventualMap;
  batch<T>(fn: () => T | Promise<T>): Promise<T>;
}
```

### 2.3 Strong API
```ts
export interface StrongApi {
  counter(ns: string, key: string): StrongCounter;
  register<T>(ns: string, key: string): StrongRegister<T>;
  map(ns: string, key: string): StrongMap;
  transaction<T>(fn: () => T | Promise<T>): Promise<T>;
}
```

## 3. Data Contracts

### 3.1 Operation envelope
```ts
export interface OperationEnvelope {
  opId: string;
  actor: { nodeId: string; processId: string };
  hlc: { physical: number; logical: number; nodeId: string };
  sequence: number;
  namespace: string;
  key: string;
  consistency: "eventual" | "strong";
  crdtType: "g-counter" | "pn-counter" | "or-set" | "lww-register" | "crdt-map";
  operation: { type: string; payload: unknown };
  schemaVersion: string;
  checksum: string;
}
```

### 3.2 Segment manifest
```ts
export interface SegmentManifest {
  version: "1.0";
  activeSegmentId: string;
  committedSegments: string[];
  lastCommittedSequenceByNode: Record<string, number>;
  updatedAt: string;
}
```

### 3.3 Snapshot metadata
```ts
export interface SnapshotMetadata {
  snapshotId: string;
  version: "1.0";
  createdAt: string;
  maxHlc: { physical: number; logical: number; nodeId: string };
  lastSegmentId: string;
  compression: "none" | "zstd";
  encrypted: boolean;
  checksum: string;
}
```

## 4. Write Path (Eventual)

### 4.1 Required order
1. Build operation envelope with HLC and per-node sequence.
2. Validate request by consistency rules.
3. Authorize principal for namespace/key/action.
4. Append operation to durable segment.
5. Apply operation in-memory.
6. Emit conflict event if merge needed.
7. Queue replication.
8. Return success.

### 4.2 Non-negotiable guarantee
- No write is acknowledged before durable append succeeds.

## 5. Recovery Path

1. Read latest valid snapshot.
2. Verify checksum, decrypt/decompress.
3. Load committed segments after snapshot checkpoint.
4. Replay operations in canonical order: `(hlc, nodeId, sequence)`.
5. Rebuild indexes and vector clocks.
6. Start replication and set node `ready=true`.

## 6. CRDT Rules

### 6.1 CRDT-Map
- If same map key exists on both replicas and values are CRDTs, merge child CRDTs.
- Never overwrite child state only because parent timestamp is newer.

### 6.2 OR-Set
- Merge tombstones from both sides first.
- Then merge element tags filtered by merged tombstones.
- Run tombstone GC only below causal stability frontier.

### 6.3 PNCounter
- Eventual PN-counter is not valid for strict non-negative invariants.
- Financial/inventory operations must be strong consistency only.

## 7. Replication Modes

### 7.1 Serverless mode (default)
- Object-storage segment sync.
- No peer membership dependency.
- Bounded-parallel segment fetch during recovery.

### 7.2 Cluster mode
- Requires membership service:
  - heartbeat interval
  - node TTL
  - join/leave protocol
- Anti-entropy sync with vector clocks.
- Backpressure limits:
  - `maxDeltaBytes`
  - `maxInFlightBytes`
  - `maxConcurrentSyncs`

## 8. Clock and Ordering Safety

- HLC rejects timestamps exceeding max future skew.
- Rejected timestamps increment `clock_skew_reject_total`.
- UUIDv7 is identifier only, not ordering source.

## 9. Security Requirements

- Default deny policy for all namespaces.
- Mandatory authz on read/write/admin.
- mTLS and signed replication messages in cluster mode.
- SSE-KMS at rest for all persisted objects.
- Immutable audit events for policy decisions and writes.

## 10. Performance Targets (Initial)

- Hot read p50: < 1ms
- Hot write p50 (eventual): < 5ms excluding remote replication
- Durable append p95: project-specific target, tracked continuously
- Recovery: bounded by snapshot size + segment count, must publish benchmark

## 11. Required Test Matrix

### 11.1 Correctness
- CRDT merge invariants:
  - commutative
  - associative
  - idempotent
- Regression tests:
  - OR-Set delete resurrection
  - CRDT-Map nested merge
  - eventual API has no CAS

### 11.2 Fault injection
- Crash during write.
- Timeout before snapshot.
- Replay after partial segment write.
- Poisoned future timestamp input.
- Partition and delayed duplicate deltas.

### 11.3 Security
- Unauthorized namespace read/write denied.
- Tampered replication message rejected.
- Encrypted persistence validated.

## 12. SF-TDD Delivery Standard

SF-TDD means every feature follows:
1. Scaffold contracts and file structure.
2. Write failing tests against those contracts.
3. Implement minimum logic to pass.
4. Refactor safely while tests stay green.
5. Add non-happy-path and fault tests before close.

### 12.1 Scaffold-first rules
- Create interfaces, types, module boundaries, and TODO stubs first.
- No production logic in scaffold commit.
- Scaffold must compile and be importable by tests.

### 12.2 Test-first rules
- Write tests immediately after scaffold, before implementation.
- Start with contract tests (public API behavior), then invariants.
- For CRDTs, include merge algebra tests (commutative, associative, idempotent).

### 12.3 Done criteria per task
- Contract tests pass.
- Regression tests exist for discovered bug class.
- Fault test added when the task touches durability/replication/time.
- Metrics or events added when behavior is operationally significant.

### 12.4 Commit pattern
1. `scaffold: <module>`
2. `test: <module> contracts/invariants`
3. `feat: <module> minimal pass`
4. `refactor: <module> + keep green`

## 13. Configuration Baseline

```ts
export interface RuntimeConfig {
  mode: "serverless" | "cluster";
  memoryBudgetMb: number;
  snapshotEveryOps: number;
  snapshotEveryMs: number;
  maxFutureClockSkewMs: number;
  maxDeltaBytes: number;
  maxInFlightBytes: number;
  maxConcurrentSyncs: number;
  encryption: { enabled: boolean; kmsKeyId?: string };
}
```

Recommended defaults (initial):
- `mode=serverless`
- `memoryBudgetMb=256`
- `snapshotEveryOps=1000`
- `snapshotEveryMs=60000`
- `maxFutureClockSkewMs=120000`

## 14. Implementation Sequence (SF-TDD)

1. Correctness and API safety:
  - Scaffold interfaces and stubs
  - Write failing contract + invariant tests
  - Implement CRDT-Map merge fix
  - Implement OR-Set merge fix
  - Remove eventual CAS
  - Rename eventual transaction to batch
2. Durable path:
  - Scaffold log/manifest contracts
  - Write crash/replay tests first
  - Implement append-before-ack
  - Implement segment manifest + recovery replay
3. Safety and scale:
  - Scaffold safety configs and interfaces
  - Write skew/load/memory tests first
  - Implement HLC skew bounds
  - Implement batched segments
  - Implement lazy loading and memory budget controls
4. Operational hardening:
  - Scaffold event/metric contracts
  - Write observability/security tests first
  - Implement conflict events
  - Implement metrics/traces
  - Implement security enforcement

# StateFabric Execution Playbook

**Version 2.0 | February 2026**

---

## Executive Summary

**Goal:** Build a portable, log-based distributed state runtime that provides low-latency local state access for serverless compute with automatic S3 persistence.

**Core Value:** Make stateless compute stateful without the database tax.

**Approach:** Local-first → Cloud-later (Git → GitHub model)

---

## Priority Gates

All 20 identified risks are categorized by priority:

| Priority | Risks | Must Fix Before |
|----------|-------|-----------------|
| **P0** | 1, 2, 3, 4, 5, 6, 9, 10, 15 | Any production use |
| **P1** | 7, 8, 11, 12, 13, 14, 16, 17, 19 | Multi-node GA |
| **P2** | 18, 20 | Enterprise rollout |

### P0 Risk Checklist (Must complete before Phase 1 ends)

- [ ] **#1 Data Loss:** Sync log append, crash-safe WAL
- [ ] **#2 PN-Counter Negative:** BoundedCounter, remove unsafe examples
- [ ] **#3 Tombstone Growth:** GC thresholds, causal stability
- [ ] **#4 Misleading Transactions:** Rename to `batch`, add warnings
- [ ] **#5 HLC Clock Skew:** Max drift bounds (5 min)
- [ ] **#6 S3 Cost/Latency:** Batched appends, parallel recovery
- [ ] **#9 CRDTMap Merge:** Recursive merge, not overwrite
- [ ] **#10 OR-Set Merge:** Fix tombstone merge order
- [ ] **#15 compareAndSwap:** Remove from eventual API

---

## Part 1: Project Foundation

### 1.1 Technology Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Language** | TypeScript | Best for serverless (Lambda, GCF, Azure Functions all support Node.js) |
| **Runtime** | Node.js 20+ | LTS, wide compatibility |
| **Package Manager** | pnpm | Fast, efficient, good monorepo support |
| **Build Tool** | tsup | Simple, fast TypeScript builds |
| **Testing** | Vitest | Fast, TypeScript-native |
| **Storage** | S3-compatible | Universal across clouds |

### 1.2 Repository Structure

```
statefabric/
├── packages/
│   ├── core/                    # Core state engine
│   │   ├── src/
│   │   │   ├── crdt/           # CRDT implementations
│   │   │   ├── engine/         # State engine
│   │   │   ├── log/            # Operation log
│   │   │   ├── persistence/    # Storage adapters
│   │   │   └── index.ts        # Public API
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── sdk/                     # Developer SDK
│   │   ├── src/
│   │   │   ├── client.ts       # Main client
│   │   │   ├── types/          # Type definitions
│   │   │   └── index.ts
│   │   └── package.json
│   │
│   ├── persistence-s3/          # S3 adapter
│   │   ├── src/
│   │   │   ├── adapter.ts
│   │   │   └── index.ts
│   │   └── package.json
│   │
│   └── persistence-local/       # Local filesystem adapter
│       ├── src/
│       │   ├── adapter.ts
│       │   └── index.ts
│       └── package.json
│
├── examples/                    # Usage examples
│   ├── lambda-session/
│   ├── rate-limiter/
│   └── ai-agent-memory/
│
├── tests/                       # Integration tests
│   ├── e2e/
│   └── performance/
│
├── pnpm-workspace.yaml
├── package.json
└── tsconfig.base.json
```

### 1.3 Dependencies

```json
{
  "dependencies": {
    "@aws-sdk/client-s3": "^3.x",     // S3 client
    "uuid": "^9.x",                   // ID generation (UUID v7)
    "zstd-codec": "^0.1.x"            // Compression (optional)
  },
  "devDependencies": {
    "typescript": "^5.x",
    "tsup": "^8.x",
    "vitest": "^1.x",
    "@types/node": "^20.x"
  }
}
```

---

## Part 2: Phase 1 Implementation (Weeks 1-4)

### Phase 1 Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PHASE 1: CORE RUNTIME                             │
│                                                                             │
│  Goal: Working local state engine with persistence                          │
│                                                                             │
│  Deliverables:                                                              │
│  ✅ CRDT implementations                                                    │
│  ✅ State engine (in-memory)                                                │
│  ✅ Operation log                                                           │
│  ✅ Local filesystem persistence                                            │
│  ✅ Basic SDK API                                                           │
│  ✅ Unit tests                                                              │
│                                                                             │
│  NOT included:                                                              │
│  ❌ S3 persistence                                                          │
│  ❌ Multi-writer sync                                                       │
│  ❌ Distributed features                                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Week 1: Foundation & CRDTs

#### Day 1-2: Project Setup

**Tasks:**
- [ ] Initialize monorepo with pnpm workspaces
- [ ] Create package structure
- [ ] Configure TypeScript
- [ ] Set up build scripts
- [ ] Configure Vitest for testing

**Commands:**
```bash
mkdir statefabric && cd statefabric
pnpm init
pnpm add -D typescript tsup vitest @types/node

# Create packages
mkdir -p packages/core/src/{crdt,engine,log,persistence}
mkdir -p packages/sdk/src
mkdir -p packages/persistence-local/src
```

**Verification:**
```bash
pnpm build    # Should compile without errors
pnpm test     # Should run (empty test suite passes)
```

#### Day 3-5: CRDT Implementations

**Task Breakdown:**

| Day | CRDT Type | Complexity | Key Methods |
|-----|-----------|------------|-------------|
| 3 | G-Counter | Simple | increment(), value(), merge() |
| 3 | PN-Counter | Simple | increment(), decrement(), value(), merge() |
| 4 | LWW-Register | Simple | set(), get(), merge() |
| 4 | G-Set | Simple | add(), has(), values(), merge() |
| 5 | OR-Set | Medium | add(), remove(), has(), values(), merge() |
| 5 | CRDT-Map | Medium | get(), set(), delete(), keys(), merge() |
| 5 | BoundedCounter | Medium | tryIncrement(), tryDecrement(), value() |

**Implementation Order:**

```typescript
// 1. Start with G-Counter (simplest)
// packages/core/src/crdt/g-counter.ts

export class GCounter {
  private counts: Map<string, number>;
  
  constructor(initial?: Map<string, number>) {
    this.counts = new Map(initial);
  }
  
  increment(actorId: string, amount = 1): void {
    const current = this.counts.get(actorId) ?? 0;
    this.counts.set(actorId, current + amount);
  }
  
  value(): number {
    let sum = 0;
    for (const count of this.counts.values()) sum += count;
    return sum;
  }
  
  merge(other: GCounter): GCounter {
    const merged = new Map(this.counts);
    for (const [actorId, count] of other.counts) {
      merged.set(actorId, Math.max(merged.get(actorId) ?? 0, count));
    }
    return new GCounter(merged);
  }
  
  toJSON(): Record<string, number> {
    return Object.fromEntries(this.counts);
  }
  
  static fromJSON(data: Record<string, number>): GCounter {
    return new GCounter(new Map(Object.entries(data)));
  }
}
```

```typescript
// P0 FIX #2: BoundedCounter - prevents negative values
// packages/core/src/crdt/bounded-counter.ts

export class BoundedCounter {
  private p: GCounter;  // Increments
  private n: GCounter;  // Decrements
  private min: number;
  private max: number;
  
  constructor(min = 0, max = Infinity) {
    this.p = new GCounter();
    this.n = new GCounter();
    this.min = min;
    this.max = max;
  }
  
  // Returns { success: boolean, value: number }
  tryIncrement(actorId: string, amount = 1): { success: boolean; value: number } {
    const newValue = this.value() + amount;
    if (newValue > this.max) {
      return { success: false, value: this.value() };
    }
    this.p.increment(actorId, amount);
    return { success: true, value: this.value() };
  }
  
  tryDecrement(actorId: string, amount = 1): { success: boolean; value: number } {
    const newValue = this.value() - amount;
    if (newValue < this.min) {
      return { success: false, value: this.value() };
    }
    this.n.increment(actorId, amount);
    return { success: true, value: this.value() };
  }
  
  value(): number {
    return this.p.value() - this.n.value();
  }
  
  merge(other: BoundedCounter): BoundedCounter {
    const merged = new BoundedCounter(this.min, this.max);
    merged.p = this.p.merge(other.p);
    merged.n = this.n.merge(other.n);
    return merged;
  }
}
```

```typescript
// P0 FIX #10: OR-Set with correct tombstone merge
// packages/core/src/crdt/or-set.ts

export class ORSet<T> {
  private elements: Map<T, Set<string>>;   // element -> set of tags
  private tombstones: Map<T, Set<string>>;  // removed element tags
  private gcThreshold: number = 10000;      // P0 FIX #3: GC threshold
  
  constructor() {
    this.elements = new Map();
    this.tombstones = new Map();
  }
  
  private makeTag(actorId: string, timestamp: HybridTimestamp): string {
    return `${actorId}:${timestamp.physical}:${timestamp.logical}`;
  }
  
  private parseTagTimestamp(tag: string): HybridTimestamp {
    const [actorId, physical, logical] = tag.split(':');
    return { physical: parseInt(physical), logical: parseInt(logical), nodeId: actorId };
  }
  
  add(element: T, actorId: string, timestamp: HybridTimestamp): void {
    const tag = this.makeTag(actorId, timestamp);
    
    if (!this.elements.has(element)) {
      this.elements.set(element, new Set());
    }
    
    // Only add if not tombstoned
    const tombs = this.tombstones.get(element);
    if (!tombs || !tombs.has(tag)) {
      this.elements.get(element)!.add(tag);
    }
  }
  
  remove(element: T, actorId: string, timestamp: HybridTimestamp): void {
    const tags = this.elements.get(element);
    if (tags) {
      // Add all current tags to tombstones
      if (!this.tombstones.has(element)) {
        this.tombstones.set(element, new Set());
      }
      for (const tag of tags) {
        this.tombstones.get(element)!.add(tag);
      }
      // Remove from elements
      this.elements.delete(element);
    }
    
    // P0 FIX #3: Trigger GC if needed
    this.maybeGC();
  }
  
  has(element: T): boolean {
    return this.elements.has(element) && this.elements.get(element)!.size > 0;
  }
  
  values(): T[] {
    return Array.from(this.elements.keys()).filter(e => this.has(e));
  }
  
  // P0 FIX #10: Correct merge - tombstones FIRST from BOTH sides
  merge(other: ORSet<T>): ORSet<T> {
    const merged = new ORSet<T>();
    
    // STEP 1: Merge tombstones from BOTH sides FIRST
    for (const [element, tags] of this.tombstones) {
      if (!merged.tombstones.has(element)) {
        merged.tombstones.set(element, new Set());
      }
      for (const tag of tags) {
        merged.tombstones.get(element)!.add(tag);
      }
    }
    for (const [element, tags] of other.tombstones) {
      if (!merged.tombstones.has(element)) {
        merged.tombstones.set(element, new Set());
      }
      for (const tag of tags) {
        merged.tombstones.get(element)!.add(tag);
      }
    }
    
    // STEP 2: Merge elements, excluding tombstoned tags
    for (const [element, tags] of [...this.elements, ...other.elements]) {
      if (!merged.elements.has(element)) {
        merged.elements.set(element, new Set());
      }
      const tombs = merged.tombstones.get(element) || new Set();
      for (const tag of tags) {
        if (!tombs.has(tag)) {
          merged.elements.get(element)!.add(tag);
        }
      }
    }
    
    // STEP 3: Clean up empty entries
    for (const [element, tags] of merged.elements) {
      if (tags.size === 0) {
        merged.elements.delete(element);
      }
    }
    
    return merged;
  }
  
  // P0 FIX #3: Garbage collection for tombstones
  private maybeGC(): void {
    const tombstoneCount = Array.from(this.tombstones.values())
      .reduce((sum, set) => sum + set.size, 0);
    
    if (tombstoneCount > this.gcThreshold) {
      this.garbageCollect();
    }
  }
  
  private garbageCollect(): void {
    const stableTime = Date.now() - 60000; // 1 minute ago
    
    for (const [element, tags] of this.tombstones) {
      const stableTags = Array.from(tags).filter(tag => {
        const tagTime = this.parseTagTimestamp(tag);
        return tagTime.physical < stableTime;
      });
      
      for (const tag of stableTags) {
        this.tombstones.get(element)!.delete(tag);
      }
      
      if (this.tombstones.get(element)!.size === 0) {
        this.tombstones.delete(element);
      }
    }
  }
}
```

```typescript
// P0 FIX #9: CRDT-Map with recursive merge
// packages/core/src/crdt/crdt-map.ts

export class CRDTMap {
  private entries: Map<string, CRDT>;
  private timestamps: Map<string, HybridTimestamp>;
  
  constructor() {
    this.entries = new Map();
    this.timestamps = new Map();
  }
  
  get(key: string): CRDT | undefined {
    return this.entries.get(key);
  }
  
  set(key: string, value: CRDT, timestamp: HybridTimestamp): void {
    const currentTs = this.timestamps.get(key);
    
    if (!currentTs || HybridLogicalClock.compare(timestamp, currentTs) >= 0) {
      this.entries.set(key, value);
      this.timestamps.set(key, timestamp);
    }
  }
  
  delete(key: string, timestamp: HybridTimestamp): void {
    const currentTs = this.timestamps.get(key);
    
    if (!currentTs || HybridLogicalClock.compare(timestamp, currentTs) >= 0) {
      this.entries.delete(key);
      this.timestamps.set(key, timestamp);
    }
  }
  
  keys(): string[] {
    return Array.from(this.entries.keys());
  }
  
  // P0 FIX #9: Recursive merge - MERGE values, don't overwrite
  merge(other: CRDTMap): CRDTMap {
    const merged = new CRDTMap();
    
    const allKeys = new Set([...this.entries.keys(), ...other.entries.keys()]);
    
    for (const key of allKeys) {
      const thisEntry = this.entries.get(key);
      const otherEntry = other.entries.get(key);
      
      if (thisEntry && otherEntry) {
        // P0 FIX #9: BOTH EXIST - MERGE the CRDTs, don't overwrite!
        const mergedCRDT = thisEntry.merge(otherEntry);
        merged.entries.set(key, mergedCRDT);
        
        // Use the later timestamp
        merged.timestamps.set(key, 
          HybridLogicalClock.compare(
            this.timestamps.get(key)!,
            other.timestamps.get(key)!
          ) > 0 
            ? this.timestamps.get(key)! 
            : other.timestamps.get(key)!
        );
      } else if (thisEntry) {
        merged.entries.set(key, thisEntry);
        merged.timestamps.set(key, this.timestamps.get(key)!);
      } else if (otherEntry) {
        merged.entries.set(key, otherEntry);
        merged.timestamps.set(key, other.timestamps.get(key)!);
      }
    }
    
    return merged;
  }
}
```

**Test Requirements:**
```typescript
// packages/core/src/crdt/__tests__/g-counter.test.ts

describe('GCounter', () => {
  it('should increment correctly', () => {
    const counter = new GCounter();
    counter.increment('actor-a', 5);
    counter.increment('actor-b', 3);
    expect(counter.value()).toBe(8);
  });
  
  it('should merge correctly', () => {
    const counterA = new GCounter();
    counterA.increment('actor-a', 5);
    
    const counterB = new GCounter();
    counterB.increment('actor-b', 3);
    counterB.increment('actor-a', 2); // actor-a seen higher value
    
    const merged = counterA.merge(counterB);
    expect(merged.value()).toBe(10); // 5 (a) + 3 (b) + max(0,2) for a
  });
  
  it('should be idempotent', () => {
    const counter = new GCounter();
    counter.increment('actor-a', 5);
    
    const merged1 = counter.merge(counter);
    const merged2 = merged1.merge(counter);
    
    expect(merged2.value()).toBe(5);
  });
});
```

**Verification:**
```bash
pnpm test packages/core/src/crdt
# All tests should pass
```

**Implementation:**
```typescript
// packages/core/src/engine/hlc.ts

export interface HybridTimestamp {
  physical: number;   // Wall clock time (ms)
  logical: number;    // Logical counter
  nodeId: string;     // Node identifier
}

export class HybridLogicalClock {
  private physical: number = 0;
  private logical: number = 0;
  
  // P0 FIX #5: Maximum allowed clock drift (5 minutes)
  private static MAX_DRIFT_MS = 5 * 60 * 1000;
  
  constructor(private nodeId: string) {}
  
  now(): HybridTimestamp {
    const wallClock = Date.now();
    
    if (wallClock > this.physical) {
      this.physical = wallClock;
      this.logical = 0;
    } else {
      this.logical++;
    }
    
    return {
      physical: this.physical,
      logical: this.logical,
      nodeId: this.nodeId
    };
  }
  
  update(received: HybridTimestamp): HybridTimestamp {
    const wallClock = Date.now();
    
    // P0 FIX #5: Reject timestamps too far in future (clock skew attack)
    if (received.physical > wallClock + HybridLogicalClock.MAX_DRIFT_MS) {
      console.error('HLC: Received timestamp too far in future, rejecting', {
        received: received.physical,
        wallClock,
        drift: received.physical - wallClock
      });
      return this.now(); // Use local clock instead
    }
    
    // P0 FIX #5: Reject timestamps too far in past (stale data)
    if (received.physical < wallClock - HybridLogicalClock.MAX_DRIFT_MS) {
      console.warn('HLC: Received timestamp too old, ignoring', {
        received: received.physical,
        wallClock
      });
      return this.now();
    }
    
    this.physical = Math.max(
      this.physical,
      received.physical,
      wallClock
    );
    
    if (this.physical === received.physical && 
        this.physical === wallClock) {
      this.logical = Math.max(this.logical, received.logical) + 1;
    } else if (this.physical === received.physical) {
      this.logical = Math.max(this.logical, received.logical) + 1;
    } else if (this.physical === wallClock) {
      this.logical++;
    } else {
      this.logical = 0;
    }
    
    return this.now();
  }
  
  static compare(a: HybridTimestamp, b: HybridTimestamp): number {
    if (a.physical !== b.physical) return a.physical - b.physical;
    if (a.logical !== b.logical) return a.logical - b.logical;
    return a.nodeId.localeCompare(b.nodeId);
  }
}
```

**Acceptance Criteria:**
- [ ] Poisoned timestamp simulation cannot permanently bias conflict resolution
- [ ] Drift violations are logged and metered per node

---

### Week 2: State Engine & Operation Log

#### Day 1-2: Operation Log

**Data Structure:**
```typescript
// packages/core/src/log/types.ts

export interface Operation {
  id: string;                    // UUID v7 (time-ordered)
  timestamp: HybridTimestamp;
  actor: {
    nodeId: string;
    processId: string;
  };
  namespace: string;
  key: string;
  type: 'set' | 'delete' | 'increment' | 'decrement' | 'add' | 'remove';
  value: unknown;
  crdtMetadata: {
    type: string;
    actorId: string;
    counter: number;
  };
}

export interface LogEntry {
  operation: Operation;
  vectorClock: Record<string, number>;
}
```

**Log Manager:**
```typescript
// packages/core/src/log/log-manager.ts

export class LogManager {
  private operations: Operation[] = [];
  private vectorClock: Map<string, number> = new Map();
  
  append(operation: Operation): void {
    this.operations.push(operation);
    
    // Update vector clock
    const nodeId = operation.actor.nodeId;
    this.vectorClock.set(
      nodeId, 
      (this.vectorClock.get(nodeId) ?? 0) + 1
    );
  }
  
  getOperations(since?: HybridTimestamp): Operation[] {
    if (!since) return [...this.operations];
    
    return this.operations.filter(
      op => HybridLogicalClock.compare(op.timestamp, since) > 0
    );
  }
  
  getVectorClock(): Record<string, number> {
    return Object.fromEntries(this.vectorClock);
  }
  
  compact(beforeIndex: number): void {
    this.operations = this.operations.slice(beforeIndex);
  }
  
  size(): number {
    return this.operations.length;
  }
}
```

#### Day 3-4: State Engine

**Core Engine:**
```typescript
// packages/core/src/engine/state-engine.ts

export class StateEngine {
  private namespaces: Map<string, Map<string, CRDT>> = new Map();
  private logManager: LogManager;
  private hlc: HybridLogicalClock;
  private nodeId: string;
  
  constructor(nodeId: string) {
    this.nodeId = nodeId;
    this.logManager = new LogManager();
    this.hlc = new HybridLogicalClock(nodeId);
  }
  
  // Namespace management
  namespace(name: string): NamespaceHandle {
    if (!this.namespaces.has(name)) {
      this.namespaces.set(name, new Map());
    }
    return new NamespaceHandle(
      name, 
      this.namespaces.get(name)!,
      this.logManager,
      this.hlc,
      this.nodeId
    );
  }
  
  // Get specific CRDT type
  counter(namespace: string, key: string): PNCounter {
    return this.namespace(namespace).counter(key);
  }
  
  set<T>(namespace: string, key: string): ORSet<T> {
    return this.namespace(namespace).set(key);
  }
  
  register<T>(namespace: string, key: string): LWWRegister<T> {
    return this.namespace(namespace).register(key);
  }
  
  map(namespace: string, key: string): CRDTMap {
    return this.namespace(namespace).map(key);
  }
  
  // Apply operation (for replay)
  applyOperation(op: Operation): void {
    const ns = this.namespace(op.namespace);
    ns.applyOperation(op);
    this.logManager.append(op);
  }
  
  // Get all operations for persistence
  getOperations(): Operation[] {
    return this.logManager.getOperations();
  }
  
  // Create snapshot
  createSnapshot(): Snapshot {
    const state: Record<string, Record<string, unknown>> = {};
    
    for (const [nsName, ns] of this.namespaces) {
      state[nsName] = {};
      for (const [key, crdt] of ns) {
        state[nsName][key] = crdt.toJSON();
      }
    }
    
    return {
      version: '1.0',
      timestamp: this.hlc.now(),
      vectorClock: this.logManager.getVectorClock(),
      state,
      metadata: {
        nodeId: this.nodeId,
        operationCount: this.logManager.size()
      }
    };
  }
  
  // Restore from snapshot
  restoreSnapshot(snapshot: Snapshot): void {
    // Clear existing state
    this.namespaces.clear();
    
    // Restore state
    for (const [nsName, ns] of Object.entries(snapshot.state)) {
      const namespace = new Map();
      for (const [key, value] of Object.entries(ns)) {
        // Reconstruct CRDT from JSON
        namespace.set(key, this.reconstructCRDT(value));
      }
      this.namespaces.set(nsName, namespace);
    }
  }
  
  private reconstructCRDT(data: unknown): CRDT {
    // Implementation depends on stored type metadata
    // This is simplified
    if (typeof data === 'object' && data !== null) {
      const typed = data as { __type: string };
      switch (typed.__type) {
        case 'pn-counter':
          return PNCounter.fromJSON(data);
        case 'lww-register':
          return LWWRegister.fromJSON(data);
        case 'or-set':
          return ORSet.fromJSON(data);
        case 'crdt-map':
          return CRDTMap.fromJSON(data);
        default:
          return new LWWRegister(data);
      }
    }
    return new LWWRegister(data);
  }
}
```

#### Day 5: Namespace Handle

```typescript
// packages/core/src/engine/namespace-handle.ts

export class NamespaceHandle {
  constructor(
    private name: string,
    private data: Map<string, CRDT>,
    private logManager: LogManager,
    private hlc: HybridLogicalClock,
    private nodeId: string
  ) {}
  
  counter(key: string): PNCounter {
    if (!this.data.has(key)) {
      this.data.set(key, new PNCounter());
    }
    return this.data.get(key) as PNCounter;
  }
  
  set<T>(key: string): ORSet<T> {
    if (!this.data.has(key)) {
      this.data.set(key, new ORSet<T>());
    }
    return this.data.get(key) as ORSet<T>;
  }
  
  register<T>(key: string): LWWRegister<T> {
    if (!this.data.has(key)) {
      this.data.set(key, new LWWRegister<T>());
    }
    return this.data.get(key) as LWWRegister<T>;
  }
  
  map(key: string): CRDTMap {
    if (!this.data.has(key)) {
      this.data.set(key, new CRDTMap());
    }
    return this.data.get(key) as CRDTMap;
  }
  
  get(key: string): CRDT | undefined {
    return this.data.get(key);
  }
  
  delete(key: string): void {
    this.data.delete(key);
    this.logOperation('delete', key, undefined);
  }
  
  keys(): string[] {
    return Array.from(this.data.keys());
  }
  
  applyOperation(op: Operation): void {
    // Apply operation to appropriate CRDT
    switch (op.type) {
      case 'set':
        this.register(op.key).set(op.value, op.actor.nodeId, op.timestamp);
        break;
      case 'increment':
        this.counter(op.key).increment(op.actor.nodeId, op.value as number);
        break;
      case 'decrement':
        this.counter(op.key).decrement(op.actor.nodeId, op.value as number);
        break;
      case 'add':
        this.set(op.key).add(op.value, op.actor.nodeId, op.timestamp);
        break;
      case 'remove':
        this.set(op.key).remove(op.value, op.actor.nodeId, op.timestamp);
        break;
    }
  }
  
  private logOperation(type: Operation['type'], key: string, value: unknown): void {
    const operation: Operation = {
      id: crypto.randomUUID(),
      timestamp: this.hlc.now(),
      actor: { nodeId: this.nodeId, processId: process.pid.toString() },
      namespace: this.name,
      key,
      type,
      value,
      crdtMetadata: {
        type: 'operation',
        actorId: this.nodeId,
        counter: 0
      }
    };
    
    this.logManager.append(operation);
  }
}
```

---

### Week 3: Persistence Layer

#### Day 1-2: Local Filesystem Persistence

```typescript
// packages/persistence-local/src/adapter.ts

import { promises as fs } from 'fs';
import path from 'path';
import type { Snapshot, Operation } from '@statefabric/core';

export interface LocalPersistenceConfig {
  dataDir: string;
  snapshotThreshold?: number;  // Operations before snapshot
}

export class LocalPersistenceAdapter {
  private snapshotThreshold: number;
  
  constructor(private config: LocalPersistenceConfig) {
    this.snapshotThreshold = config.snapshotThreshold ?? 1000;
  }
  
  async initialize(): Promise<void> {
    await fs.mkdir(this.config.dataDir, { recursive: true });
    await fs.mkdir(path.join(this.config.dataDir, 'log'), { recursive: true });
  }
  
  async saveSnapshot(snapshot: Snapshot): Promise<void> {
    const snapshotPath = path.join(this.config.dataDir, 'snapshot.json');
    const data = JSON.stringify(snapshot);
    await fs.writeFile(snapshotPath, data, 'utf-8');
  }
  
  async loadSnapshot(): Promise<Snapshot | null> {
    const snapshotPath = path.join(this.config.dataDir, 'snapshot.json');
    try {
      const data = await fs.readFile(snapshotPath, 'utf-8');
      return JSON.parse(data);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return null;
      }
      throw error;
    }
  }
  
  async appendOperation(operation: Operation): Promise<void> {
    const opPath = path.join(
      this.config.dataDir, 
      'log', 
      `${operation.id}.json`
    );
    await fs.writeFile(opPath, JSON.stringify(operation), 'utf-8');
  }
  
  async loadOperations(since?: string): Promise<Operation[]> {
    const logDir = path.join(this.config.dataDir, 'log');
    const files = await fs.readdir(logDir);
    
    const operations: Operation[] = [];
    for (const file of files) {
      if (since && file <= since) continue;
      const data = await fs.readFile(path.join(logDir, file), 'utf-8');
      operations.push(JSON.parse(data));
    }
    
    // Sort by timestamp
    operations.sort((a, b) => 
      HybridLogicalClock.compare(a.timestamp, b.timestamp)
    );
    
    return operations;
  }
  
  async compact(beforeOpId: string): Promise<void> {
    const logDir = path.join(this.config.dataDir, 'log');
    const files = await fs.readdir(logDir);
    
    for (const file of files) {
      if (file < beforeOpId) {
        await fs.unlink(path.join(logDir, file));
      }
    }
  }
}
```

#### Day 3-4: Persistence Manager

```typescript
// packages/core/src/persistence/persistence-manager.ts

import type { StateEngine, Snapshot, Operation } from '../index';
import type { LocalPersistenceAdapter } from '@statefabric/persistence-local';

export interface PersistenceAdapter {
  initialize(): Promise<void>;
  saveSnapshot(snapshot: Snapshot): Promise<void>;
  loadSnapshot(): Promise<Snapshot | null>;
  appendOperation(operation: Operation): Promise<void>;
  loadOperations(since?: string): Promise<Operation[]>;
  compact(beforeOpId: string): Promise<void>;
}

export class PersistenceManager {
  private pendingOperations: Operation[] = [];
  private snapshotThreshold: number;
  private lastSnapshotOpCount: number = 0;
  
  constructor(
    private engine: StateEngine,
    private adapter: PersistenceAdapter,
    options?: { snapshotThreshold?: number }
  ) {
    this.snapshotThreshold = options?.snapshotThreshold ?? 1000;
  }
  
  async initialize(): Promise<void> {
    await this.adapter.initialize();
    
    // Load snapshot
    const snapshot = await this.adapter.loadSnapshot();
    if (snapshot) {
      this.engine.restoreSnapshot(snapshot);
      this.lastSnapshotOpCount = snapshot.metadata.operationCount;
    }
    
    // Replay operations since snapshot
    const operations = await this.adapter.loadOperations();
    for (const op of operations) {
      this.engine.applyOperation(op);
    }
  }
  
  // Every operation is synchronously appended to S3 before returning
  // This eliminates the durability gap - no data loss on crash
  async persist(operation: Operation): Promise<void> {
    // CRITICAL: Sync append BEFORE returning to caller
    // This ensures durability even on hard crash
    await this.adapter.appendOperation(operation);
    this.pendingOperations.push(operation);
    
    // Check if snapshot needed
    if (this.pendingOperations.length >= this.snapshotThreshold) {
      await this.createSnapshot();
    }
  }
  
  async createSnapshot(): Promise<void> {
    const snapshot = this.engine.createSnapshot();
    await this.adapter.saveSnapshot(snapshot);
    
    // Compact old operations
    const firstPending = this.pendingOperations[0];
    if (firstPending) {
      await this.adapter.compact(firstPending.id);
    }
    
    this.pendingOperations = [];
  }
  
  async shutdown(): Promise<void> {
    // Create final snapshot
    await this.createSnapshot();
  }
}
```

#### Day 5: Lifecycle Hooks

```typescript
// packages/core/src/engine/lifecycle.ts

// P1 FIX #19: Graceful shutdown WITHOUT process.exit()

export function setupLifecycleHooks(manager: PersistenceManager): void {
  let isShuttingDown = false;
  
  const gracefulShutdown = async (signal: string) => {
    if (isShuttingDown) return; // Prevent duplicate shutdowns
    isShuttingDown = true;
    
    console.log(`StateFabric: ${signal} received, persisting state...`);
    
    try {
      await manager.shutdown();
      console.log('StateFabric: State persisted successfully');
      // P1 FIX #19: Do NOT call process.exit() - let runtime handle exit naturally
    } catch (error) {
      console.error('StateFabric: Failed to persist state:', error);
      // Log but don't force exit - let Lambda handle cleanup
    }
  };
  
  // Register signal handlers
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  
  // P1 FIX #19: Use beforeExit for final cleanup (fires when event loop is empty)
  process.on('beforeExit', async () => {
    if (!isShuttingDown) {
      await manager.shutdown();
    }
  });
  
  // Handle uncaught exceptions
  process.on('uncaughtException', async (error) => {
    console.error('Uncaught exception:', error);
    try {
      await manager.shutdown();
    } catch (e) {
      console.error('Failed to persist state:', e);
    }
    // For uncaught exceptions, we DO need to exit with error code
    // but only after cleanup
    process.exitCode = 1;
  });
}

// Lambda Extension pattern for guaranteed cleanup (alternative approach)
export const stateFabricExtension = {
  init: async () => {
    // Initialize StateFabric
  },
  shutdown: async () => {
    // This is called by Lambda before freezing/terminating
    // See: https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html
  }
};
```

**Acceptance Criteria:**
- [ ] Runtime exits without truncating writes
- [ ] No `process.exit()` calls in normal shutdown path
- [ ] Lambda Extension integration documented

---

### Week 4: SDK & Integration

#### Day 1-2: SDK Client

```typescript
// packages/sdk/src/client.ts

import { StateEngine } from '@statefabric/core';
import { PersistenceManager } from '@statefabric/core';
import type { PersistenceAdapter } from '@statefabric/core';

export interface StateFabricConfig {
  nodeId?: string;
  persistence?: {
    adapter: PersistenceAdapter;
    snapshotThreshold?: number;
  };
}

export class StateFabric {
  private engine: StateEngine;
  private persistence?: PersistenceManager;
  private initialized: boolean = false;
  
  constructor(private config: StateFabricConfig = {}) {
    const nodeId = config.nodeId ?? this.generateNodeId();
    this.engine = new StateEngine(nodeId);
    
    if (config.persistence) {
      this.persistence = new PersistenceManager(
        this.engine,
        config.persistence.adapter,
        { snapshotThreshold: config.persistence.snapshotThreshold }
      );
    }
  }
  
  async initialize(): Promise<void> {
    if (this.initialized) return;
    
    if (this.persistence) {
      await this.persistence.initialize();
      setupLifecycleHooks(this.persistence);
    }
    
    this.initialized = true;
  }
  
  // CRDT factories
  counter(namespace: string, key: string) {
    return this.engine.counter(namespace, key);
  }
  
  set<T>(namespace: string, key: string) {
    return this.engine.set<T>(namespace, key);
  }
  
  register<T>(namespace: string, key: string) {
    return this.engine.register<T>(namespace, key);
  }
  
  map(namespace: string, key: string) {
    return this.engine.map(namespace, key);
  }
  
  // Namespace access
  namespace(name: string) {
    return this.engine.namespace(name);
  }
  
  // Persistence
  async persist(): Promise<void> {
    if (!this.persistence) {
      throw new Error('No persistence configured');
    }
    await this.persistence.createSnapshot();
  }
  
  async shutdown(): Promise<void> {
    if (this.persistence) {
      await this.persistence.shutdown();
    }
  }
  
  // Snapshot
  createSnapshot() {
    return this.engine.createSnapshot();
  }
  
  restoreSnapshot(snapshot: Snapshot) {
    return this.engine.restoreSnapshot(snapshot);
  }
  
  private generateNodeId(): string {
    return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }
}
```

#### Day 3-4: Type Definitions

```typescript
// packages/sdk/src/types.ts

// Re-export all types
export type { Operation, LogEntry } from '@statefabric/core';
export type { Snapshot, SnapshotMetadata } from '@statefabric/core';
export type { HybridTimestamp } from '@statefabric/core';
export type { 
  CRDT, 
  GCounter, 
  PNCounter, 
  LWWRegister, 
  ORSet, 
  CRDTMap 
} from '@statefabric/core';
export type { 
  PersistenceAdapter, 
  PersistenceManager 
} from '@statefabric/core';
export type { LocalPersistenceConfig } from '@statefabric/persistence-local';

// SDK-specific types
export interface StateFabricOptions {
  nodeId?: string;
  persistence?: PersistenceConfig;
}

export interface PersistenceConfig {
  type: 'local' | 's3';
  dataDir?: string;  // for local
  bucket?: string;   // for s3
  region?: string;   // for s3
  snapshotThreshold?: number;
}
```

#### Day 5: Integration Tests

```typescript
// tests/e2e/local-persistence.test.ts

import { StateFabric } from '@statefabric/sdk';
import { LocalPersistenceAdapter } from '@statefabric/persistence-local';
import { promises as fs } from 'fs';
import path from 'path';

describe('StateFabric with Local Persistence', () => {
  const testDir = './test-data';
  let state: StateFabric;
  
  beforeEach(async () => {
    // Clean test directory
    await fs.rm(testDir, { recursive: true, force: true });
    
    state = new StateFabric({
      persistence: {
        adapter: new LocalPersistenceAdapter({ dataDir: testDir })
      }
    });
    
    await state.initialize();
  });
  
  afterEach(async () => {
    await state.shutdown();
  });
  
  it('should persist counter state across restarts', async () => {
    // First session
    const counter = state.counter('test', 'visits');
    counter.increment('user-1', 5);
    counter.increment('user-2', 3);
    
    await state.persist();
    await state.shutdown();
    
    // Second session
    const state2 = new StateFabric({
      persistence: {
        adapter: new LocalPersistenceAdapter({ dataDir: testDir })
      }
    });
    
    await state2.initialize();
    
    const restoredCounter = state2.counter('test', 'visits');
    expect(restoredCounter.value()).toBe(8);
    
    await state2.shutdown();
  });
  
  it('should persist map state correctly', async () => {
    const cart = state.map('carts');
    cart.set('user-1', { items: ['apple', 'banana'], total: 5.00 });
    
    await state.persist();
    await state.shutdown();
    
    const state2 = new StateFabric({
      persistence: {
        adapter: new LocalPersistenceAdapter({ dataDir: testDir })
      }
    });
    
    await state2.initialize();
    
    const restoredCart = state2.map('carts');
    const userCart = restoredCart.get('user-1');
    
    expect(userCart.items).toEqual(['apple', 'banana']);
    expect(userCart.total).toBe(5.00);
    
    await state2.shutdown();
  });
  
  it('should handle concurrent operations from different actors', async () => {
    const stateA = new StateFabric({ nodeId: 'node-a' });
    const stateB = new StateFabric({ nodeId: 'node-b' });
    
    const counterA = stateA.counter('test', 'votes');
    const counterB = stateB.counter('test', 'votes');
    
    counterA.increment('node-a', 5);
    counterB.increment('node-b', 3);
    
    // Both should have their own increments
    expect(counterA.value()).toBe(5);
    expect(counterB.value()).toBe(3);
    
    // After merge (simulating sync)
    const merged = counterA.merge(counterB);
    expect(merged.value()).toBe(8);
  });
});
```

---

## Part 3: Phase 2 Implementation (Weeks 5-8)

### Phase 2 Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PHASE 2: S3 PERSISTENCE                           │
│                                                                             │
│  Goal: Production-ready S3 persistence for serverless                      │
│                                                                             │
│  Deliverables:                                                              │
│  ✅ S3 persistence adapter                                                  │
│  ✅ Compression support                                                     │
│  ✅ Serverless integration examples                                         │
│  ✅ Performance optimization                                                │
│  ✅ Error handling & retry logic                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Week 5: S3 Adapter

#### S3 Adapter Implementation

```typescript
// packages/persistence-s3/src/adapter.ts

import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';
import type { Snapshot, Operation } from '@statefabric/core';

export interface S3PersistenceConfig {
  bucket: string;
  keyPrefix?: string;
  region?: string;
  compression?: boolean;
}

export class S3PersistenceAdapter {
  private client: S3Client;
  private compression: boolean;
  
  constructor(private config: S3PersistenceConfig) {
    this.client = new S3Client({ region: config.region });
    this.compression = config.compression ?? false;
  }
  
  private get keyPrefix() {
    return this.config.keyPrefix ?? '';
  }
  
  async saveSnapshot(snapshot: Snapshot): Promise<void> {
    const key = `${this.keyPrefix}snapshot.json`;
    let body: string | Buffer = JSON.stringify(snapshot);
    
    if (this.compression) {
      body = await this.compress(body);
    }
    
    await this.client.send(new PutObjectCommand({
      Bucket: this.config.bucket,
      Key: key,
      Body: body,
      ContentType: 'application/json',
      Metadata: {
        'statefabric-version': '1.0',
        'node-id': snapshot.metadata.nodeId,
        'compressed': String(this.compression)
      }
    }));
  }
  
  async loadSnapshot(): Promise<Snapshot | null> {
    const key = `${this.keyPrefix}snapshot.json`;
    
    try {
      const response = await this.client.send(new GetObjectCommand({
        Bucket: this.config.bucket,
        Key: key
      }));
      
      let data = await response.Body!.transformToString();
      
      // Check if compressed
      if (response.Metadata?.compressed === 'true') {
        data = await this.decompress(data);
      }
      
      return JSON.parse(data);
    } catch (error) {
      if ((error as any).name === 'NoSuchKey') {
        return null;
      }
      throw error;
    }
  }
  
  async appendOperation(operation: Operation): Promise<void> {
    const key = `${this.keyPrefix}log/${operation.id}.json`;
    
    await this.client.send(new PutObjectCommand({
      Bucket: this.config.bucket,
      Key: key,
      Body: JSON.stringify(operation),
      ContentType: 'application/json'
    }));
  }
  
  async loadOperations(since?: string): Promise<Operation[]> {
    const prefix = `${this.keyPrefix}log/`;
    
    const response = await this.client.send(new ListObjectsV2Command({
      Bucket: this.config.bucket,
      Prefix: prefix
    }));
    
    const operations: Operation[] = [];
    
    for (const object of response.Contents ?? []) {
      if (since && object.Key! <= `${prefix}${since}.json`) continue;
      
      const opResponse = await this.client.send(new GetObjectCommand({
        Bucket: this.config.bucket,
        Key: object.Key!
      }));
      
      const data = await opResponse.Body!.transformToString();
      operations.push(JSON.parse(data));
    }
    
    // Sort by timestamp
    operations.sort((a, b) => 
      HybridLogicalClock.compare(a.timestamp, b.timestamp)
    );
    
    return operations;
  }
  
  async compact(beforeOpId: string): Promise<void> {
    const prefix = `${this.keyPrefix}log/`;
    
    const response = await this.client.send(new ListObjectsV2Command({
      Bucket: this.config.bucket,
      Prefix: prefix
    }));
    
    const toDelete = (response.Contents ?? [])
      .filter(obj => obj.Key! < `${prefix}${beforeOpId}.json`)
      .map(obj => ({ Key: obj.Key! }));
    
    if (toDelete.length > 0) {
      await this.client.send(new DeleteObjectsCommand({
        Bucket: this.config.bucket,
        Delete: { Objects: toDelete }
      }));
    }
  }
  
  private async compress(data: string): Promise<Buffer> {
    // Use zstd or gzip
    const { compress } = await import('zstd-codec');
    return compress(data);
  }
  
  private async decompress(data: string): Promise<string> {
    const { decompress } = await import('zstd-codec');
    return decompress(data);
  }
}
```

### Week 6: Serverless Integration

#### AWS Lambda Example

```typescript
// examples/lambda-session/src/handler.ts

import { StateFabric } from '@statefabric/sdk';
import { S3PersistenceAdapter } from '@statefabric/persistence-s3';

// Global state (persists across warm invocations)
let state: StateFabric | null = null;

async function getState(): Promise<StateFabric> {
  if (!state) {
    state = new StateFabric({
      nodeId: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
      persistence: {
        adapter: new S3PersistenceAdapter({
          bucket: process.env.STATE_BUCKET!,
          keyPrefix: 'sessions/',
          region: process.env.AWS_REGION
        }),
        snapshotThreshold: 100
      }
    });
    await state.initialize();
  }
  return state;
}

export async function handler(event: any) {
  const state = await getState();
  
  // Example: Session management
  const sessions = state.map('sessions');
  const userId = event.requestContext?.authorizer?.userId;
  
  if (!userId) {
    return { statusCode: 401, body: 'Unauthorized' };
  }
  
  // Fast in-memory access
  const session = sessions.get(userId) || {
    userId,
    createdAt: Date.now(),
    visits: 0
  };
  
  session.visits++;
  session.lastAccess = Date.now();
  
  sessions.set(userId, session);
  
  // Persist on termination (handled automatically by lifecycle hooks)
  // But you can also persist manually:
  // await state.persist();
  
  return {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Session updated',
      visits: session.visits
    })
  };
}
```

#### Rate Limiter Example

```typescript
// examples/rate-limiter/src/handler.ts

import { StateFabric, WindowedCounter } from '@statefabric/sdk';
import { S3PersistenceAdapter } from '@statefabric/persistence-s3';

let state: StateFabric | null = null;

const RATE_LIMIT = 100;  // requests per minute
const WINDOW_MS = 60 * 1000;

async function getState(): Promise<StateFabric> {
  if (!state) {
    state = new StateFabric({
      persistence: {
        adapter: new S3PersistenceAdapter({
          bucket: process.env.STATE_BUCKET!,
          keyPrefix: 'rate-limits/',
          region: process.env.AWS_REGION
        })
      }
    });
    await state.initialize();
  }
  return state;
}

// P1 FIX #11: WindowedCounter for proper rate limiting
export async function handler(event: any) {
  const state = await getState();
  const apiKey = event.headers['x-api-key'];
  
  if (!apiKey) {
    return { statusCode: 401, body: 'Missing API key' };
  }
  
  // Use WindowedCounter - automatically resets each window
  const counter = state.windowedCounter('api-limits', apiKey, { windowMs: WINDOW_MS });
  
  const currentCount = counter.value();
  
  if (currentCount >= RATE_LIMIT) {
    return {
      statusCode: 429,
      body: JSON.stringify({
        error: 'Rate limit exceeded',
        limit: RATE_LIMIT,
        windowMs: WINDOW_MS
      })
    };
  }
  
  counter.increment();
  
  return {
    statusCode: 200,
    body: JSON.stringify({ 
      message: 'OK', 
      remaining: RATE_LIMIT - currentCount - 1
    })
  };
}

// WindowedCounter implementation
// packages/core/src/crdt/windowed-counter.ts

export class WindowedCounter {
  private counters: Map<string, Map<string, number>> = new Map();
  private windowSizeMs: number;
  
  constructor(windowSizeMs: number = 60000) {
    this.windowSizeMs = windowSizeMs;
  }
  
  private getWindowId(timestamp: number): string {
    return Math.floor(timestamp / this.windowSizeMs).toString();
  }
  
  increment(actorId: string, timestamp: number = Date.now()): void {
    const windowId = this.getWindowId(timestamp);
    
    if (!this.counters.has(windowId)) {
      this.counters.set(windowId, new Map());
    }
    
    const window = this.counters.get(windowId)!;
    window.set(actorId, (window.get(actorId) ?? 0) + 1);
  }
  
  value(timestamp: number = Date.now()): number {
    const windowId = this.getWindowId(timestamp);
    const window = this.counters.get(windowId);
    
    if (!window) return 0;
    
    let sum = 0;
    for (const count of window.values()) {
      sum += count;
    }
    return sum;
  }
  
  // GC old windows
  gc(maxAge: number = 5 * this.windowSizeMs): void {
    const cutoff = this.getWindowId(Date.now() - maxAge);
    
    for (const windowId of this.counters.keys()) {
      if (windowId < cutoff) {
        this.counters.delete(windowId);
      }
    }
  }
}
```

**Acceptance Criteria:**
- [ ] Counter state remains bounded
- [ ] Rate limiter recovers each window automatically

### Week 7-8: Testing & Documentation

#### Performance Benchmarks

```typescript
// tests/performance/benchmark.ts

import { StateFabric } from '@statefabric/sdk';
import { LocalPersistenceAdapter } from '@statefabric/persistence-local';

async function benchmark() {
  const state = new StateFabric({
    persistence: {
      adapter: new LocalPersistenceAdapter({ dataDir: './benchmark-data' })
    }
  });
  
  await state.initialize();
  
  // Benchmark counter operations
  console.log('Benchmarking counter operations...');
  const counter = state.counter('bench', 'counter');
  
  const startCounter = performance.now();
  for (let i = 0; i < 100000; i++) {
    counter.increment('bench-node');
  }
  const endCounter = performance.now();
  
  console.log(`100k counter increments: ${(endCounter - startCounter).toFixed(2)}ms`);
  console.log(`Per operation: ${((endCounter - startCounter) / 100000).toFixed(4)}ms`);
  
  // Benchmark map operations
  console.log('\nBenchmarking map operations...');
  const map = state.map('bench', 'map');
  
  const startMap = performance.now();
  for (let i = 0; i < 10000; i++) {
    map.set(`key-${i}`, { value: i, data: 'x'.repeat(100) });
  }
  const endMap = performance.now();
  
  console.log(`10k map writes: ${(endMap - startMap).toFixed(2)}ms`);
  console.log(`Per operation: ${((endMap - startMap) / 10000).toFixed(4)}ms`);
  
  // Benchmark persistence
  console.log('\nBenchmarking persistence...');
  const startPersist = performance.now();
  await state.persist();
  const endPersist = performance.now();
  
  console.log(`Snapshot save: ${(endPersist - startPersist).toFixed(2)}ms`);
  
  // Benchmark recovery
  console.log('\nBenchmarking recovery...');
  const state2 = new StateFabric({
    persistence: {
      adapter: new LocalPersistenceAdapter({ dataDir: './benchmark-data' })
    }
  });
  
  const startRecover = performance.now();
  await state2.initialize();
  const endRecover = performance.now();
  
  console.log(`State recovery: ${(endRecover - startRecover).toFixed(2)}ms`);
  
  await state.shutdown();
  await state2.shutdown();
}

benchmark();
```

**Expected Results:**

| Operation | Target | Notes |
|-----------|--------|-------|
| Counter increment | <0.01ms | In-memory, no I/O |
| Map set | <0.01ms | In-memory, no I/O |
| Snapshot save (10k ops) | <100ms | Depends on state size |
| State recovery | <200ms | Snapshot + log replay |

---

## Part 4: Quality Assurance

### Test Coverage Requirements

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TEST COVERAGE TARGETS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Package              Coverage    Focus Areas                               │
│  ─────────────────    ─────────   ──────────────                            │
│  @statefabric/core    >95%        CRDT merge, state engine, log             │
│  @statefabric/sdk     >90%        API surface, integration                  │
│  persistence-local    >85%        File I/O, recovery                        │
│  persistence-s3       >80%        S3 operations, error handling             │
│                                                                             │
│  Test Types:                                                                │
│  • Unit tests: Each CRDT, each component                                   │
│  • Integration tests: Full flow with persistence                           │
│  • E2E tests: Lambda simulation                                            │
│  • Performance tests: Benchmarks, stress tests                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml

name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: pnpm/action-setup@v2
        with:
          version: 8
          
      - uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'pnpm'
          
      - run: pnpm install
      
      - run: pnpm build
      
      - run: pnpm test:coverage
      
      - uses: codecov/codecov-action@v3
        with:
          files: ./coverage/lcov.info
          
  lint:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: pnpm/action-setup@v2
        with:
          version: 8
          
      - uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'pnpm'
          
      - run: pnpm install
      
      - run: pnpm lint
```

---

## Part 5: Success Metrics

### Phase 1 Completion Criteria

| Criteria | Verification |
|----------|--------------|
| All CRDTs implemented | `pnpm test` passes with >95% coverage |
| State engine functional | Can create/read/update/delete state |
| Operation log working | All operations logged and replayable |
| Local persistence working | State survives restart |
| SDK API complete | TypeScript types fully defined |

### Phase 2 Completion Criteria

| Criteria | Verification |
|----------|--------------|
| S3 adapter functional | Integration test with LocalStack |
| Lambda example working | Deployed and tested |
| Performance targets met | Benchmark results documented |
| Error handling complete | Edge cases tested |

### Definition of Done

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ✅ Feature implemented with tests                                          │
│  ✅ Code reviewed and approved                                              │
│  ✅ Documentation updated                                                   │
│  ✅ CI pipeline passes                                                      │
│  ✅ Performance verified                                                    │
│  ✅ Example/demonstration working                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Part 6: Risk Mitigation

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| S3 latency issues | Medium | High | Parallel loading, compression, regional buckets |
| State size growth | Medium | Medium | Partitioning, compaction strategy |
| Concurrent write conflicts | Low | Medium | CRDT merge handles automatically |
| Memory limits in Lambda | Medium | High | Lazy loading, state pruning |
| Cold start recovery time | Medium | Medium | Incremental snapshots, log truncation |

### Rollback Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  If StateFabric fails:                                                     │
│                                                                             │
│  1. Fallback to direct database access                                     │
│     - Keep existing DB logic as backup                                     │
│     - Feature flag to toggle StateFabric                                   │
│                                                                             │
│  2. Data recovery                                                          │
│     - All state in S3 is recoverable                                       │
│     - Operation log provides audit trail                                   │
│                                                                             │
│  3. Gradual rollout                                                        │
│     - Start with read-only workloads                                       │
│     - Add write support once stable                                        │
│     - Expand to more namespaces progressively                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Appendix: Quick Start Commands

```bash
# Initial setup
git clone https://github.com/yourorg/statefabric.git
cd statefabric
pnpm install

# Development
pnpm build          # Build all packages
pnpm test           # Run all tests
pnpm lint           # Lint code

# Local testing
cd examples/lambda-session
pnpm dev            # Run local development server

# Package
pnpm package        # Create distributable packages
```

---

**End of Execution Playbook**

---

## Part 7: Release Gate Checklist

### Go/No-Go Criteria

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        RELEASE GATE CHECKLIST                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ❌ NOT RELEASED UNTIL ALL P0 ITEMS CLOSED                                  │
│                                                                             │
│  P0 REQUIREMENTS:                                                           │
│  ───────────────                                                            │
│  [ ] #1: Kill-process tests show zero acknowledged-write loss              │
│  [ ] #2: Concurrency tests prove no negative inventory/balance             │
│  [ ] #3: Long-run tests show bounded memory for churn-heavy sets           │
│  [ ] #4: Docs clearly separate eventual vs strong guarantees               │
│  [ ] #5: Poisoned timestamp cannot bias conflict resolution               │
│  [ ] #6: Cost/latency benchmarks meet target at projected write rates      │
│  [ ] #9: Concurrent nested updates converge without data loss              │
│  [ ] #10: Deleted elements never reappear after any merge ordering         │
│  [ ] #15: No eventual API implies linearizable semantics                   │
│                                                                             │
│  FAULT INJECTION SUITE:                                                     │
│  ───────────────────                                                        │
│  [ ] Crash fault injection passes                                           │
│  [ ] Timeout fault injection passes                                         │
│  [ ] Clock skew simulation passes                                           │
│  [ ] Network partition simulation passes                                    │
│  [ ] Operation replay is deterministic                                      │
│                                                                             │
│  DOCUMENTATION:                                                             │
│  ─────────────                                                              │
│  [ ] API docs separate eventual vs strong guarantees                        │
│  [ ] All financial examples use strong APIs only                            │
│  [ ] Unsafe patterns have visible warnings                                  │
│                                                                             │
│  PERFORMANCE:                                                               │
│  ───────────                                                                │
│  [ ] Memory usage fits in 256MB Lambda profile                              │
│  [ ] Cold start recovery < 500ms                                            │
│  [ ] Warm invocation latency < 1ms                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Execution Sequence

1. **Correctness blockers:** #9, #10, #15, #4
2. **Durability and ordering safety:** #1, #5, #6
3. **Resource safety:** #3, #11, #12, #19
4. **Distributed operability:** #7, #8, #16, #17
5. **Governance and enterprise readiness:** #13, #14, #18, #20

---

## Appendix A: Conflict Visibility (P1 #8)

```typescript
// P1 FIX #8: Conflict event callbacks
// packages/core/src/crdt/conflict-events.ts

export interface ConflictEvent {
  type: 'lww-register' | 'or-set' | 'crdt-map' | 'bounded-counter';
  namespace: string;
  key: string;
  localValue: unknown;
  remoteValue: unknown;
  resolvedValue: unknown;
  resolution: 'local-wins' | 'remote-wins' | 'merged';
  timestamp: HybridTimestamp;
}

export type ConflictCallback = (event: ConflictEvent) => void;

// Add to LWWRegister:
export class LWWRegisterWithEvents<T> {
  private onConflict?: ConflictCallback;
  
  setOnConflict(callback: ConflictCallback): void {
    this.onConflict = callback;
  }
  
  merge(other: LWWRegister<T>): LWWRegister<T> {
    const cmp = HybridLogicalClock.compare(this.timestamp, other.timestamp);
    
    if (cmp === 0) {
      // Conflict! Same timestamp, need tie-breaker
      const localWins = this.actorId > other.actorId;
      
      this.onConflict?.({
        type: 'lww-register',
        namespace: this.namespace,
        key: this.key,
        localValue: this.value,
        remoteValue: other.value,
        resolvedValue: localWins ? this.value : other.value,
        resolution: localWins ? 'local-wins' : 'remote-wins',
        timestamp: this.timestamp
      });
    }
    
    // ... rest of merge logic
  }
}

// Usage:
state.register('config', 'setting')
  .setOnConflict((event) => {
    console.warn('Conflict resolved:', event);
    // Send to monitoring
    metrics.increment('statefabric.conflict', { type: event.type });
  });
```

---

## Appendix B: API Safety Markers (P0 #4, #15)

```typescript
// P0 FIX #4, #15: Separate eventual and strong APIs
// packages/sdk/src/client.ts

export class StateFabric {
  // ============================================================
  // EVENTUALLY CONSISTENT APIs (Default)
  // These do NOT provide atomicity or linearizability guarantees
  // ============================================================
  
  /**
   * Eventually consistent counter.
   * ⚠️ WARNING: Not safe for inventory, balances, or any bounded resource.
   */
  counter(namespace: string, key: string): PNCounter {
    return this.engine.counter(namespace, key);
  }
  
  /**
   * Eventually consistent map.
   * ⚠️ WARNING: Concurrent writes may conflict and be auto-resolved.
   */
  map(namespace: string, key: string): CRDTMap {
    return this.engine.map(namespace, key);
  }
  
  /**
   * Eventually consistent register.
   * ⚠️ WARNING: Last-writer-wins - concurrent writes may be lost.
   */
  register<T>(namespace: string, key: string): LWWRegister<T> {
    return this.engine.register(namespace, key);
  }
  
  // ============================================================
  // STRONGLY CONSISTENT APIs (Requires Coordination)
  // These provide linearizability but require coordination module
  // ============================================================
  
  /**
   * Strongly consistent counter.
   * Requires coordination module. Throws if unavailable.
   * ✅ SAFE for inventory, balances, and bounded resources.
   */
  strongCounter(namespace: string, key: string, options?: { min?: number; max?: number }): StrongCounter {
    if (!this.coordination) {
      throw new Error('Strong consistency requires coordination module. Initialize with { coordination: true }');
    }
    return this.coordination.getCounter(namespace, key, options);
  }
  
  /**
   * Strongly consistent map with atomic operations.
   * Requires coordination module. Throws if unavailable.
   * ✅ SAFE for financial transactions.
   */
  strongMap(namespace: string, key: string): StrongMap {
    if (!this.coordination) {
      throw new Error('Strong consistency requires coordination module.');
    }
    return this.coordination.getMap(namespace, key);
  }
  
  // ============================================================
  // BATCH OPERATIONS (Not Transactions!)
  // ⚠️ These are NOT atomic across nodes
  // ============================================================
  
  /**
   * Batch multiple operations.
   * ⚠️ WARNING: This is NOT a transaction. Operations are not atomic.
   * Renamed from 'transaction' to prevent confusion.
   */
  beginBatch(): BatchHandle {
    return new BatchHandle(this.engine);
  }
  
  /**
   * @deprecated Use beginBatch() instead.
   * This was renamed to prevent confusion with ACID transactions.
   */
  transaction(): BatchHandle {
    console.warn('state.transaction() is deprecated. Use beginBatch() instead. ' +
      'This is NOT an ACID transaction - operations are not atomic across nodes.');
    return this.beginBatch();
  }
}

// Strong consistency interfaces
export interface StrongCounter {
  value(): Promise<number>;
  increment(amount?: number): Promise<void>;
  decrement(amount?: number): Promise<void>;
  tryDecrement(amount: number): Promise<{ success: boolean; value: number }>;
}

export interface StrongMap {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
  
  // Only available on strong maps
  compareAndSwap(key: string, expected: unknown, newValue: unknown): Promise<boolean>;
  
  // Atomic multi-key operations
  transaction<T>(fn: (map: MapOperations) => Promise<T>): Promise<T>;
}
```

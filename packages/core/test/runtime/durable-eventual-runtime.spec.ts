import { describe, expect, it } from "vitest";
import {
  type ConflictEvent,
  DurableEventualRuntime,
  InMemoryMetricsCollector,
  type DurableRuntimeStore,
  type RuntimeOperation,
  type SegmentManifest,
  type RecoverySnapshot,
  type StoredSegmentGcReport
} from "../../src";

class InMemoryRuntimeStore implements DurableRuntimeStore {
  manifest: SegmentManifest | null = null;
  snapshot: RecoverySnapshot | null = null;
  readonly segments: Record<string, RuntimeOperation[]> = {};
  readonly appendCalls: string[][] = [];
  readonly deletedSegments: string[] = [];
  readonly deleteFailuresRemaining = new Map<string, number>();
  readonly gcReports = new Map<string, StoredSegmentGcReport>();

  async loadManifest(): Promise<SegmentManifest | null> {
    return this.manifest;
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    this.manifest = manifest;
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    this.snapshot = snapshot;
  }

  async appendToSegment(segmentId: string, operations: RuntimeOperation[]): Promise<void> {
    if (!this.segments[segmentId]) {
      this.segments[segmentId] = [];
    }
    this.segments[segmentId].push(...operations);
    this.appendCalls.push(operations.map(op => op.opId));
  }

  async readSegment(segmentId: string): Promise<RuntimeOperation[]> {
    return this.segments[segmentId] ?? [];
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    return this.snapshot;
  }

  async listSegments(): Promise<string[]> {
    return Object.keys(this.segments).sort();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    const failuresRemaining = this.deleteFailuresRemaining.get(segmentId) ?? 0;
    if (failuresRemaining > 0) {
      this.deleteFailuresRemaining.set(segmentId, failuresRemaining - 1);
      throw new Error(`delete failed for ${segmentId}`);
    }
    if (segmentId in this.segments) {
      delete this.segments[segmentId];
      this.deletedSegments.push(segmentId);
    }
  }

  async loadSegmentGcReport(executionId: string): Promise<StoredSegmentGcReport | null> {
    return this.gcReports.get(executionId) ?? null;
  }

  async saveSegmentGcReport(report: StoredSegmentGcReport): Promise<void> {
    this.gcReports.set(report.executionId, { ...report });
  }
}

class NoGcRuntimeStore implements DurableRuntimeStore {
  manifest: SegmentManifest | null = null;
  snapshot: RecoverySnapshot | null = null;
  readonly segments: Record<string, RuntimeOperation[]> = {};

  async loadManifest(): Promise<SegmentManifest | null> {
    return this.manifest;
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    this.manifest = manifest;
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    this.snapshot = snapshot;
  }

  async appendToSegment(segmentId: string, operations: RuntimeOperation[]): Promise<void> {
    if (!this.segments[segmentId]) {
      this.segments[segmentId] = [];
    }
    this.segments[segmentId].push(...operations);
  }

  async readSegment(segmentId: string): Promise<RuntimeOperation[]> {
    return this.segments[segmentId] ?? [];
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    return this.snapshot;
  }
}

describe("DurableEventualRuntime", () => {
  it("writes state durably and recovers from committed segments", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();

    const runtimeA = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 2,
      flushIntervalMs: 1_000,
      metrics
    });

    await Promise.all([
      runtimeA.setRegister("session", "user", "alice"),
      runtimeA.incrementCounter("rate", "user", 2)
    ]);

    await runtimeA.flush();
    await runtimeA.close();

    const runtimeB = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 10,
      flushIntervalMs: 1_000,
      metrics
    });

    const recovery = await runtimeB.recover();

    expect(recovery.appliedCount).toBe(2);
    expect(runtimeB.getRegister("session", "user")).toBe("alice");
    expect(runtimeB.getCounter("rate", "user")).toBe(2);
    expect(metrics.counter("statefabric.recovery_applied_ops_total")).toBe(2);
    expect(metrics.counter("statefabric.recovery_skipped_duplicates_total")).toBe(0);
    expect(metrics.counter("statefabric.recovery_segments_total")).toBeGreaterThanOrEqual(1);
  });

  it("respects snapshot checkpoint when selecting replay segments", async () => {
    const store = new InMemoryRuntimeStore();

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      initialSegmentId: "seg-1",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    await runtime.setRegister("profile", "name", "old");
    await runtime.rotateSegment("seg-2");

    store.snapshot = {
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-1" }
    };

    await runtime.setRegister("profile", "name", "new");
    await runtime.close();

    const recoveryRuntime = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    const recovery = await recoveryRuntime.recover();

    expect(recovery.snapshotId).toBe("snap-1");
    expect(recovery.replayedSegments).toEqual(["seg-2"]);
    expect(recoveryRuntime.getRegister("profile", "name")).toBe("new");
  });

  it("batches concurrent writes when maxBatchSize is reached", async () => {
    const store = new InMemoryRuntimeStore();

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 2,
      flushIntervalMs: 1_000
    });

    await Promise.all([
      runtime.setRegister("a", "x", 1),
      runtime.setRegister("a", "y", 2)
    ]);

    expect(store.appendCalls.length).toBe(1);
    expect(store.appendCalls[0].length).toBe(2);

    await runtime.close();
  });

  it("emits conflict event when duplicate operation is skipped during recovery", async () => {
    const store = new InMemoryRuntimeStore();

    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const duplicate: RuntimeOperation = {
      opId: "dup-1",
      namespace: "session",
      key: "user",
      type: "register.set",
      payload: { value: "alice" },
      actorNodeId: "node-a",
      sequence: 1,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    };

    const unique: RuntimeOperation = {
      opId: "uniq-1",
      namespace: "rate",
      key: "user",
      type: "counter.increment",
      payload: { amount: 3 },
      actorNodeId: "node-b",
      sequence: 1,
      hlc: { physical: 101, logical: 0, nodeId: "node-b" }
    };

    store.segments["seg-1"] = [duplicate];
    store.segments["seg-2"] = [
      {
        ...duplicate,
        sequence: 2,
        hlc: { physical: 102, logical: 0, nodeId: "node-a" }
      },
      unique
    ];

    const conflicts: ConflictEvent[] = [];
    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-c",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      onConflictEvent: event => {
        conflicts.push(event);
      }
    });

    const recovery = await runtime.recover();

    expect(recovery.skippedDuplicateCount).toBe(1);
    expect(conflicts).toEqual([
      {
        type: "replay.duplicate-operation",
        opId: "dup-1"
      }
    ]);
    expect(runtime.getRegister("session", "user")).toBe("alice");
    expect(runtime.getCounter("rate", "user")).toBe(3);
  });

  it("auto-compacts into snapshot state when snapshot thresholds are reached", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();

    const runtimeA = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 2,
      flushIntervalMs: 1_000,
      snapshotEveryOps: 2,
      snapshotEveryMs: 0,
      retainPreCheckpointSegments: 0,
      metrics
    });

    await Promise.all([
      runtimeA.setRegister("session", "user", "alice"),
      runtimeA.incrementCounter("rate", "user", 5)
    ]);
    await runtimeA.close();

    expect(store.snapshot?.checkpoint.lastSegmentId).toBe("seg-1");
    expect(store.snapshot?.state?.registers?.["session:user"]).toBe("alice");
    expect(store.snapshot?.state?.counters?.["rate:user"]).toBe(5);
    expect(store.manifest?.activeSegmentId).toBe("seg-2");
    expect(store.manifest?.committedSegments).toEqual(["seg-1"]);
    expect(metrics.counter("statefabric.snapshot_compactions_total")).toBe(1);
    expect(metrics.counter("statefabric.snapshot_compaction_failures_total")).toBe(0);

    const runtimeB = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    const recovery = await runtimeB.recover();

    expect(recovery.appliedCount).toBe(0);
    expect(runtimeB.getRegister("session", "user")).toBe("alice");
    expect(runtimeB.getCounter("rate", "user")).toBe(5);
    await runtimeB.close();
  });

  it("replays only post-checkpoint segments on top of snapshot state", async () => {
    const store = new InMemoryRuntimeStore();
    store.snapshot = {
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-1" },
      state: {
        counters: {
          "rate:user": 2
        }
      }
    };
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: { "node-a": 2 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [
      {
        opId: "op-1",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 2 },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      }
    ];
    store.segments["seg-2"] = [
      {
        opId: "op-2",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 3 },
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "node-a" }
      }
    ];

    const runtimeB = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    const recovery = await runtimeB.recover();

    expect(recovery.replayedSegments).toEqual(["seg-2"]);
    expect(recovery.appliedCount).toBe(1);
    expect(runtimeB.getCounter("rate", "user")).toBe(5);
    await runtimeB.close();
  });

  it("manual compaction prunes old committed segments by retention policy", async () => {
    const store = new InMemoryRuntimeStore();

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      snapshotEveryOps: 1_000,
      snapshotEveryMs: 0,
      retainPreCheckpointSegments: 1
    });

    await runtime.setRegister("profile", "name", "v1");
    await runtime.rotateSegment("seg-2");
    await runtime.incrementCounter("rate", "user", 2);
    await runtime.rotateSegment("seg-3");
    await runtime.setRegister("profile", "name", "v2");

    const snapshot = await runtime.compactNow();
    await runtime.close();

    expect(snapshot?.checkpoint.lastSegmentId).toBe("seg-3");
    expect(store.manifest?.committedSegments).toEqual(["seg-2", "seg-3"]);
    expect(store.manifest?.activeSegmentId).toBe("seg-4");
    expect(store.deletedSegments).toEqual(["seg-1"]);

    const recovered = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });
    const recovery = await recovered.recover();

    expect(recovery.appliedCount).toBe(0);
    expect(recovered.getRegister("profile", "name")).toBe("v2");
    expect(recovered.getCounter("rate", "user")).toBe(2);
    await recovered.close();
  });

  it("applies maxReplaySegments bound during recovery", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();

    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-4",
      committedSegments: ["seg-1", "seg-2", "seg-3"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [
      {
        opId: "op-1",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 1 },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      }
    ];
    store.segments["seg-2"] = [
      {
        opId: "op-2",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 2 },
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "node-a" }
      }
    ];
    store.segments["seg-3"] = [
      {
        opId: "op-3",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 3 },
        actorNodeId: "node-a",
        sequence: 3,
        hlc: { physical: 102, logical: 0, nodeId: "node-a" }
      }
    ];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      maxReplaySegments: 2,
      metrics
    });

    const recovery = await runtime.recover();

    expect(recovery.replayedSegments).toEqual(["seg-2", "seg-3"]);
    expect(runtime.getCounter("rate", "user")).toBe(5);
    expect(metrics.counter("statefabric.recovery_segment_bound_hits_total")).toBe(1);
    await runtime.close();
  });

  it("applies maxReplayOperations bound during recovery", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();

    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [
      {
        opId: "op-1",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 1 },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      },
      {
        opId: "op-2",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 2 },
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "node-a" }
      }
    ];
    store.segments["seg-2"] = [
      {
        opId: "op-3",
        namespace: "rate",
        key: "user",
        type: "counter.increment",
        payload: { amount: 3 },
        actorNodeId: "node-a",
        sequence: 3,
        hlc: { physical: 102, logical: 0, nodeId: "node-a" }
      }
    ];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      maxReplayOperations: 2,
      metrics
    });

    const recovery = await runtime.recover();

    expect(recovery.appliedCount).toBe(2);
    expect(runtime.getCounter("rate", "user")).toBe(5);
    expect(metrics.counter("statefabric.recovery_operation_bound_hits_total")).toBe(1);
    await runtime.close();
  });

  it("gcOrphanSegments keeps committed and active segments", async () => {
    const store = new InMemoryRuntimeStore();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [];
    store.segments["seg-2"] = [];
    store.segments["seg-3"] = [];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    const report = await runtime.gcOrphanSegments();

    expect(report.supported).toBe(true);
    expect(report.referencedSegments).toEqual(["seg-1", "seg-2", "seg-3"]);
    expect(report.orphanSegments).toEqual([]);
    expect(report.deletedSegments).toEqual([]);
    expect(Object.keys(store.segments).sort()).toEqual(["seg-1", "seg-2", "seg-3"]);
    await runtime.close();
  });

  it("gcOrphanSegments deletes unreferenced old segments", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [];
    store.segments["seg-2"] = [];
    store.segments["seg-3"] = [];
    store.segments["seg-old"] = [];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      metrics
    });

    const report = await runtime.gcOrphanSegments();

    expect(report.supported).toBe(true);
    expect(report.orphanSegments).toEqual(["seg-1", "seg-old"]);
    expect(report.deletedSegments).toEqual(["seg-1", "seg-old"]);
    expect(report.failedSegments).toEqual([]);
    expect(metrics.counter("statefabric.segment_gc_runs_total")).toBe(1);
    expect(metrics.counter("statefabric.segment_gc_deleted_total")).toBe(2);
    expect(Object.keys(store.segments).sort()).toEqual(["seg-2", "seg-3"]);
    await runtime.close();
  });

  it("gcOrphanSegments supports dryRun reports without deleting", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [];
    store.segments["seg-2"] = [];
    store.segments["seg-orphan"] = [];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      metrics
    });

    const report = await runtime.gcOrphanSegments({ dryRun: true });

    expect(report.supported).toBe(true);
    expect(report.dryRun).toBe(true);
    expect(report.orphanSegments).toEqual(["seg-orphan"]);
    expect(report.deletedSegments).toEqual([]);
    expect(Object.keys(store.segments).sort()).toEqual(["seg-1", "seg-2", "seg-orphan"]);
    expect(metrics.counter("statefabric.segment_gc_runs_total")).toBe(1);
    expect(metrics.counter("statefabric.segment_gc_dry_run_total")).toBe(1);
    await runtime.close();
  });

  it("gcOrphanSegments no-ops when store does not support list/delete hooks", async () => {
    const store = new NoGcRuntimeStore();
    const metrics = new InMemoryMetricsCollector();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      metrics
    });

    const report = await runtime.gcOrphanSegments({ dryRun: true });

    expect(report.supported).toBe(false);
    expect(report.reason).toBe("store-missing-hooks");
    expect(report.deletedSegments).toEqual([]);
    expect(report.failedSegments).toEqual([]);
    expect(metrics.counter("statefabric.segment_gc_unsupported_total")).toBe(1);
    await runtime.close();
  });

  it("gcOrphanSegments replays completed result for the same executionId", async () => {
    const store = new InMemoryRuntimeStore();
    const metrics = new InMemoryMetricsCollector();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [];
    store.segments["seg-2"] = [];
    store.segments["seg-orphan"] = [];

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      metrics
    });

    const first = await runtime.gcOrphanSegments({ executionId: "gc-1" });
    const second = await runtime.gcOrphanSegments({ executionId: "gc-1" });

    expect(first.replayedResult).toBe(false);
    expect(second.replayedResult).toBe(true);
    expect(second.deletedSegments).toEqual(["seg-orphan"]);
    expect(store.deletedSegments).toEqual(["seg-orphan"]);
    expect(metrics.counter("statefabric.segment_gc_replayed_total")).toBe(1);
    await runtime.close();
  });

  it("gcOrphanSegments resumes in-progress executionId after partial failures", async () => {
    const store = new InMemoryRuntimeStore();
    store.manifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };
    store.segments["seg-1"] = [];
    store.segments["seg-2"] = [];
    store.segments["seg-old-a"] = [];
    store.segments["seg-old-b"] = [];
    store.deleteFailuresRemaining.set("seg-old-b", 1);

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    const first = await runtime.gcOrphanSegments({ executionId: "gc-2" });
    const checkpoint = await store.loadSegmentGcReport("gc-2");
    const second = await runtime.gcOrphanSegments({ executionId: "gc-2" });
    const completed = await store.loadSegmentGcReport("gc-2");

    expect(first.failedSegments).toEqual(["seg-old-b"]);
    expect(checkpoint?.status).toBe("in_progress");
    expect(checkpoint?.remainingSegments).toEqual(["seg-old-b"]);
    expect(second.replayedResult).toBe(true);
    expect(second.deletedSegments).toEqual(["seg-old-a", "seg-old-b"]);
    expect(second.failedSegments).toEqual([]);
    expect(completed?.status).toBe("completed");
    expect(completed?.remainingSegments).toEqual([]);
    await runtime.close();
  });
});

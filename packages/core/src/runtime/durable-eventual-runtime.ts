import {
  DurableWritePipeline,
  SegmentBatchWriter,
  type BatchedDurableAppender,
  type OperationEnvelope,
  RecoveryManager,
  activateSegment,
  compactManifest,
  commitSegment,
  createManifest,
  type RecoveryResult,
  type RecoverySnapshot,
  type SegmentManifest
} from "../log";
import { ConflictEventStream, type ConflictEvent, type ConflictEventListener } from "../events";
import type { MetricsCollector } from "../observability";
import { HybridLogicalClock, type HLCTimestamp } from "../time";

export type RuntimeOperationType = "register.set" | "counter.increment";

export interface RuntimeOperation extends OperationEnvelope {
  hlc: HLCTimestamp;
  actorNodeId: string;
  sequence: number;
  type: RuntimeOperationType;
  payload: {
    value?: unknown;
    amount?: number;
  };
}

export interface DurableRuntimeStore {
  loadManifest(): Promise<SegmentManifest | null>;
  saveManifest(manifest: SegmentManifest): Promise<void>;
  saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void>;
  appendToSegment(segmentId: string, operations: RuntimeOperation[]): Promise<void>;
  readSegment(segmentId: string): Promise<RuntimeOperation[]>;
  loadLatestSnapshot(): Promise<RecoverySnapshot | null>;
  listSegments?(): Promise<string[]>;
  deleteSegment?(segmentId: string): Promise<void>;
  loadSegmentGcReport?(executionId: string): Promise<StoredSegmentGcReport | null>;
  saveSegmentGcReport?(report: StoredSegmentGcReport): Promise<void>;
}

export interface DurableEventualRuntimeOptions {
  nodeId: string;
  initialSegmentId?: string;
  maxBatchSize?: number;
  flushIntervalMs?: number;
  maxReplaySegments?: number;
  maxReplayOperations?: number;
  now?: () => number;
  snapshotEveryOps?: number;
  snapshotEveryMs?: number;
  retainPreCheckpointSegments?: number;
  orSetCompactTombstones?: () => number;
  onConflictEvent?: ConflictEventListener;
  metrics?: MetricsCollector;
}

export interface SegmentGcOptions {
  dryRun?: boolean;
  executionId?: string;
}

export interface SegmentGcReport {
  dryRun: boolean;
  supported: boolean;
  reason?: "store-missing-hooks" | "list-failed";
  executionId?: string;
  replayedResult: boolean;
  referencedSegments: string[];
  orphanSegments: string[];
  deletedSegments: string[];
  failedSegments: string[];
}

export interface StoredSegmentGcReport extends SegmentGcReport {
  executionId: string;
  status: "in_progress" | "completed";
  remainingSegments: string[];
}

const DEFAULT_MAX_BATCH_SIZE = 100;
const DEFAULT_FLUSH_INTERVAL_MS = 1_000;
const DEFAULT_SNAPSHOT_EVERY_OPS = 1_000;
const DEFAULT_SNAPSHOT_EVERY_MS = 60_000;
const DEFAULT_RETAIN_PRECHECKPOINT_SEGMENTS = 0;

export class DurableEventualRuntime {
  private readonly store: DurableRuntimeStore;
  private readonly nodeId: string;
  private readonly defaultSegmentId: string;
  private readonly now: () => number;
  private readonly maxReplaySegments?: number;
  private readonly maxReplayOperations?: number;
  private readonly snapshotEveryOps: number;
  private readonly snapshotEveryMs: number;
  private readonly retainPreCheckpointSegments: number;
  private readonly orSetCompactTombstones?: () => number;
  private readonly clock: HybridLogicalClock;

  private readonly registers = new Map<string, unknown>();
  private readonly counters = new Map<string, number>();

  private sequence = 0;
  private operationsSinceSnapshot = 0;
  private lastSnapshotAtMs: number;
  private compactionPromise: Promise<void> | null = null;
  private manifestState: SegmentManifest | null = null;
  private readonly batchWriter: SegmentBatchWriter;
  private readonly writePipeline: DurableWritePipeline;
  private readonly conflictEvents = new ConflictEventStream();
  private readonly metrics?: MetricsCollector;

  constructor(store: DurableRuntimeStore, options: DurableEventualRuntimeOptions) {
    this.store = store;
    this.nodeId = options.nodeId;
    this.defaultSegmentId = options.initialSegmentId ?? "seg-1";
    this.now = options.now ?? Date.now;
    this.maxReplaySegments =
      typeof options.maxReplaySegments === "number"
        ? Math.max(0, Math.floor(options.maxReplaySegments))
        : undefined;
    this.maxReplayOperations =
      typeof options.maxReplayOperations === "number"
        ? Math.max(0, Math.floor(options.maxReplayOperations))
        : undefined;
    this.snapshotEveryOps = Math.max(0, options.snapshotEveryOps ?? DEFAULT_SNAPSHOT_EVERY_OPS);
    this.snapshotEveryMs = Math.max(0, options.snapshotEveryMs ?? DEFAULT_SNAPSHOT_EVERY_MS);
    this.retainPreCheckpointSegments = Math.max(
      0,
      options.retainPreCheckpointSegments ?? DEFAULT_RETAIN_PRECHECKPOINT_SEGMENTS
    );
    this.orSetCompactTombstones = options.orSetCompactTombstones;
    this.lastSnapshotAtMs = this.now();
    this.metrics = options.metrics;
    this.clock = new HybridLogicalClock({
      nodeId: options.nodeId,
      now: this.now,
      metrics: this.metrics
    });
    if (options.onConflictEvent) {
      this.conflictEvents.subscribe(options.onConflictEvent);
    }

    const batchAppender: BatchedDurableAppender = {
      appendBatch: async operations => {
        await this.appendBatchToStore(operations as RuntimeOperation[]);
      }
    };

    this.batchWriter = new SegmentBatchWriter(batchAppender, {
      maxBatchSize: options.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE,
      flushIntervalMs: options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      metrics: this.metrics
    });

    this.writePipeline = new DurableWritePipeline(this.batchWriter, {
      apply: operation => this.applyOperation(operation as RuntimeOperation)
    });
  }

  async start(): Promise<void> {
    await this.recover();
  }

  async close(): Promise<void> {
    if (this.compactionPromise) {
      await this.compactionPromise;
    }
    await this.batchWriter.close();
  }

  async flush(): Promise<void> {
    await this.batchWriter.flush();
  }

  async setRegister(namespace: string, key: string, value: unknown): Promise<void> {
    const operation = this.makeOperation(namespace, key, "register.set", { value });
    await this.writePipeline.write(operation);
    await this.compactIfDue();
  }

  getRegister(namespace: string, key: string): unknown {
    return this.registers.get(composeKey(namespace, key));
  }

  async incrementCounter(namespace: string, key: string, amount = 1): Promise<void> {
    const operation = this.makeOperation(namespace, key, "counter.increment", { amount });
    await this.writePipeline.write(operation);
    await this.compactIfDue();
  }

  getCounter(namespace: string, key: string): number {
    return this.counters.get(composeKey(namespace, key)) ?? 0;
  }

  async recover(): Promise<RecoveryResult> {
    this.registers.clear();
    this.counters.clear();
    this.operationsSinceSnapshot = 0;

    const latestSnapshot = await this.store.loadLatestSnapshot();
    if (latestSnapshot?.state?.registers) {
      for (const [key, value] of Object.entries(latestSnapshot.state.registers)) {
        this.registers.set(key, value);
      }
    }
    if (latestSnapshot?.state?.counters) {
      for (const [key, value] of Object.entries(latestSnapshot.state.counters)) {
        this.counters.set(key, value);
      }
    }

    const manager = new RecoveryManager<RuntimeOperation>({
      manifestLoader: {
        loadManifest: async () => {
          const manifest = await this.ensureManifest();
          return manifest;
        }
      },
      bounds: {
        maxReplaySegments: this.maxReplaySegments,
        maxReplayOperations: this.maxReplayOperations
      },
      snapshotLoader: {
        loadLatest: async () => latestSnapshot
      },
      segmentReader: {
        readSegment: async segmentId => this.store.readSegment(segmentId)
      },
      apply: operation => this.applyOperation(operation, "recovery"),
      onDuplicateOperation: opId => {
        this.conflictEvents.emit({
          type: "replay.duplicate-operation",
          opId
        });
      }
    });

    const result = await manager.recover();
    this.metrics?.increment("statefabric.recovery_applied_ops_total", result.appliedCount);
    this.metrics?.increment(
      "statefabric.recovery_skipped_duplicates_total",
      result.skippedDuplicateCount
    );
    this.metrics?.increment("statefabric.recovery_segments_total", result.replayedSegments.length);
    if (result.truncatedBySegmentLimit) {
      this.metrics?.increment("statefabric.recovery_segment_bound_hits_total");
    }
    if (result.truncatedByOperationLimit) {
      this.metrics?.increment("statefabric.recovery_operation_bound_hits_total");
    }
    this.lastSnapshotAtMs = this.now();
    return result;
  }

  async rotateSegment(nextSegmentId: string): Promise<void> {
    const manifest = await this.ensureManifest();
    const next = activateSegment(manifest, nextSegmentId);
    this.manifestState = next;
    await this.store.saveManifest(next);
  }

  async compactNow(): Promise<RecoverySnapshot | null> {
    if (this.compactionPromise) {
      await this.compactionPromise;
    }
    await this.batchWriter.flush();
    const manifest = await this.ensureManifest();
    if (manifest.committedSegments.length === 0) {
      return null;
    }
    return this.compactFromManifest(manifest);
  }

  async gcOrphanSegments(options: SegmentGcOptions = {}): Promise<SegmentGcReport> {
    let dryRun = options.dryRun ?? false;
    const executionId = options.executionId;
    const reportStoreSupported =
      !!executionId &&
      !!this.store.loadSegmentGcReport &&
      !!this.store.saveSegmentGcReport;

    if (this.compactionPromise) {
      await this.compactionPromise;
    }
    await this.batchWriter.flush();

    const manifest = await this.ensureManifest();
    let referencedSegments = [
      ...new Set([...manifest.committedSegments, manifest.activeSegmentId])
    ].sort();

    if (!this.store.listSegments || !this.store.deleteSegment) {
      this.metrics?.increment("statefabric.segment_gc_unsupported_total");
      return {
        dryRun,
        supported: false,
        reason: "store-missing-hooks",
        executionId,
        replayedResult: false,
        referencedSegments,
        orphanSegments: [],
        deletedSegments: [],
        failedSegments: []
      };
    }

    const persisted = reportStoreSupported
      ? await this.store.loadSegmentGcReport!(executionId)
      : null;
    if (persisted?.status === "completed") {
      this.metrics?.increment("statefabric.segment_gc_replayed_total");
      return {
        ...persisted,
        replayedResult: true
      };
    }

    let orphanSegments: string[];
    let deletedSegments: string[];
    let failedSegments: string[];
    let remainingSegments: string[];
    let replayedResult = false;

    if (persisted?.status === "in_progress") {
      replayedResult = true;
      dryRun = persisted.dryRun;
      referencedSegments = [...persisted.referencedSegments];
      orphanSegments = [...persisted.orphanSegments].sort();
      deletedSegments = [...persisted.deletedSegments].sort();
      failedSegments = [...persisted.failedSegments].sort();
      remainingSegments = [...persisted.remainingSegments].sort();
      this.metrics?.increment("statefabric.segment_gc_resumed_total");
    } else {
      let listedSegments: string[];
      try {
        listedSegments = await this.store.listSegments!();
      } catch {
        this.metrics?.increment("statefabric.segment_gc_list_failures_total");
        return {
          dryRun,
          supported: false,
          reason: "list-failed",
          executionId,
          replayedResult: false,
          referencedSegments,
          orphanSegments: [],
          deletedSegments: [],
          failedSegments: []
        };
      }

      const referencedSet = new Set(referencedSegments);
      orphanSegments = listedSegments
        .filter(segmentId => !referencedSet.has(segmentId))
        .sort();
      deletedSegments = [];
      failedSegments = [];
      remainingSegments = [...orphanSegments];
    }

    if (orphanSegments.length === 0) {
      const report: SegmentGcReport = {
        dryRun,
        supported: true,
        executionId,
        replayedResult,
        referencedSegments,
        orphanSegments: [],
        deletedSegments: [],
        failedSegments: []
      };
      if (reportStoreSupported) {
        await this.store.saveSegmentGcReport!({
          ...report,
          executionId,
          status: "completed",
          remainingSegments: []
        });
      }
      return report;
    }

    this.metrics?.increment("statefabric.segment_gc_runs_total");
    if (dryRun) {
      this.metrics?.increment("statefabric.segment_gc_dry_run_total");
      const report: SegmentGcReport = {
        dryRun,
        supported: true,
        executionId,
        replayedResult,
        referencedSegments,
        orphanSegments,
        deletedSegments: [],
        failedSegments: []
      };
      if (reportStoreSupported) {
        await this.store.saveSegmentGcReport!({
          ...report,
          executionId,
          status: "completed",
          remainingSegments: []
        });
      }
      return report;
    }

    if (reportStoreSupported) {
      await this.store.saveSegmentGcReport!({
        dryRun,
        supported: true,
        executionId,
        replayedResult,
        referencedSegments,
        orphanSegments,
        deletedSegments,
        failedSegments,
        status: "in_progress",
        remainingSegments
      });
    }

    for (const segmentId of [...remainingSegments]) {
      try {
        await this.store.deleteSegment!(segmentId);
        deletedSegments.push(segmentId);
        failedSegments = failedSegments.filter(id => id !== segmentId);
        remainingSegments = remainingSegments.filter(id => id !== segmentId);
        this.metrics?.increment("statefabric.segment_gc_deleted_total");
      } catch {
        if (!failedSegments.includes(segmentId)) {
          failedSegments.push(segmentId);
        }
        this.metrics?.increment("statefabric.segment_gc_delete_failures_total");
      }

      if (reportStoreSupported) {
        await this.store.saveSegmentGcReport!({
          dryRun,
          supported: true,
          executionId,
          replayedResult,
          referencedSegments,
          orphanSegments,
          deletedSegments: [...new Set(deletedSegments)].sort(),
          failedSegments: [...new Set(failedSegments)].sort(),
          status: remainingSegments.length === 0 ? "completed" : "in_progress",
          remainingSegments
        });
      }
    }

    return {
      dryRun,
      supported: true,
      executionId,
      replayedResult,
      referencedSegments,
      orphanSegments,
      deletedSegments: [...new Set(deletedSegments)].sort(),
      failedSegments: [...new Set(failedSegments)].sort()
    };
  }

  private async appendBatchToStore(operations: RuntimeOperation[]): Promise<void> {
    if (operations.length === 0) {
      return;
    }

    const manifest = await this.ensureManifest();
    const segmentId = manifest.activeSegmentId;

    await this.store.appendToSegment(segmentId, operations);

    const maxSequence = operations.reduce((max, op) => Math.max(max, op.sequence), 0);
    const committed = commitSegment(manifest, {
      segmentId,
      nodeId: this.nodeId,
      sequence: maxSequence
    });

    this.manifestState = committed;
    await this.store.saveManifest(committed);
  }

  private async ensureManifest(): Promise<SegmentManifest> {
    if (this.manifestState) {
      return this.manifestState;
    }

    const loaded = await this.store.loadManifest();
    if (loaded) {
      this.manifestState = loaded;
      return loaded;
    }

    const created = createManifest({ activeSegmentId: this.defaultSegmentId });
    this.manifestState = created;
    await this.store.saveManifest(created);
    return created;
  }

  private async compactIfDue(): Promise<void> {
    if (this.compactionPromise) {
      await this.compactionPromise;
      return;
    }

    const manifest = await this.ensureManifest();
    if (this.compactionPromise) {
      await this.compactionPromise;
      return;
    }

    if (manifest.committedSegments.length === 0) {
      return;
    }

    const nowMs = this.now();
    const dueByOps = this.snapshotEveryOps > 0 && this.operationsSinceSnapshot >= this.snapshotEveryOps;
    const dueByTime = this.snapshotEveryMs > 0 && nowMs - this.lastSnapshotAtMs >= this.snapshotEveryMs;

    if (!dueByOps && !dueByTime) {
      return;
    }

    this.compactionPromise = (async () => {
      try {
        await this.compactFromManifest(manifest, nowMs);
      } catch {
        this.metrics?.increment("statefabric.snapshot_compaction_failures_total");
      } finally {
        this.compactionPromise = null;
      }
    })();

    await this.compactionPromise;
  }

  private async compactFromManifest(
    manifest: SegmentManifest,
    nowMs = this.now()
  ): Promise<RecoverySnapshot> {
    const checkpointSegmentId =
      manifest.committedSegments[manifest.committedSegments.length - 1];

    let afterRotation = manifest;
    if (manifest.activeSegmentId === checkpointSegmentId) {
      const nextSegmentId = deriveNextSegmentId(manifest, nowMs);
      afterRotation = activateSegment(manifest, nextSegmentId, () => new Date(nowMs));
    }

    const compacted = compactManifest(afterRotation, {
      checkpointSegmentId,
      retainPreCheckpointSegments: this.retainPreCheckpointSegments,
      now: () => new Date(nowMs)
    });
    const segmentsToPrune = manifest.committedSegments.filter(
      segmentId => !compacted.committedSegments.includes(segmentId)
    );

    const snapshot: RecoverySnapshot = {
      snapshotId: `snap-${this.nodeId}-${nowMs}-${this.sequence}`,
      checkpoint: {
        lastSegmentId: checkpointSegmentId
      },
      state: {
        registers: Object.fromEntries(this.registers.entries()),
        counters: Object.fromEntries(this.counters.entries())
      }
    };

    if (this.orSetCompactTombstones) {
      try {
        const compactedTombstones = this.orSetCompactTombstones();
        if (compactedTombstones > 0) {
          this.metrics?.increment(
            "statefabric.orset_tombstones_compacted_total",
            compactedTombstones
          );
        }
      } catch {
        this.metrics?.increment("statefabric.orset_tombstone_gc_failures_total");
      }
    }

    await this.store.saveLatestSnapshot(snapshot);
    await this.store.saveManifest(compacted);
    await this.pruneSegments(segmentsToPrune);

    this.manifestState = compacted;
    this.operationsSinceSnapshot = 0;
    this.lastSnapshotAtMs = nowMs;
    this.metrics?.increment("statefabric.snapshot_compactions_total");
    this.metrics?.observe(
      "statefabric.snapshot_retained_segments",
      compacted.committedSegments.length
    );

    return snapshot;
  }

  private async pruneSegments(segmentIds: string[]): Promise<void> {
    if (!this.store.deleteSegment || segmentIds.length === 0) {
      return;
    }

    let candidateIds = [...new Set(segmentIds)];
    if (this.store.listSegments) {
      try {
        const existing = new Set(await this.store.listSegments());
        candidateIds = candidateIds.filter(segmentId => existing.has(segmentId));
      } catch {
        this.metrics?.increment("statefabric.segment_gc_list_failures_total");
      }
    }

    if (candidateIds.length === 0) {
      return;
    }

    this.metrics?.increment("statefabric.segment_gc_runs_total");

    for (const segmentId of candidateIds) {
      try {
        await this.store.deleteSegment(segmentId);
        this.metrics?.increment("statefabric.segment_gc_deleted_total");
      } catch {
        this.metrics?.increment("statefabric.segment_gc_delete_failures_total");
      }
    }
  }

  private makeOperation(
    namespace: string,
    key: string,
    type: RuntimeOperationType,
    payload: RuntimeOperation["payload"]
  ): RuntimeOperation {
    this.sequence += 1;

    return {
      opId: `${this.nodeId}-${this.sequence}`,
      namespace,
      key,
      type,
      payload,
      hlc: this.clock.tick(),
      actorNodeId: this.nodeId,
      sequence: this.sequence
    };
  }

  private applyOperation(operation: RuntimeOperation, mode: "live" | "recovery" = "live"): void {
    const stateKey = composeKey(operation.namespace, operation.key);

    if (operation.type === "register.set") {
      this.registers.set(stateKey, operation.payload.value);
    } else if (operation.type === "counter.increment") {
      const amount = operation.payload.amount ?? 0;
      const current = this.counters.get(stateKey) ?? 0;
      this.counters.set(stateKey, current + amount);
    }

    if (mode === "live") {
      this.operationsSinceSnapshot += 1;
    }
  }

  onConflictEvent(listener: ConflictEventListener): () => void {
    return this.conflictEvents.subscribe(listener);
  }

  emitConflictEvent(event: ConflictEvent): void {
    this.conflictEvents.emit(event);
  }
}

function composeKey(namespace: string, key: string): string {
  return `${namespace}:${key}`;
}

function deriveNextSegmentId(manifest: SegmentManifest, nowMs: number): string {
  const current = manifest.activeSegmentId;
  const numericSuffix = current.match(/^(.*?)(\d+)$/);
  let candidate = numericSuffix
    ? `${numericSuffix[1]}${Number(numericSuffix[2]) + 1}`
    : `${current}-next-${nowMs}`;

  if (candidate === current || manifest.committedSegments.includes(candidate)) {
    candidate = `${candidate}-${nowMs}`;
  }

  return candidate;
}

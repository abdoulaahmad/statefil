import { describe, expect, it } from "vitest";
import {
  DurableEventualRuntime,
  InMemoryMetricsCollector,
  type DurableRuntimeStore,
  type RecoverySnapshot,
  type RuntimeOperation,
  type SegmentManifest
} from "../../src";

class InMemoryRuntimeStore implements DurableRuntimeStore {
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

  async listSegments(): Promise<string[]> {
    return Object.keys(this.segments).sort();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    delete this.segments[segmentId];
  }
}

describe("DurableEventualRuntime load behavior", () => {
  it(
    "keeps recovery bounded and sweeps orphan segments under high segment counts",
    async () => {
      const store = new InMemoryRuntimeStore();
      const metrics = new InMemoryMetricsCollector();
      const committedSegments: string[] = [];
      let sequence = 0;
      const committedSegmentCount = 120;
      const operationsPerSegment = 5;
      const orphanSegmentCount = 80;

      for (let i = 1; i <= committedSegmentCount; i += 1) {
        const segmentId = `seg-${i.toString().padStart(3, "0")}`;
        committedSegments.push(segmentId);
        store.segments[segmentId] = [];

        for (let opIndex = 0; opIndex < operationsPerSegment; opIndex += 1) {
          sequence += 1;
          store.segments[segmentId].push({
            opId: `op-${sequence}`,
            namespace: "rate",
            key: "user",
            type: "counter.increment",
            payload: { amount: 1 },
            actorNodeId: "node-a",
            sequence,
            hlc: { physical: 1_000 + sequence, logical: 0, nodeId: "node-a" }
          });
        }
      }

      for (let i = 1; i <= orphanSegmentCount; i += 1) {
        store.segments[`orphan-${i.toString().padStart(3, "0")}`] = [];
      }

      store.manifest = {
        version: "1.0",
        activeSegmentId: "seg-121",
        committedSegments,
        lastCommittedSequenceByNode: { "node-a": sequence },
        updatedAt: "2026-02-20T00:00:00.000Z"
      };
      store.segments["seg-121"] = [];

      const runtime = new DurableEventualRuntime(store, {
        nodeId: "node-b",
        maxBatchSize: 32,
        flushIntervalMs: 1_000,
        maxReplaySegments: 25,
        maxReplayOperations: 40,
        metrics
      });

      const startedAt = Date.now();
      const recovery = await runtime.recover();
      const gcReport = await runtime.gcOrphanSegments();
      const elapsedMs = Date.now() - startedAt;

      expect(recovery.appliedCount).toBe(40);
      expect(recovery.replayedSegments.length).toBe(25);
      expect(recovery.truncatedBySegmentLimit).toBe(true);
      expect(recovery.truncatedByOperationLimit).toBe(true);
      expect(metrics.counter("statefabric.recovery_segment_bound_hits_total")).toBe(1);
      expect(metrics.counter("statefabric.recovery_operation_bound_hits_total")).toBe(1);

      expect(gcReport.supported).toBe(true);
      expect(gcReport.orphanSegments.length).toBe(orphanSegmentCount);
      expect(gcReport.deletedSegments.length).toBe(orphanSegmentCount);
      expect(Object.keys(store.segments).length).toBe(committedSegmentCount + 1);
      expect(elapsedMs).toBeLessThan(2_000);

      await runtime.close();
    },
    15_000
  );
});

import { describe, expect, it } from "vitest";
import {
  UnifiedStateFabricRuntime,
  type DurableRuntimeStore,
  type RuntimeOperation,
  type SegmentManifest,
  type RecoverySnapshot
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
}

describe("UnifiedStateFabricRuntime", () => {
  it("exposes eventual and strong runtimes", async () => {
    const store = new InMemoryRuntimeStore();
    const runtime = new UnifiedStateFabricRuntime({
      eventualStore: store,
      eventual: {
        nodeId: "node-a",
        maxBatchSize: 2,
        flushIntervalMs: 1_000
      }
    });

    await runtime.eventual.setRegister("session", "user", "alice");
    await runtime.strong.setRegister("payment", "balance", 100);

    await expect(runtime.strong.getRegister<number>("payment", "balance")).resolves.toBe(100);
    expect(runtime.eventual.getRegister("session", "user")).toBe("alice");

    await runtime.close();
  });

  it("recovers eventual state across unified runtime restart", async () => {
    const store = new InMemoryRuntimeStore();

    const runtimeA = new UnifiedStateFabricRuntime({
      eventualStore: store,
      eventual: {
        nodeId: "node-a",
        maxBatchSize: 1,
        flushIntervalMs: 1_000
      }
    });

    await runtimeA.eventual.setRegister("profile", "name", "v1");
    await runtimeA.close();

    const runtimeB = new UnifiedStateFabricRuntime({
      eventualStore: store,
      eventual: {
        nodeId: "node-b",
        maxBatchSize: 1,
        flushIntervalMs: 1_000
      }
    });

    const recovery = await runtimeB.recover();

    expect(recovery.appliedCount).toBe(1);
    expect(runtimeB.eventual.getRegister("profile", "name")).toBe("v1");

    await runtimeB.close();
  });

  it("forwards orphan-segment GC to eventual runtime", async () => {
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
    store.segments["seg-old"] = [];

    const runtime = new UnifiedStateFabricRuntime({
      eventualStore: store,
      eventual: {
        nodeId: "node-a",
        maxBatchSize: 1,
        flushIntervalMs: 1_000
      }
    });

    const report = await runtime.gcOrphanSegments();

    expect(report.supported).toBe(false);
    await runtime.close();
  });
});

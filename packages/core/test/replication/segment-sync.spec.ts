import { describe, expect, it } from "vitest";
import {
  DurableEventualRuntime,
  SegmentSyncManager,
  InMemoryMetricsCollector,
  type DurableRuntimeStore,
  type RecoverySnapshot,
  type RuntimeOperation,
  type SegmentManifest,
  type StoredSegmentGcReport
} from "../../src";

class InMemoryRuntimeStore implements DurableRuntimeStore {
  readonly segments = new Map<string, RuntimeOperation[]>();
  manifest: SegmentManifest | null = null;
  snapshot: RecoverySnapshot | null = null;

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
    const existing = this.segments.get(segmentId) ?? [];
    existing.push(...operations);
    this.segments.set(segmentId, existing);
  }

  async readSegment(segmentId: string): Promise<RuntimeOperation[]> {
    return [...(this.segments.get(segmentId) ?? [])];
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    return this.snapshot;
  }

  async listSegments(): Promise<string[]> {
    return [...this.segments.keys()].sort();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    this.segments.delete(segmentId);
  }

  async loadSegmentGcReport(_executionId: string): Promise<StoredSegmentGcReport | null> {
    return null;
  }

  async saveSegmentGcReport(_report: StoredSegmentGcReport): Promise<void> {}
}

async function createNode(
  store: DurableRuntimeStore,
  nodeId: string,
  now: () => number
): Promise<DurableEventualRuntime> {
  const runtime = new DurableEventualRuntime(store, { nodeId, now });
  await runtime.start();
  return runtime;
}

function fixedClock(startMs: number, stepMs = 1): () => number {
  let current = startMs;
  return () => {
    const value = current;
    current += stepMs;
    return value;
  };
}

describe("SegmentSyncManager", () => {
  it("copies missing committed segments and merges manifests", async () => {
    const localStore = new InMemoryRuntimeStore();
    const remoteStore = new InMemoryRuntimeStore();
    const remote = await createNode(remoteStore, "node-b", fixedClock(1_000));

    await remote.setRegister("session", "u1", "alice");
    await remote.close();

    const sync = new SegmentSyncManager(localStore, remoteStore);
    const report = await sync.sync();

    expect(report.copiedSegments).toEqual(["seg-1"]);
    expect(report.copiedOperationCount).toBe(1);
    expect(report.mergedCommittedSegments).toContain("seg-1");
    expect(await localStore.readSegment("seg-1")).toEqual(await remoteStore.readSegment("seg-1"));
  });

  it("is idempotent: a second sync copies nothing", async () => {
    const localStore = new InMemoryRuntimeStore();
    const remoteStore = new InMemoryRuntimeStore();
    const remote = await createNode(remoteStore, "node-b", fixedClock(1_000));
    await remote.setRegister("session", "u1", "alice");
    await remote.close();

    const sync = new SegmentSyncManager(localStore, remoteStore);
    await sync.sync();
    const second = await sync.sync();

    expect(second.copiedSegments).toEqual([]);
    expect(second.skippedSegments).toEqual(["seg-1"]);
    expect(second.copiedOperationCount).toBe(0);
  });

  it("skips segments not committed in the remote manifest", async () => {
    const localStore = new InMemoryRuntimeStore();
    const remoteStore = new InMemoryRuntimeStore();

    await remoteStore.appendToSegment("seg-open", [
      {
        opId: "node-b-1",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "bob" },
        hlc: { physical: 100, logical: 0, nodeId: "node-b" },
        actorNodeId: "node-b",
        sequence: 1
      }
    ]);
    remoteStore.manifest = {
      version: "1.0",
      activeSegmentId: "seg-open",
      committedSegments: [],
      lastCommittedSequenceByNode: {},
      updatedAt: new Date().toISOString()
    };

    const report = await new SegmentSyncManager(localStore, remoteStore).sync();

    expect(report.copiedSegments).toEqual([]);
    expect(report.skippedSegments).toEqual(["seg-open"]);
    expect(await localStore.readSegment("seg-open")).toEqual([]);
  });

  it("converges two runtimes after bidirectional sync and recover", async () => {
    const storeA = new InMemoryRuntimeStore();
    const storeB = new InMemoryRuntimeStore();
    const nodeA = await createNode(storeA, "node-a", fixedClock(2_000));
    const nodeB = await createNode(storeB, "node-b", fixedClock(4_000));

    await nodeA.setRegister("session", "u1", "from-a");
    await nodeB.incrementCounter("rate", "u1", 3);
    await nodeA.close();
    await nodeB.close();

    await new SegmentSyncManager(storeA, storeB).sync();
    await new SegmentSyncManager(storeB, storeA).sync();

    const recoveredA = await createNode(storeA, "node-a", fixedClock(10_000));
    const recoveredB = await createNode(storeB, "node-b", fixedClock(10_000));

    expect(recoveredA.getRegister("session", "u1")).toBe("from-a");
    expect(recoveredB.getRegister("session", "u1")).toBe("from-a");
    expect(recoveredA.getCounter("rate", "u1")).toBe(3);
    expect(recoveredB.getCounter("rate", "u1")).toBe(3);
  });

  it("records sync metrics for copied work", async () => {
    const localStore = new InMemoryRuntimeStore();
    const remoteStore = new InMemoryRuntimeStore();
    const remote = await createNode(remoteStore, "node-b", fixedClock(1_000));
    await remote.setRegister("session", "u1", "alice");
    await remote.close();

    const metrics = new InMemoryMetricsCollector();
    await new SegmentSyncManager(localStore, remoteStore, { metrics }).sync();

    expect(metrics.counter("statefabric.sync_copied_segments_total")).toBe(1);
    expect(metrics.observed("statefabric.sync_copied_operations")).toEqual([1]);
  });

  it("rejects stores without listSegments support", () => {
    const store = new InMemoryRuntimeStore();
    const bare: DurableRuntimeStore = {
      loadManifest: store.loadManifest.bind(store),
      saveManifest: store.saveManifest.bind(store),
      saveLatestSnapshot: store.saveLatestSnapshot.bind(store),
      appendToSegment: store.appendToSegment.bind(store),
      readSegment: store.readSegment.bind(store),
      loadLatestSnapshot: store.loadLatestSnapshot.bind(store)
    };

    expect(() => new SegmentSyncManager(bare, store)).toThrow();
  });
});

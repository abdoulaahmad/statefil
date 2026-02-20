import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  DurableEventualRuntime,
  MANIFEST_DOCUMENT_SCHEMA,
  OPERATION_DOCUMENT_SCHEMA
} from "@statefabric/core";
import { LocalDurableRuntimeStore, LocalPersistenceAdapter } from "../src";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map(dir => rm(dir, { recursive: true, force: true })));
});

async function createStore(): Promise<LocalDurableRuntimeStore> {
  const root = await mkdtemp(path.join(tmpdir(), "statefabric-runtime-"));
  tempDirs.push(root);

  const adapter = new LocalPersistenceAdapter({ rootDir: root });
  return new LocalDurableRuntimeStore(adapter);
}

async function createStoreWithRoot(): Promise<{ store: LocalDurableRuntimeStore; rootDir: string }> {
  const root = await mkdtemp(path.join(tmpdir(), "statefabric-runtime-"));
  tempDirs.push(root);

  const adapter = new LocalPersistenceAdapter({ rootDir: root });
  return {
    store: new LocalDurableRuntimeStore(adapter),
    rootDir: root
  };
}

describe("LocalDurableRuntimeStore integration", () => {
  it("persists state across runtime restart", async () => {
    const store = await createStore();

    const runtimeA = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 2,
      flushIntervalMs: 1_000
    });

    await Promise.all([
      runtimeA.setRegister("session", "user", "alice"),
      runtimeA.incrementCounter("rate", "user", 3)
    ]);

    await runtimeA.flush();
    await runtimeA.close();

    const runtimeB = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 10,
      flushIntervalMs: 1_000
    });

    const recovery = await runtimeB.recover();

    expect(recovery.appliedCount).toBe(2);
    expect(runtimeB.getRegister("session", "user")).toBe("alice");
    expect(runtimeB.getCounter("rate", "user")).toBe(3);

    await runtimeB.close();
  });

  it("recovers from 1.0 enveloped operation logs", async () => {
    const { store, rootDir } = await createStoreWithRoot();
    await store.loadManifest();

    await writeFile(
      path.join(rootDir, "manifest.json"),
      JSON.stringify({
        schema: MANIFEST_DOCUMENT_SCHEMA,
        version: "1.0",
        data: {
          version: "1.0",
          activeSegmentId: "seg-1",
          committedSegments: ["seg-1"],
          lastCommittedSequenceByNode: { "node-a": 2 },
          updatedAt: "2026-02-20T00:00:00.000Z"
        }
      }),
      "utf8"
    );

    await writeFile(
      path.join(rootDir, "segments", "seg-1.jsonl"),
      [
        JSON.stringify({
          schema: OPERATION_DOCUMENT_SCHEMA,
          version: "1.0",
          data: {
            opId: "op-1",
            namespace: "session",
            key: "user",
            type: "register.set",
            payload: { value: "alice" },
            actorNodeId: "node-a",
            sequence: 1,
            hlc: { physical: 100, logical: 0, nodeId: "node-a" }
          }
        }),
        JSON.stringify({
          schema: OPERATION_DOCUMENT_SCHEMA,
          version: "1.0",
          data: {
            opId: "op-2",
            namespace: "rate",
            key: "user",
            type: "counter.increment",
            payload: { amount: 3 },
            actorNodeId: "node-a",
            sequence: 2,
            hlc: { physical: 101, logical: 0, nodeId: "node-a" }
          }
        })
      ].join("\n") + "\n",
      "utf8"
    );

    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-b",
      maxBatchSize: 10,
      flushIntervalMs: 1_000
    });

    const recovery = await runtime.recover();

    expect(recovery.appliedCount).toBe(2);
    expect(runtime.getRegister("session", "user")).toBe("alice");
    expect(runtime.getCounter("rate", "user")).toBe(3);

    await runtime.close();
  });

  it("replays completed orphan-GC result for the same executionId", async () => {
    const store = await createStore();
    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    await runtime.setRegister("session", "user", "alice");
    await runtime.rotateSegment("seg-2");
    await runtime.setRegister("session", "user", "bob");
    await store.appendToSegment("seg-orphan", [
      {
        opId: "orphan-1",
        namespace: "orphan",
        key: "k",
        type: "register.set",
        payload: { value: "v" },
        actorNodeId: "node-x",
        sequence: 1,
        hlc: { physical: 999, logical: 0, nodeId: "node-x" }
      }
    ]);

    const first = await runtime.gcOrphanSegments({ executionId: "gc-local-1" });
    const second = await runtime.gcOrphanSegments({ executionId: "gc-local-1" });
    const persisted = await store.loadSegmentGcReport?.("gc-local-1");

    expect(first.replayedResult).toBe(false);
    expect(second.replayedResult).toBe(true);
    expect(first.deletedSegments).toContain("seg-orphan");
    expect(second.deletedSegments).toContain("seg-orphan");
    expect(persisted?.status).toBe("completed");
    expect(persisted?.remainingSegments).toEqual([]);

    await runtime.close();
  });

  it("resumes in-progress orphan-GC report and completes remaining segments", async () => {
    const store = await createStore();
    const runtime = new DurableEventualRuntime(store, {
      nodeId: "node-a",
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    await runtime.setRegister("session", "user", "alice");
    await runtime.rotateSegment("seg-2");
    await runtime.setRegister("session", "user", "bob");
    await store.appendToSegment("seg-orphan-a", [
      {
        opId: "orphan-a",
        namespace: "orphan",
        key: "a",
        type: "register.set",
        payload: { value: "a" },
        actorNodeId: "node-x",
        sequence: 1,
        hlc: { physical: 998, logical: 0, nodeId: "node-x" }
      }
    ]);
    await store.appendToSegment("seg-orphan-b", [
      {
        opId: "orphan-b",
        namespace: "orphan",
        key: "b",
        type: "register.set",
        payload: { value: "b" },
        actorNodeId: "node-x",
        sequence: 2,
        hlc: { physical: 999, logical: 0, nodeId: "node-x" }
      }
    ]);

    await store.saveSegmentGcReport?.({
      executionId: "gc-local-2",
      status: "in_progress",
      dryRun: false,
      supported: true,
      replayedResult: false,
      referencedSegments: ["seg-1", "seg-2"],
      orphanSegments: ["seg-orphan-a", "seg-orphan-b"],
      deletedSegments: ["seg-orphan-a"],
      failedSegments: [],
      remainingSegments: ["seg-orphan-b"]
    });

    const resumed = await runtime.gcOrphanSegments({ executionId: "gc-local-2" });
    const completed = await store.loadSegmentGcReport?.("gc-local-2");

    expect(resumed.replayedResult).toBe(true);
    expect(resumed.deletedSegments).toEqual(["seg-orphan-a", "seg-orphan-b"]);
    expect(resumed.failedSegments).toEqual([]);
    expect(completed?.status).toBe("completed");
    expect(completed?.remainingSegments).toEqual([]);

    await runtime.close();
  });
});

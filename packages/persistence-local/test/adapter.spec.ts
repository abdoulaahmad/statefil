import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { LocalPersistenceAdapter } from "../src";
import {
  MANIFEST_DOCUMENT_SCHEMA,
  MANIFEST_DOCUMENT_VERSION,
  OPERATION_DOCUMENT_SCHEMA,
  OPERATION_DOCUMENT_VERSION,
  SNAPSHOT_DOCUMENT_SCHEMA,
  SNAPSHOT_DOCUMENT_VERSION,
  type RecoverySnapshot,
  type StoredSegmentGcReport,
  type SegmentManifest,
  type RuntimeOperation
} from "@statefabric/core";

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map(dir => rm(dir, { recursive: true, force: true })));
});

async function createAdapter(): Promise<{ adapter: LocalPersistenceAdapter; rootDir: string }> {
  const root = await mkdtemp(path.join(tmpdir(), "statefabric-local-"));
  tempDirs.push(root);
  return {
    adapter: new LocalPersistenceAdapter({ rootDir: root }),
    rootDir: root
  };
}

describe("LocalPersistenceAdapter", () => {
  it("appends and reads raw segment content", async () => {
    const { adapter } = await createAdapter();

    await adapter.appendSegment("seg-1", "line-1\n");
    await adapter.appendSegment("seg-1", "line-2\n");

    const content = await adapter.readSegment("seg-1");

    expect(content).toBe("line-1\nline-2\n");
  });

  it("lists and deletes segments", async () => {
    const { adapter } = await createAdapter();

    await adapter.appendSegment("seg-2", "line-a\n");
    await adapter.appendSegment("seg-1", "line-b\n");

    await expect(adapter.listSegments()).resolves.toEqual(["seg-1", "seg-2"]);

    await adapter.deleteSegment("seg-1");
    await adapter.deleteSegment("seg-missing");

    await expect(adapter.listSegments()).resolves.toEqual(["seg-2"]);
    await expect(adapter.readSegment("seg-1")).resolves.toBeNull();
  });

  it("saves and loads segment GC execution reports", async () => {
    const { adapter } = await createAdapter();
    const report: StoredSegmentGcReport = {
      executionId: "gc-run-1",
      status: "in_progress",
      dryRun: false,
      supported: true,
      replayedResult: false,
      referencedSegments: ["seg-2", "seg-3"],
      orphanSegments: ["seg-1"],
      deletedSegments: [],
      failedSegments: [],
      remainingSegments: ["seg-1"]
    };

    await adapter.saveSegmentGcReport(report);
    await expect(adapter.loadSegmentGcReport("gc-run-1")).resolves.toEqual(report);
    await expect(adapter.loadSegmentGcReport("missing")).resolves.toBeNull();
  });

  it("saves and loads manifest", async () => {
    const { adapter } = await createAdapter();

    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: { "node-a": 5 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    await adapter.saveManifest(manifest);
    const loaded = await adapter.loadManifest();

    expect(loaded).toEqual(manifest);
  });

  it("saves and loads latest snapshot", async () => {
    const { adapter } = await createAdapter();

    const snapshot: RecoverySnapshot = {
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-3" }
    };

    await adapter.saveLatestSnapshot(snapshot);
    const loaded = await adapter.loadLatestSnapshot();

    expect(loaded).toEqual(snapshot);
  });

  it("appends and reads segment operations as JSONL", async () => {
    const { adapter } = await createAdapter();

    const operations: RuntimeOperation[] = [
      {
        opId: "op-1",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "alice" },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      },
      {
        opId: "op-2",
        namespace: "rate",
        key: "u1",
        type: "counter.increment",
        payload: { amount: 2 },
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "node-a" }
      }
    ];

    await adapter.appendSegmentOperations("seg-1", operations);
    const loaded = await adapter.readSegmentOperations("seg-1");

    expect(loaded).toEqual(operations);
  });

  it("reads legacy unversioned manifest/snapshot/operations", async () => {
    const { adapter, rootDir } = await createAdapter();
    await adapter.readSegment("seed");

    await writeFile(
      path.join(rootDir, "manifest.json"),
      JSON.stringify({
        version: "1.0",
        activeSegmentId: "seg-1",
        committedSegments: ["seg-1"],
        lastCommittedSequenceByNode: { "node-a": 2 },
        updatedAt: "2026-02-20T00:00:00.000Z"
      }),
      "utf8"
    );

    await writeFile(
      path.join(rootDir, "snapshots", "latest.json"),
      JSON.stringify({
        snapshotId: "snap-legacy",
        checkpoint: { lastSegmentId: "seg-1" }
      }),
      "utf8"
    );

    await writeFile(
      path.join(rootDir, "segments", "seg-1.jsonl"),
      `${JSON.stringify({
        opId: "op-legacy",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "legacy" },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      })}\n`,
      "utf8"
    );

    await expect(adapter.loadManifest()).resolves.toEqual({
      version: "1.0",
      activeSegmentId: "seg-1",
      committedSegments: ["seg-1"],
      lastCommittedSequenceByNode: { "node-a": 2 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    });

    await expect(adapter.loadLatestSnapshot()).resolves.toEqual({
      snapshotId: "snap-legacy",
      checkpoint: { lastSegmentId: "seg-1" }
    });

    await expect(adapter.readSegmentOperations("seg-1")).resolves.toEqual([
      {
        opId: "op-legacy",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "legacy" },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      }
    ]);
  });

  it("writes manifest/snapshot/operations using 1.1 envelopes", async () => {
    const { adapter, rootDir } = await createAdapter();

    await adapter.saveManifest({
      version: "1.0",
      activeSegmentId: "seg-1",
      committedSegments: [],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    });
    await adapter.saveLatestSnapshot({
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-1" }
    });
    await adapter.appendSegmentOperations("seg-1", [
      {
        opId: "op-1",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "alice" },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      }
    ]);

    const manifestRaw = await readFile(path.join(rootDir, "manifest.json"), "utf8");
    const snapshotRaw = await readFile(path.join(rootDir, "snapshots", "latest.json"), "utf8");
    const segmentRaw = await readFile(path.join(rootDir, "segments", "seg-1.jsonl"), "utf8");

    const manifestEnvelope = JSON.parse(manifestRaw) as { schema: string; version: string };
    const snapshotEnvelope = JSON.parse(snapshotRaw) as { schema: string; version: string };
    const operationEnvelope = JSON.parse(segmentRaw.trim()) as { schema: string; version: string };

    expect(manifestEnvelope.schema).toBe(MANIFEST_DOCUMENT_SCHEMA);
    expect(manifestEnvelope.version).toBe(MANIFEST_DOCUMENT_VERSION);
    expect(snapshotEnvelope.schema).toBe(SNAPSHOT_DOCUMENT_SCHEMA);
    expect(snapshotEnvelope.version).toBe(SNAPSHOT_DOCUMENT_VERSION);
    expect(operationEnvelope.schema).toBe(OPERATION_DOCUMENT_SCHEMA);
    expect(operationEnvelope.version).toBe(OPERATION_DOCUMENT_VERSION);
  });
});

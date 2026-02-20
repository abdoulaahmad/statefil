import { describe, expect, it } from "vitest";
import {
  MANIFEST_DOCUMENT_SCHEMA,
  MANIFEST_DOCUMENT_VERSION,
  OPERATION_DOCUMENT_SCHEMA,
  OPERATION_DOCUMENT_VERSION,
  SNAPSHOT_DOCUMENT_SCHEMA,
  SNAPSHOT_DOCUMENT_VERSION,
  type RecoverySnapshot,
  type StoredSegmentGcReport,
  type RuntimeOperation,
  type SegmentManifest
} from "@statefabric/core";
import { S3PersistenceAdapter, type S3ClientLike } from "../src";

class InMemoryS3Client implements S3ClientLike {
  private readonly objects = new Map<string, string>();

  async putObject(input: { Bucket: string; Key: string; Body: string }): Promise<void> {
    this.objects.set(this.objectKey(input.Bucket, input.Key), input.Body);
  }

  async getObject(input: { Bucket: string; Key: string }): Promise<{ Body: string }> {
    const fullKey = this.objectKey(input.Bucket, input.Key);
    const body = this.objects.get(fullKey);
    if (typeof body === "undefined") {
      const error = new Error("NoSuchKey");
      (error as Error & { code?: string }).code = "NoSuchKey";
      throw error;
    }

    return { Body: body };
  }

  async deleteObject(input: { Bucket: string; Key: string }): Promise<void> {
    this.objects.delete(this.objectKey(input.Bucket, input.Key));
  }

  async listObjects(input: { Bucket: string; Prefix: string }): Promise<{ Keys: string[] }> {
    const root = `${input.Bucket}/${input.Prefix}`;
    const keys = [...this.objects.keys()]
      .filter(key => key.startsWith(root))
      .map(full => full.slice(`${input.Bucket}/`.length));
    return { Keys: keys };
  }

  private objectKey(bucket: string, key: string): string {
    return `${bucket}/${key}`;
  }

  readRawObject(bucket: string, key: string): string | undefined {
    return this.objects.get(this.objectKey(bucket, key));
  }
}

describe("S3PersistenceAdapter", () => {
  it("appends and reads raw segment content", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

    await adapter.appendSegment("seg-1", "line-1\n");
    await adapter.appendSegment("seg-1", "line-2\n");

    const content = await adapter.readSegment("seg-1");

    expect(content).toBe("line-1\nline-2\n");
  });

  it("lists and deletes segments", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

    await adapter.appendSegment("seg-2", "line-a\n");
    await adapter.appendSegment("seg-1", "line-b\n");

    await expect(adapter.listSegments()).resolves.toEqual(["seg-1", "seg-2"]);

    await adapter.deleteSegment("seg-1");
    await adapter.deleteSegment("seg-missing");

    await expect(adapter.listSegments()).resolves.toEqual(["seg-2"]);
    await expect(adapter.readSegment("seg-1")).resolves.toBeNull();
  });

  it("gracefully no-ops when list/delete APIs are unavailable", async () => {
    const objects = new Map<string, string>();
    const client: S3ClientLike = {
      async putObject(input: { Bucket: string; Key: string; Body: string }) {
        objects.set(`${input.Bucket}/${input.Key}`, input.Body);
      },
      async getObject(input: { Bucket: string; Key: string }) {
        const key = `${input.Bucket}/${input.Key}`;
        const body = objects.get(key);
        if (typeof body === "undefined") {
          const error = new Error("NoSuchKey");
          (error as Error & { code?: string }).code = "NoSuchKey";
          throw error;
        }
        return { Body: body };
      }
    };

    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

    await adapter.appendSegment("seg-1", "line-a\n");

    await expect(adapter.listSegments()).resolves.toEqual([]);
    await expect(adapter.deleteSegment("seg-1")).resolves.toBeUndefined();
    await expect(adapter.readSegment("seg-1")).resolves.toBe("line-a\n");
  });

  it("saves and loads segment GC execution reports", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });
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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

    const snapshot: RecoverySnapshot = {
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-3" }
    };

    await adapter.saveLatestSnapshot(snapshot);
    const loaded = await adapter.loadLatestSnapshot();

    expect(loaded).toEqual(snapshot);
  });

  it("appends and reads segment operations as JSONL", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

    await client.putObject({
      Bucket: "b1",
      Key: "statefabric/manifest.json",
      Body: JSON.stringify({
        version: "1.0",
        activeSegmentId: "seg-1",
        committedSegments: ["seg-1"],
        lastCommittedSequenceByNode: { "node-a": 2 },
        updatedAt: "2026-02-20T00:00:00.000Z"
      })
    });

    await client.putObject({
      Bucket: "b1",
      Key: "statefabric/snapshots/latest.json",
      Body: JSON.stringify({
        snapshotId: "snap-legacy",
        checkpoint: { lastSegmentId: "seg-1" }
      })
    });

    await client.putObject({
      Bucket: "b1",
      Key: "statefabric/segments/seg-1.jsonl",
      Body: `${JSON.stringify({
        opId: "op-legacy",
        namespace: "session",
        key: "u1",
        type: "register.set",
        payload: { value: "legacy" },
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      })}\n`
    });

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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "b1",
      prefix: "statefabric"
    });

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

    const manifestRaw = client.readRawObject("b1", "statefabric/manifest.json");
    const snapshotRaw = client.readRawObject("b1", "statefabric/snapshots/latest.json");
    const operationRaw = client.readRawObject("b1", "statefabric/segments/seg-1.jsonl");

    expect(manifestRaw).toBeDefined();
    expect(snapshotRaw).toBeDefined();
    expect(operationRaw).toBeDefined();

    const manifestEnvelope = JSON.parse(manifestRaw!) as { schema: string; version: string };
    const snapshotEnvelope = JSON.parse(snapshotRaw!) as { schema: string; version: string };
    const operationEnvelope = JSON.parse(operationRaw!.trim()) as { schema: string; version: string };

    expect(manifestEnvelope.schema).toBe(MANIFEST_DOCUMENT_SCHEMA);
    expect(manifestEnvelope.version).toBe(MANIFEST_DOCUMENT_VERSION);
    expect(snapshotEnvelope.schema).toBe(SNAPSHOT_DOCUMENT_SCHEMA);
    expect(snapshotEnvelope.version).toBe(SNAPSHOT_DOCUMENT_VERSION);
    expect(operationEnvelope.schema).toBe(OPERATION_DOCUMENT_SCHEMA);
    expect(operationEnvelope.version).toBe(OPERATION_DOCUMENT_VERSION);
  });
});

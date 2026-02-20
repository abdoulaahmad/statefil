import { describe, expect, it } from "vitest";
import {
  DurableEventualRuntime,
  MANIFEST_DOCUMENT_SCHEMA,
  OPERATION_DOCUMENT_SCHEMA
} from "@statefabric/core";
import { S3DurableRuntimeStore, S3PersistenceAdapter, type S3ClientLike } from "../src";

class InMemoryS3Client implements S3ClientLike {
  private readonly objects = new Map<string, string>();

  async putObject(input: { Bucket: string; Key: string; Body: string }): Promise<void> {
    this.objects.set(`${input.Bucket}/${input.Key}`, input.Body);
  }

  async getObject(input: { Bucket: string; Key: string }): Promise<{ Body: string }> {
    const key = `${input.Bucket}/${input.Key}`;
    const body = this.objects.get(key);
    if (typeof body === "undefined") {
      const error = new Error("NoSuchKey");
      (error as Error & { code?: string }).code = "NoSuchKey";
      throw error;
    }
    return { Body: body };
  }

  async deleteObject(input: { Bucket: string; Key: string }): Promise<void> {
    this.objects.delete(`${input.Bucket}/${input.Key}`);
  }

  async listObjects(input: { Bucket: string; Prefix: string }): Promise<{ Keys: string[] }> {
    const prefix = `${input.Bucket}/${input.Prefix}`;
    return {
      Keys: [...this.objects.keys()]
        .filter(key => key.startsWith(prefix))
        .map(full => full.slice(`${input.Bucket}/`.length))
    };
  }
}

describe("S3DurableRuntimeStore integration", () => {
  it("persists and recovers state across runtime restart", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "bucket-a",
      prefix: "sf-test"
    });
    const store = new S3DurableRuntimeStore(adapter);

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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "bucket-a",
      prefix: "sf-test"
    });
    const store = new S3DurableRuntimeStore(adapter);

    await client.putObject({
      Bucket: "bucket-a",
      Key: "sf-test/manifest.json",
      Body: JSON.stringify({
        schema: MANIFEST_DOCUMENT_SCHEMA,
        version: "1.0",
        data: {
          version: "1.0",
          activeSegmentId: "seg-1",
          committedSegments: ["seg-1"],
          lastCommittedSequenceByNode: { "node-a": 2 },
          updatedAt: "2026-02-20T00:00:00.000Z"
        }
      })
    });

    await client.putObject({
      Bucket: "bucket-a",
      Key: "sf-test/segments/seg-1.jsonl",
      Body:
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
        ].join("\n") + "\n"
    });

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
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "bucket-a",
      prefix: "sf-test"
    });
    const store = new S3DurableRuntimeStore(adapter);
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

    const first = await runtime.gcOrphanSegments({ executionId: "gc-s3-1" });
    const second = await runtime.gcOrphanSegments({ executionId: "gc-s3-1" });
    const persisted = await store.loadSegmentGcReport?.("gc-s3-1");

    expect(first.replayedResult).toBe(false);
    expect(second.replayedResult).toBe(true);
    expect(first.deletedSegments).toContain("seg-orphan");
    expect(second.deletedSegments).toContain("seg-orphan");
    expect(persisted?.status).toBe("completed");
    expect(persisted?.remainingSegments).toEqual([]);

    await runtime.close();
  });

  it("resumes in-progress orphan-GC report and completes remaining segments", async () => {
    const client = new InMemoryS3Client();
    const adapter = new S3PersistenceAdapter(client, {
      bucket: "bucket-a",
      prefix: "sf-test"
    });
    const store = new S3DurableRuntimeStore(adapter);
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
      executionId: "gc-s3-2",
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

    const resumed = await runtime.gcOrphanSegments({ executionId: "gc-s3-2" });
    const completed = await store.loadSegmentGcReport?.("gc-s3-2");

    expect(resumed.replayedResult).toBe(true);
    expect(resumed.deletedSegments).toEqual(["seg-orphan-a", "seg-orphan-b"]);
    expect(resumed.failedSegments).toEqual([]);
    expect(completed?.status).toBe("completed");
    expect(completed?.remainingSegments).toEqual([]);

    await runtime.close();
  });
});

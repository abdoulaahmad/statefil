import { describe, expect, it, vi } from "vitest";
import type { OperationEnvelope } from "../../src/log";
import { SegmentBatchWriter, type BatchedDurableAppender } from "../../src/log";
import { InMemoryMetricsCollector } from "../../src";

interface Deferred<T> {
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
  promise: Promise<T>;
}

class RecordingBatchAppender implements BatchedDurableAppender {
  readonly batches: string[][] = [];

  async appendBatch(operations: OperationEnvelope[]): Promise<void> {
    this.batches.push(operations.map(op => op.opId));
  }
}

class DeferredBatchAppender implements BatchedDurableAppender {
  readonly batches: string[][] = [];
  readonly deferreds: Deferred<void>[] = [];

  async appendBatch(operations: OperationEnvelope[]): Promise<void> {
    this.batches.push(operations.map(op => op.opId));
    const deferred = createDeferred<void>();
    this.deferreds.push(deferred);
    return deferred.promise;
  }
}

function op(opId: string): OperationEnvelope {
  return {
    opId,
    namespace: "session",
    key: opId,
    type: "set",
    payload: { value: opId }
  };
}

describe("SegmentBatchWriter", () => {
  it("flushes by max batch size", async () => {
    const sink = new RecordingBatchAppender();
    const metrics = new InMemoryMetricsCollector();
    const writer = new SegmentBatchWriter(sink, {
      maxBatchSize: 2,
      flushIntervalMs: 1_000,
      metrics
    });

    const p1 = writer.append(op("op-1"));
    const p2 = writer.append(op("op-2"));

    await Promise.all([p1, p2]);

    expect(sink.batches).toEqual([["op-1", "op-2"]]);
    expect(metrics.counter("statefabric.batch_flush_total")).toBe(1);
    expect(metrics.observed("statefabric.batch_size")).toEqual([2]);
  });

  it("flushes by timer when batch size is not reached", async () => {
    vi.useFakeTimers();
    try {
      const sink = new RecordingBatchAppender();
      const writer = new SegmentBatchWriter(sink, {
        maxBatchSize: 10,
        flushIntervalMs: 100
      });

      const p = writer.append(op("op-timeout"));

      await vi.advanceTimersByTimeAsync(100);
      await p;

      expect(sink.batches).toEqual([["op-timeout"]]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("dedupes concurrent flush calls during in-flight drain", async () => {
    const sink = new DeferredBatchAppender();
    const writer = new SegmentBatchWriter(sink, {
      maxBatchSize: 10,
      flushIntervalMs: 1_000
    });

    const appendPromise = writer.append(op("op-a"));
    const flush1 = writer.flush();
    const flush2 = writer.flush();

    expect(flush1).toBe(flush2);
    expect(sink.batches).toEqual([["op-a"]]);

    sink.deferreds[0].resolve();
    await Promise.all([appendPromise, flush1]);
  });

  it("does not resolve append ack before flush completes", async () => {
    const sink = new DeferredBatchAppender();
    const writer = new SegmentBatchWriter(sink, {
      maxBatchSize: 1,
      flushIntervalMs: 1_000
    });

    let resolved = false;
    const p = writer.append(op("op-ack"));
    p.then(() => {
      resolved = true;
    });

    await Promise.resolve();
    expect(resolved).toBe(false);

    sink.deferreds[0].resolve();
    await p;

    expect(resolved).toBe(true);
  });

  it("rejects pending appends when batch sink fails", async () => {
    const failure = new Error("sink down");
    const metrics = new InMemoryMetricsCollector();

    const sink: BatchedDurableAppender = {
      async appendBatch(): Promise<void> {
        throw failure;
      }
    };

    const writer = new SegmentBatchWriter(sink, {
      maxBatchSize: 1,
      flushIntervalMs: 1_000,
      metrics
    });

    await expect(writer.append(op("op-fail"))).rejects.toThrow("sink down");
    expect(metrics.counter("statefabric.batch_flush_error_total")).toBe(1);
  });
});

function createDeferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"];
  let reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });

  return { resolve, reject, promise };
}

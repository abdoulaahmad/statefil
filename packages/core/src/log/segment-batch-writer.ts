import type { DurableAppender, OperationEnvelope } from "./types";
import type { MetricsCollector } from "../observability";

interface Deferred<T> {
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
  promise: Promise<T>;
}

interface PendingOperation {
  operation: OperationEnvelope;
  deferred: Deferred<void>;
}

export interface BatchedDurableAppender {
  appendBatch(operations: OperationEnvelope[]): Promise<void>;
}

export interface SegmentBatchWriterOptions {
  maxBatchSize: number;
  flushIntervalMs: number;
  setTimeoutFn?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimeoutFn?: (id: ReturnType<typeof setTimeout>) => void;
  metrics?: MetricsCollector;
}

export class SegmentBatchWriter implements DurableAppender {
  private readonly maxBatchSize: number;
  private readonly flushIntervalMs: number;
  private readonly setTimeoutFn: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  private readonly clearTimeoutFn: (id: ReturnType<typeof setTimeout>) => void;
  private readonly batchAppender: BatchedDurableAppender;
  private readonly metrics?: MetricsCollector;

  private queue: PendingOperation[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private drainPromise: Promise<void> | null = null;
  private closed = false;

  constructor(batchAppender: BatchedDurableAppender, options: SegmentBatchWriterOptions) {
    if (options.maxBatchSize < 1) {
      throw new Error("maxBatchSize must be >= 1");
    }
    if (options.flushIntervalMs < 1) {
      throw new Error("flushIntervalMs must be >= 1");
    }

    this.batchAppender = batchAppender;
    this.maxBatchSize = options.maxBatchSize;
    this.flushIntervalMs = options.flushIntervalMs;
    this.setTimeoutFn = options.setTimeoutFn ?? setTimeout;
    this.clearTimeoutFn = options.clearTimeoutFn ?? clearTimeout;
    this.metrics = options.metrics;
  }

  async append(operation: OperationEnvelope): Promise<void> {
    if (this.closed) {
      throw new Error("SegmentBatchWriter is closed");
    }

    const deferred = createDeferred<void>();
    this.queue.push({ operation, deferred });

    this.ensureFlushTimer();
    if (this.queue.length >= this.maxBatchSize) {
      this.triggerDrain();
    }

    return deferred.promise;
  }

  flush(): Promise<void> {
    return this.requestDrain();
  }

  async close(): Promise<void> {
    this.closed = true;
    this.clearFlushTimer();
    await this.requestDrain();
  }

  private requestDrain(): Promise<void> {
    if (!this.drainPromise) {
      this.drainPromise = this.drainLoop();
    }
    return this.drainPromise;
  }

  private async drainLoop(): Promise<void> {
    try {
      this.clearFlushTimer();

      while (this.queue.length > 0) {
        const batch = this.queue.splice(0, this.maxBatchSize);
        const operations = batch.map(item => item.operation);

        try {
          await this.batchAppender.appendBatch(operations);
          this.metrics?.increment("statefabric.batch_flush_total");
          this.metrics?.observe("statefabric.batch_size", operations.length);
          for (const item of batch) {
            item.deferred.resolve();
          }
        } catch (error) {
          this.metrics?.increment("statefabric.batch_flush_error_total");
          for (const item of batch) {
            item.deferred.reject(error);
          }

          // Fail queued operations as well: no durable ack can be given while sink fails.
          const pending = this.queue.splice(0, this.queue.length);
          for (const item of pending) {
            item.deferred.reject(error);
          }

          throw error;
        }
      }
    } finally {
      this.drainPromise = null;

      // If operations arrived between the final queue check and drainPromise reset,
      // trigger another drain pass to keep append behavior deterministic.
      if (this.queue.length > 0 && !this.closed) {
        this.triggerDrain();
      }
    }
  }

  private ensureFlushTimer(): void {
    if (this.flushTimer || this.queue.length === 0) {
      return;
    }

    this.flushTimer = this.setTimeoutFn(() => {
      this.flushTimer = null;
      this.triggerDrain();
    }, this.flushIntervalMs);
  }

  private clearFlushTimer(): void {
    if (!this.flushTimer) {
      return;
    }
    this.clearTimeoutFn(this.flushTimer);
    this.flushTimer = null;
  }

  private triggerDrain(): void {
    // Fire-and-forget usage path. Rejections are propagated to per-append
    // promises, but background drain rejections must still be marked handled.
    this.requestDrain().catch(() => {});
  }
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"];
  let reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });

  return { resolve, reject, promise };
}

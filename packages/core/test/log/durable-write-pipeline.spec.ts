import { describe, expect, it } from "vitest";
import {
  DurableWritePipeline,
  type DurableAppender,
  type OperationEnvelope,
  type StateApplier
} from "../../src/log";

class RecordingAppender implements DurableAppender {
  constructor(
    private readonly events: string[],
    private readonly shouldFail = false
  ) {}

  async append(operation: OperationEnvelope): Promise<void> {
    this.events.push(`append:${operation.opId}`);
    if (this.shouldFail) {
      throw new Error("append failed");
    }
  }
}

class RecordingApplier implements StateApplier {
  constructor(private readonly events: string[]) {}

  apply(operation: OperationEnvelope): void {
    this.events.push(`apply:${operation.opId}`);
  }
}

describe("DurableWritePipeline", () => {
  it("appends before applying state", async () => {
    const events: string[] = [];
    const pipeline = new DurableWritePipeline(
      new RecordingAppender(events),
      new RecordingApplier(events)
    );

    const op: OperationEnvelope = {
      opId: "op-1",
      namespace: "session",
      key: "u1",
      type: "set",
      payload: { value: 1 }
    };

    const result = await pipeline.write(op);

    expect(result).toEqual({ ack: true, opId: "op-1" });
    expect(events).toEqual(["append:op-1", "apply:op-1"]);
  });

  it("does not apply when append fails", async () => {
    const events: string[] = [];
    const pipeline = new DurableWritePipeline(
      new RecordingAppender(events, true),
      new RecordingApplier(events)
    );

    const op: OperationEnvelope = {
      opId: "op-2",
      namespace: "session",
      key: "u2",
      type: "set",
      payload: { value: 2 }
    };

    await expect(pipeline.write(op)).rejects.toThrow("append failed");
    expect(events).toEqual(["append:op-2"]);
  });
});

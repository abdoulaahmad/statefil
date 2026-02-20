import { describe, expect, it } from "vitest";
import { replayOperations, type CanonicalOrderedOperation } from "../../src/log";

describe("replayOperations", () => {
  it("replays in canonical order", () => {
    const ops: CanonicalOrderedOperation[] = [
      {
        opId: "op-3",
        actorNodeId: "node-b",
        sequence: 1,
        hlc: { physical: 101, logical: 0, nodeId: "node-b" }
      },
      {
        opId: "op-2",
        actorNodeId: "node-b",
        sequence: 1,
        hlc: { physical: 100, logical: 1, nodeId: "x" }
      },
      {
        opId: "op-1",
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 1, nodeId: "x" }
      }
    ];

    const applied: string[] = [];
    const result = replayOperations(ops, op => {
      applied.push(op.opId);
    });

    expect(applied).toEqual(["op-1", "op-2", "op-3"]);
    expect(result.appliedCount).toBe(3);
    expect(result.skippedDuplicateCount).toBe(0);
  });

  it("skips duplicate op ids for idempotent replay", () => {
    const ops: CanonicalOrderedOperation[] = [
      {
        opId: "dup",
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "a" }
      },
      {
        opId: "dup",
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "a" }
      },
      {
        opId: "unique",
        actorNodeId: "node-b",
        sequence: 1,
        hlc: { physical: 102, logical: 0, nodeId: "b" }
      }
    ];

    const applied: string[] = [];
    const result = replayOperations(ops, op => {
      applied.push(op.opId);
    });

    expect(applied).toEqual(["dup", "unique"]);
    expect(result.appliedCount).toBe(2);
    expect(result.skippedDuplicateCount).toBe(1);
  });

  it("emits duplicate callback when duplicate op id is skipped", () => {
    const ops: CanonicalOrderedOperation[] = [
      {
        opId: "dup",
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 0, nodeId: "a" }
      },
      {
        opId: "dup",
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 101, logical: 0, nodeId: "a" }
      }
    ];

    const duplicates: string[] = [];
    replayOperations(
      ops,
      () => {},
      {
        onDuplicate: opId => duplicates.push(opId)
      }
    );

    expect(duplicates).toEqual(["dup"]);
  });
});

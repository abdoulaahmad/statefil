import { describe, expect, it } from "vitest";
import {
  compareCanonicalOperation,
  sortCanonicalOperations,
  type CanonicalOrderedOperation
} from "../../src/log";

describe("canonical operation ordering", () => {
  it("orders by hlc first", () => {
    const a: CanonicalOrderedOperation = {
      opId: "a",
      actorNodeId: "node-a",
      sequence: 1,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    };
    const b: CanonicalOrderedOperation = {
      opId: "b",
      actorNodeId: "node-b",
      sequence: 1,
      hlc: { physical: 101, logical: 0, nodeId: "node-b" }
    };

    expect(compareCanonicalOperation(a, b)).toBeLessThan(0);
  });

  it("uses actor node id when hlc values are equal", () => {
    const a: CanonicalOrderedOperation = {
      opId: "a",
      actorNodeId: "node-a",
      sequence: 2,
      hlc: { physical: 100, logical: 1, nodeId: "same" }
    };
    const b: CanonicalOrderedOperation = {
      opId: "b",
      actorNodeId: "node-b",
      sequence: 1,
      hlc: { physical: 100, logical: 1, nodeId: "same" }
    };

    expect(compareCanonicalOperation(a, b)).toBeLessThan(0);
  });

  it("uses sequence as final tie-breaker", () => {
    const base = {
      actorNodeId: "node-a",
      hlc: { physical: 100, logical: 1, nodeId: "same" }
    };

    const a: CanonicalOrderedOperation = { ...base, opId: "a", sequence: 1 };
    const b: CanonicalOrderedOperation = { ...base, opId: "b", sequence: 2 };

    expect(compareCanonicalOperation(a, b)).toBeLessThan(0);
  });

  it("sorts deterministic output for mixed operations", () => {
    const ops: CanonicalOrderedOperation[] = [
      {
        opId: "3",
        actorNodeId: "node-b",
        sequence: 1,
        hlc: { physical: 101, logical: 0, nodeId: "node-b" }
      },
      {
        opId: "2",
        actorNodeId: "node-b",
        sequence: 1,
        hlc: { physical: 100, logical: 1, nodeId: "x" }
      },
      {
        opId: "1",
        actorNodeId: "node-a",
        sequence: 1,
        hlc: { physical: 100, logical: 1, nodeId: "x" }
      }
    ];

    const sorted = sortCanonicalOperations(ops);

    expect(sorted.map(op => op.opId)).toEqual(["1", "2", "3"]);
  });
});

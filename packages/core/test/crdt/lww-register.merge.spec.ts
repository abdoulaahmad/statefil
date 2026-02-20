import { describe, expect, it } from "vitest";
import { LWWRegister, type LWWTimestamp } from "../../src/crdt";

describe("LWWRegister merge semantics", () => {
  it("keeps value with newer timestamp", () => {
    const left = new LWWRegister<string>(undefined, "node-a");
    const right = new LWWRegister<string>(undefined, "node-b");

    const t1: LWWTimestamp = { physical: 1000, logical: 1, nodeId: "node-a" };
    const t2: LWWTimestamp = { physical: 1001, logical: 0, nodeId: "node-b" };

    left.setWithClock("old", t1, "node-a");
    right.setWithClock("new", t2, "node-b");

    const merged = left.merge(right);

    expect(merged.get()).toBe("new");
  });

  it("breaks equal timestamp ties by actor id", () => {
    const conflicts: string[] = [];
    const left = new LWWRegister<string>(undefined, "node-a", event => {
      conflicts.push(`${event.loserActorId}->${event.winnerActorId}`);
    });
    const right = new LWWRegister<string>(undefined, "node-b");

    const same: LWWTimestamp = { physical: 2000, logical: 1, nodeId: "node-z" };

    left.setWithClock("left", same, "actor-a");
    right.setWithClock("right", same, "actor-b");

    const merged = left.merge(right);

    expect(merged.get()).toBe("right");
    expect(conflicts).toEqual(["actor-a->actor-b"]);
  });
});

import { describe, expect, it } from "vitest";
import { ORSet } from "../../src/crdt";

describe("ORSet merge tombstone safety", () => {
  it("does not resurrect deleted element when left tombstone must win", () => {
    const left = new ORSet<string>();
    const right = new ORSet<string>();

    left.add("session-1", "tag-1");
    left.remove("session-1");

    right.add("session-1", "tag-1");

    const merged = left.merge(right);

    expect(merged.has("session-1")).toBe(false);
  });
});

import { describe, expect, it } from "vitest";
import { ORSet } from "../../src/crdt";

describe("ORSet tombstone compaction", () => {
  it("compacts only tags at or below actor watermark", () => {
    const set = new ORSet<string>();
    set.add("session", "node-a:1");
    set.remove("session");
    set.add("session", "node-a:2");
    set.remove("session");

    const removed = set.compactTombstones({ "node-a": 1 });

    expect(removed).toBe(1);
    expect(set.tombstoneCount()).toBe(1);
  });

  it("retains legacy/unparseable tombstones for safety", () => {
    const set = new ORSet<string>();
    set.add("session", "legacy-tag-1");
    set.remove("session");

    const removed = set.compactTombstones({ "node-a": 10_000 });

    expect(removed).toBe(0);
    expect(set.tombstoneCount()).toBe(1);
  });

  it("does not resurrect tags that remain above watermark after merge", () => {
    const left = new ORSet<string>();
    const right = new ORSet<string>();

    left.add("session", "node-a:1");
    left.remove("session");
    left.add("session", "node-a:2");
    left.remove("session");

    // Keep tombstone for node-a:2, compact only older node-a:1.
    left.compactTombstones({ "node-a": 1 });

    // Stale replica that still carries node-a:2 add.
    right.add("session", "node-a:2");

    const merged = left.merge(right);

    expect(merged.has("session")).toBe(false);
  });
});

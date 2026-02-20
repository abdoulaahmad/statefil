import { describe, expect, it } from "vitest";
import {
  activateSegment,
  compactManifest,
  commitSegment,
  createManifest,
  isSegmentCommitted
} from "../../src/log/manifest";

describe("segment manifest", () => {
  it("creates manifest with expected defaults", () => {
    const manifest = createManifest({
      activeSegmentId: "seg-1",
      now: () => new Date("2026-02-19T00:00:00.000Z")
    });

    expect(manifest.version).toBe("1.0");
    expect(manifest.activeSegmentId).toBe("seg-1");
    expect(manifest.committedSegments).toEqual([]);
    expect(manifest.lastCommittedSequenceByNode).toEqual({});
    expect(manifest.updatedAt).toBe("2026-02-19T00:00:00.000Z");
  });

  it("commits segment once and tracks max sequence by node", () => {
    const base = createManifest({ activeSegmentId: "seg-active" });

    const withFirstCommit = commitSegment(base, {
      segmentId: "seg-1",
      nodeId: "node-a",
      sequence: 2
    });

    const withLowerSequence = commitSegment(withFirstCommit, {
      segmentId: "seg-1",
      nodeId: "node-a",
      sequence: 1
    });

    const withHigherSequence = commitSegment(withLowerSequence, {
      segmentId: "seg-2",
      nodeId: "node-a",
      sequence: 5
    });

    expect(withHigherSequence.committedSegments).toEqual(["seg-1", "seg-2"]);
    expect(withHigherSequence.lastCommittedSequenceByNode["node-a"]).toBe(5);
    expect(isSegmentCommitted(withHigherSequence, "seg-1")).toBe(true);
  });

  it("activates new segment id", () => {
    const manifest = createManifest({ activeSegmentId: "seg-1" });

    const next = activateSegment(manifest, "seg-2", () => new Date("2026-02-20T00:00:00.000Z"));

    expect(next.activeSegmentId).toBe("seg-2");
    expect(next.updatedAt).toBe("2026-02-20T00:00:00.000Z");
  });

  it("compacts committed segments around checkpoint with retention", () => {
    const base = createManifest({ activeSegmentId: "seg-4" });
    const withCommits = {
      ...base,
      committedSegments: ["seg-1", "seg-2", "seg-3", "seg-4"]
    };

    const compacted = compactManifest(withCommits, {
      checkpointSegmentId: "seg-3",
      retainPreCheckpointSegments: 1,
      now: () => new Date("2026-02-20T01:00:00.000Z")
    });

    expect(compacted.committedSegments).toEqual(["seg-2", "seg-3", "seg-4"]);
    expect(compacted.updatedAt).toBe("2026-02-20T01:00:00.000Z");
  });
});

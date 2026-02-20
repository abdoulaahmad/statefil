import { describe, expect, it } from "vitest";
import type { SegmentManifest } from "../../src/log/manifest";
import {
  RecoveryManager,
  type RecoverySnapshot,
  type SegmentReader,
  type ManifestLoader,
  type SnapshotLoader
} from "../../src/log/recovery";
import type { CanonicalOrderedOperation } from "../../src/log/ordering";

interface TestOperation extends CanonicalOrderedOperation {
  value: string;
}

class StaticManifestLoader implements ManifestLoader {
  constructor(private readonly manifest: SegmentManifest) {}

  async loadManifest(): Promise<SegmentManifest> {
    return this.manifest;
  }
}

class StaticSnapshotLoader implements SnapshotLoader {
  constructor(private readonly snapshot: RecoverySnapshot | null) {}

  async loadLatest(): Promise<RecoverySnapshot | null> {
    return this.snapshot;
  }
}

class StaticSegmentReader implements SegmentReader<TestOperation> {
  constructor(private readonly segments: Record<string, TestOperation[]>) {}

  async readSegment(segmentId: string): Promise<TestOperation[]> {
    return this.segments[segmentId] ?? [];
  }
}

function op(
  opId: string,
  physical: number,
  logical: number,
  actorNodeId: string,
  sequence: number,
  value = opId
): TestOperation {
  return {
    opId,
    actorNodeId,
    sequence,
    value,
    hlc: { physical, logical, nodeId: actorNodeId }
  };
}

describe("RecoveryManager", () => {
  it("replays only committed segments after snapshot checkpoint", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-4",
      committedSegments: ["seg-1", "seg-2", "seg-3"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const snapshot: RecoverySnapshot = {
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-1" }
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-1": [op("old", 100, 0, "node-a", 1)],
      "seg-2": [op("b", 101, 0, "node-b", 1)],
      "seg-3": [op("a", 101, 0, "node-a", 1)]
    };

    const applied: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      snapshotLoader: new StaticSnapshotLoader(snapshot),
      segmentReader: new StaticSegmentReader(segmentData),
      apply: operation => {
        applied.push(operation.opId);
      }
    });

    const result = await manager.recover();

    expect(result.snapshotId).toBe("snap-1");
    expect(result.replayedSegments).toEqual(["seg-2", "seg-3"]);
    expect(applied).toEqual(["a", "b"]);
  });

  it("replays all committed segments when no snapshot exists", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-1": [op("x", 100, 0, "node-a", 1)],
      "seg-2": [op("y", 101, 0, "node-a", 2)]
    };

    const applied: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      snapshotLoader: new StaticSnapshotLoader(null),
      segmentReader: new StaticSegmentReader(segmentData),
      apply: operation => {
        applied.push(operation.opId);
      }
    });

    const result = await manager.recover();

    expect(result.replayedSegments).toEqual(["seg-1", "seg-2"]);
    expect(applied).toEqual(["x", "y"]);
  });

  it("skips duplicate op ids during replay", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-1": [op("dup", 100, 0, "node-a", 1)],
      "seg-2": [op("dup", 101, 0, "node-a", 2), op("uniq", 102, 0, "node-b", 1)]
    };

    const applied: string[] = [];
    const duplicates: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      segmentReader: new StaticSegmentReader(segmentData),
      apply: operation => {
        applied.push(operation.opId);
      },
      onDuplicateOperation: opId => {
        duplicates.push(opId);
      }
    });

    const result = await manager.recover();

    expect(applied).toEqual(["dup", "uniq"]);
    expect(result.skippedDuplicateCount).toBe(1);
    expect(duplicates).toEqual(["dup"]);
  });

  it("falls back to full replay if snapshot checkpoint segment is missing", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-2",
      committedSegments: ["seg-a", "seg-b"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const snapshot: RecoverySnapshot = {
      snapshotId: "snap-missing",
      checkpoint: { lastSegmentId: "not-found" }
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-a": [op("a", 100, 0, "node-a", 1)],
      "seg-b": [op("b", 101, 0, "node-b", 1)]
    };

    const applied: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      snapshotLoader: new StaticSnapshotLoader(snapshot),
      segmentReader: new StaticSegmentReader(segmentData),
      apply: operation => {
        applied.push(operation.opId);
      }
    });

    const result = await manager.recover();

    expect(result.replayedSegments).toEqual(["seg-a", "seg-b"]);
    expect(applied).toEqual(["a", "b"]);
  });

  it("limits replay to most recent committed segments when maxReplaySegments is set", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-4",
      committedSegments: ["seg-1", "seg-2", "seg-3"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-1": [op("a", 100, 0, "node-a", 1)],
      "seg-2": [op("b", 101, 0, "node-a", 2)],
      "seg-3": [op("c", 102, 0, "node-a", 3)]
    };

    const applied: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      segmentReader: new StaticSegmentReader(segmentData),
      bounds: {
        maxReplaySegments: 2
      },
      apply: operation => {
        applied.push(operation.opId);
      }
    });

    const result = await manager.recover();

    expect(result.replayedSegments).toEqual(["seg-2", "seg-3"]);
    expect(result.truncatedBySegmentLimit).toBe(true);
    expect(result.candidateSegmentCount).toBe(3);
    expect(applied).toEqual(["b", "c"]);
  });

  it("limits replay to most recent canonical operations when maxReplayOperations is set", async () => {
    const manifest: SegmentManifest = {
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    };

    const segmentData: Record<string, TestOperation[]> = {
      "seg-1": [op("a", 100, 0, "node-a", 1), op("b", 101, 0, "node-a", 2)],
      "seg-2": [op("c", 102, 0, "node-a", 3)]
    };

    const applied: string[] = [];

    const manager = new RecoveryManager<TestOperation>({
      manifestLoader: new StaticManifestLoader(manifest),
      segmentReader: new StaticSegmentReader(segmentData),
      bounds: {
        maxReplayOperations: 2
      },
      apply: operation => {
        applied.push(operation.opId);
      }
    });

    const result = await manager.recover();

    expect(result.candidateOperationCount).toBe(3);
    expect(result.truncatedByOperationLimit).toBe(true);
    expect(applied).toEqual(["b", "c"]);
  });
});

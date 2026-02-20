import { describe, expect, it } from "vitest";
import type {
  RecoverySnapshot,
  RuntimeOperation,
  SegmentManifest,
  StoredSegmentGcReport
} from "../src";

interface AdapterHandle<TAdapter> {
  adapter: TAdapter;
  cleanup?: () => Promise<void> | void;
}

export interface PersistenceAdapterContract<TAdapter> {
  name: string;
  createAdapter(): Promise<AdapterHandle<TAdapter>>;
  appendSegment(adapter: TAdapter, segmentId: string, content: string): Promise<void>;
  readSegment(adapter: TAdapter, segmentId: string): Promise<string | null>;
  listSegments(adapter: TAdapter): Promise<string[]>;
  deleteSegment(adapter: TAdapter, segmentId: string): Promise<void>;
  saveManifest(adapter: TAdapter, manifest: SegmentManifest): Promise<void>;
  loadManifest(adapter: TAdapter): Promise<SegmentManifest | null>;
  saveLatestSnapshot(adapter: TAdapter, snapshot: RecoverySnapshot): Promise<void>;
  loadLatestSnapshot(adapter: TAdapter): Promise<RecoverySnapshot | null>;
  appendSegmentOperations(
    adapter: TAdapter,
    segmentId: string,
    operations: RuntimeOperation[]
  ): Promise<void>;
  readSegmentOperations(adapter: TAdapter, segmentId: string): Promise<RuntimeOperation[]>;
  saveSegmentGcReport(adapter: TAdapter, report: StoredSegmentGcReport): Promise<void>;
  loadSegmentGcReport(
    adapter: TAdapter,
    executionId: string
  ): Promise<StoredSegmentGcReport | null>;
}

export function definePersistenceAdapterContract<TAdapter>(
  contract: PersistenceAdapterContract<TAdapter>
): void {
  describe(`${contract.name} shared contract`, () => {
    it("round-trips raw segment content", async () => {
      const handle = await contract.createAdapter();
      try {
        await contract.appendSegment(handle.adapter, "seg-1", "line-1\n");
        await contract.appendSegment(handle.adapter, "seg-1", "line-2\n");
        await expect(contract.readSegment(handle.adapter, "seg-1")).resolves.toBe(
          "line-1\nline-2\n"
        );
      } finally {
        await handle.cleanup?.();
      }
    });

    it("round-trips manifests and snapshots", async () => {
      const handle = await contract.createAdapter();
      try {
        const manifest: SegmentManifest = {
          version: "1.0",
          activeSegmentId: "seg-2",
          committedSegments: ["seg-1", "seg-2"],
          lastCommittedSequenceByNode: { "node-a": 2 },
          updatedAt: "2026-02-20T00:00:00.000Z"
        };
        const snapshot: RecoverySnapshot = {
          snapshotId: "snap-1",
          checkpoint: { lastSegmentId: "seg-2" }
        };

        await contract.saveManifest(handle.adapter, manifest);
        await contract.saveLatestSnapshot(handle.adapter, snapshot);

        await expect(contract.loadManifest(handle.adapter)).resolves.toEqual(manifest);
        await expect(contract.loadLatestSnapshot(handle.adapter)).resolves.toEqual(snapshot);
      } finally {
        await handle.cleanup?.();
      }
    });

    it("round-trips operation JSONL", async () => {
      const handle = await contract.createAdapter();
      try {
        const operations: RuntimeOperation[] = [
          {
            opId: "op-1",
            namespace: "session",
            key: "u1",
            type: "register.set",
            payload: { value: "alice" },
            actorNodeId: "node-a",
            sequence: 1,
            hlc: { physical: 100, logical: 0, nodeId: "node-a" }
          },
          {
            opId: "op-2",
            namespace: "rate",
            key: "u1",
            type: "counter.increment",
            payload: { amount: 2 },
            actorNodeId: "node-a",
            sequence: 2,
            hlc: { physical: 101, logical: 0, nodeId: "node-a" }
          }
        ];

        await contract.appendSegmentOperations(handle.adapter, "seg-1", operations);
        await expect(contract.readSegmentOperations(handle.adapter, "seg-1")).resolves.toEqual(
          operations
        );
      } finally {
        await handle.cleanup?.();
      }
    });

    it("lists and deletes segments consistently", async () => {
      const handle = await contract.createAdapter();
      try {
        await contract.appendSegment(handle.adapter, "seg-2", "line-a\n");
        await contract.appendSegment(handle.adapter, "seg-1", "line-b\n");
        await expect(contract.listSegments(handle.adapter)).resolves.toEqual(["seg-1", "seg-2"]);

        await contract.deleteSegment(handle.adapter, "seg-1");
        await contract.deleteSegment(handle.adapter, "seg-missing");
        await expect(contract.listSegments(handle.adapter)).resolves.toEqual(["seg-2"]);
      } finally {
        await handle.cleanup?.();
      }
    });

    it("persists and reloads execution-token GC reports", async () => {
      const handle = await contract.createAdapter();
      try {
        const report: StoredSegmentGcReport = {
          executionId: "gc-1",
          status: "in_progress",
          dryRun: false,
          supported: true,
          replayedResult: false,
          referencedSegments: ["seg-2"],
          orphanSegments: ["seg-1"],
          deletedSegments: [],
          failedSegments: [],
          remainingSegments: ["seg-1"]
        };

        await contract.saveSegmentGcReport(handle.adapter, report);
        await expect(contract.loadSegmentGcReport(handle.adapter, "gc-1")).resolves.toEqual(
          report
        );
        await expect(contract.loadSegmentGcReport(handle.adapter, "missing")).resolves.toBeNull();
      } finally {
        await handle.cleanup?.();
      }
    });
  });
}

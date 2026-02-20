import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { definePersistenceAdapterContract } from "../../core/test-support/persistence-adapter-contract";
import { LocalPersistenceAdapter } from "../src";

definePersistenceAdapterContract({
  name: "LocalPersistenceAdapter",
  async createAdapter() {
    const root = await mkdtemp(path.join(tmpdir(), "statefabric-local-contract-"));
    return {
      adapter: new LocalPersistenceAdapter({ rootDir: root }),
      cleanup: async () => {
        await rm(root, { recursive: true, force: true });
      }
    };
  },
  appendSegment: (adapter, segmentId, content) => adapter.appendSegment(segmentId, content),
  readSegment: (adapter, segmentId) => adapter.readSegment(segmentId),
  listSegments: adapter => adapter.listSegments(),
  deleteSegment: (adapter, segmentId) => adapter.deleteSegment(segmentId),
  saveManifest: (adapter, manifest) => adapter.saveManifest(manifest),
  loadManifest: adapter => adapter.loadManifest(),
  saveLatestSnapshot: (adapter, snapshot) => adapter.saveLatestSnapshot(snapshot),
  loadLatestSnapshot: adapter => adapter.loadLatestSnapshot(),
  appendSegmentOperations: (adapter, segmentId, operations) =>
    adapter.appendSegmentOperations(segmentId, operations),
  readSegmentOperations: (adapter, segmentId) => adapter.readSegmentOperations(segmentId),
  saveSegmentGcReport: (adapter, report) => adapter.saveSegmentGcReport(report),
  loadSegmentGcReport: (adapter, executionId) => adapter.loadSegmentGcReport(executionId)
});

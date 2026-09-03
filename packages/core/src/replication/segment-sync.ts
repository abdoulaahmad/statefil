import type { MetricsCollector } from "../observability";
import type { DurableRuntimeStore, RuntimeOperation } from "../runtime/durable-eventual-runtime";
import type { SegmentManifest } from "../log";

export interface SegmentSyncReport {
  remoteSegmentCount: number;
  localSegmentCountBefore: number;
  copiedSegments: string[];
  skippedSegments: string[];
  copiedOperationCount: number;
  mergedCommittedSegments: string[];
}

export interface SegmentSyncManagerOptions {
  metrics?: MetricsCollector;
}

/**
 * Pull-based object-log synchronization: copies operations the local store is
 * missing from a remote store and merges the committed-segment manifests.
 * Convergence is completed by replaying the merged log (runtime.recover()),
 * which deduplicates by opId and orders canonically, so re-copying shared
 * operations is safe but avoided to minimize transfer size.
 */
export class SegmentSyncManager {
  private readonly local: DurableRuntimeStore;
  private readonly remote: DurableRuntimeStore;
  private readonly metrics?: MetricsCollector;

  constructor(local: DurableRuntimeStore, remote: DurableRuntimeStore, options: SegmentSyncManagerOptions = {}) {
    if (!local.listSegments || !remote.listSegments) {
      throw new Error("SegmentSyncManager requires stores with listSegments support");
    }
    this.local = local;
    this.remote = remote;
    this.metrics = options.metrics;
  }

  async sync(): Promise<SegmentSyncReport> {
    const localSegmentsBefore = await this.local.listSegments!();
    const remoteSegments = await this.remote.listSegments!();
    const localSegmentSet = new Set(localSegmentsBefore);

    const remoteManifest = await this.remote.loadManifest();
    const remoteCommitted = new Set(remoteManifest?.committedSegments ?? []);

    const copiedSegments: string[] = [];
    const skippedSegments: string[] = [];
    let copiedOperationCount = 0;

    for (const segmentId of remoteSegments) {
      // Uncommitted remote segments are still being written; skip them until committed.
      if (remoteManifest && !remoteCommitted.has(segmentId)) {
        skippedSegments.push(segmentId);
        continue;
      }

      const remoteOps = await this.remote.readSegment(segmentId);
      if (remoteOps.length === 0) {
        skippedSegments.push(segmentId);
        continue;
      }

      let newOps: RuntimeOperation[];
      if (!localSegmentSet.has(segmentId)) {
        newOps = remoteOps;
      } else {
        const localOps = await this.local.readSegment(segmentId);
        const localOpIds = new Set(localOps.map(op => op.opId));
        newOps = remoteOps.filter(op => !localOpIds.has(op.opId));
      }

      if (newOps.length === 0) {
        skippedSegments.push(segmentId);
        continue;
      }

      await this.local.appendToSegment(segmentId, newOps);
      copiedSegments.push(segmentId);
      copiedOperationCount += newOps.length;
      localSegmentSet.add(segmentId);
    }

    const mergedManifest = await this.mergeManifests();
    if (mergedManifest) {
      await this.local.saveManifest(mergedManifest);
    }

    if (copiedSegments.length > 0) {
      this.metrics?.increment("statefabric.sync_copied_segments_total", copiedSegments.length);
      this.metrics?.observe("statefabric.sync_copied_operations", copiedOperationCount);
    }

    return {
      remoteSegmentCount: remoteSegments.length,
      localSegmentCountBefore: localSegmentsBefore.length,
      copiedSegments,
      skippedSegments,
      copiedOperationCount,
      mergedCommittedSegments: mergedManifest?.committedSegments ?? []
    };
  }

  private async mergeManifests(): Promise<SegmentManifest | null> {
    const [localManifest, remoteManifest] = await Promise.all([
      this.local.loadManifest(),
      this.remote.loadManifest()
    ]);

    if (!localManifest && !remoteManifest) {
      return null;
    }
    if (!localManifest) {
      return { ...remoteManifest! };
    }
    if (!remoteManifest) {
      return localManifest;
    }

    const committedSegments = [...localManifest.committedSegments];
    for (const segmentId of remoteManifest.committedSegments) {
      if (!committedSegments.includes(segmentId)) {
        committedSegments.push(segmentId);
      }
    }

    const lastCommittedSequenceByNode = { ...localManifest.lastCommittedSequenceByNode };
    for (const [nodeId, sequence] of Object.entries(remoteManifest.lastCommittedSequenceByNode)) {
      lastCommittedSequenceByNode[nodeId] = Math.max(
        lastCommittedSequenceByNode[nodeId] ?? 0,
        sequence
      );
    }

    return {
      ...localManifest,
      committedSegments,
      lastCommittedSequenceByNode,
      updatedAt: new Date().toISOString()
    };
  }
}

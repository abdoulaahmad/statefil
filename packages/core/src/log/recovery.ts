import type { SegmentManifest } from "./manifest";
import type { CanonicalOrderedOperation } from "./ordering";
import { sortCanonicalOperations } from "./ordering";
import { replayOperations, type ReplayResult } from "./replay";

export interface RecoverySnapshot {
  snapshotId: string;
  checkpoint: {
    lastSegmentId?: string;
  };
  state?: {
    registers?: Record<string, unknown>;
    counters?: Record<string, number>;
  };
}

export interface SnapshotLoader {
  loadLatest(): Promise<RecoverySnapshot | null>;
}

export interface ManifestLoader {
  loadManifest(): Promise<SegmentManifest>;
}

export interface SegmentReader<TOperation extends CanonicalOrderedOperation> {
  readSegment(segmentId: string): Promise<TOperation[]>;
}

export interface RecoveryResult extends ReplayResult {
  snapshotId?: string;
  replayedSegments: string[];
  candidateSegmentCount: number;
  candidateOperationCount: number;
  truncatedBySegmentLimit: boolean;
  truncatedByOperationLimit: boolean;
}

export interface RecoveryBounds {
  maxReplaySegments?: number;
  maxReplayOperations?: number;
}

export interface RecoveryManagerDeps<TOperation extends CanonicalOrderedOperation> {
  manifestLoader: ManifestLoader;
  segmentReader: SegmentReader<TOperation>;
  apply: (operation: TOperation) => void;
  bounds?: RecoveryBounds;
  snapshotLoader?: SnapshotLoader;
  onDuplicateOperation?: (opId: string) => void;
}

export class RecoveryManager<TOperation extends CanonicalOrderedOperation> {
  private readonly manifestLoader: ManifestLoader;
  private readonly segmentReader: SegmentReader<TOperation>;
  private readonly apply: (operation: TOperation) => void;
  private readonly bounds?: RecoveryBounds;
  private readonly snapshotLoader?: SnapshotLoader;
  private readonly onDuplicateOperation?: (opId: string) => void;

  constructor(deps: RecoveryManagerDeps<TOperation>) {
    this.manifestLoader = deps.manifestLoader;
    this.segmentReader = deps.segmentReader;
    this.apply = deps.apply;
    this.bounds = deps.bounds;
    this.snapshotLoader = deps.snapshotLoader;
    this.onDuplicateOperation = deps.onDuplicateOperation;
  }

  async recover(): Promise<RecoveryResult> {
    const [manifest, snapshot] = await Promise.all([
      this.manifestLoader.loadManifest(),
      this.snapshotLoader?.loadLatest() ?? Promise.resolve(null)
    ]);

    const candidateSegments = this.selectSegmentsToReplay(manifest, snapshot);
    const replayedSegments = this.applySegmentBound(candidateSegments);

    const operations: TOperation[] = [];
    for (const segmentId of replayedSegments) {
      const fromSegment = await this.segmentReader.readSegment(segmentId);
      operations.push(...fromSegment);
    }

    const candidateOperationCount = operations.length;
    const boundedOperations = this.applyOperationBound(operations);

    const replay = replayOperations(boundedOperations, this.apply, {
      onDuplicate: this.onDuplicateOperation
    });

    return {
      ...replay,
      snapshotId: snapshot?.snapshotId,
      replayedSegments,
      candidateSegmentCount: candidateSegments.length,
      candidateOperationCount,
      truncatedBySegmentLimit: replayedSegments.length < candidateSegments.length,
      truncatedByOperationLimit: boundedOperations.length < candidateOperationCount
    };
  }

  private selectSegmentsToReplay(
    manifest: SegmentManifest,
    snapshot: RecoverySnapshot | null
  ): string[] {
    const committed = manifest.committedSegments;
    const checkpoint = snapshot?.checkpoint.lastSegmentId;

    if (!checkpoint) {
      return [...committed];
    }

    const idx = committed.indexOf(checkpoint);
    if (idx < 0) {
      return [...committed];
    }

    return committed.slice(idx + 1);
  }

  private applySegmentBound(segments: string[]): string[] {
    const rawLimit = this.bounds?.maxReplaySegments;
    if (typeof rawLimit !== "number") {
      return [...segments];
    }

    const limit = Math.max(0, Math.floor(rawLimit));
    if (limit === 0) {
      return [];
    }

    return segments.slice(-limit);
  }

  private applyOperationBound(operations: TOperation[]): TOperation[] {
    const rawLimit = this.bounds?.maxReplayOperations;
    if (typeof rawLimit !== "number") {
      return operations;
    }

    const limit = Math.max(0, Math.floor(rawLimit));
    if (limit === 0) {
      return [];
    }

    return sortCanonicalOperations(operations).slice(-limit);
  }
}

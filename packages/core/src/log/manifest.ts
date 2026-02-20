export interface SegmentManifest {
  version: "1.0";
  activeSegmentId: string;
  committedSegments: string[];
  lastCommittedSequenceByNode: Record<string, number>;
  updatedAt: string;
}

export interface CreateManifestInput {
  activeSegmentId: string;
  now?: () => Date;
}

export interface CommitSegmentInput {
  segmentId: string;
  nodeId?: string;
  sequence?: number;
  now?: () => Date;
}

export interface CompactManifestInput {
  checkpointSegmentId: string;
  retainPreCheckpointSegments?: number;
  now?: () => Date;
}

export function createManifest(input: CreateManifestInput): SegmentManifest {
  const now = input.now ?? (() => new Date());

  return {
    version: "1.0",
    activeSegmentId: input.activeSegmentId,
    committedSegments: [],
    lastCommittedSequenceByNode: {},
    updatedAt: now().toISOString()
  };
}

export function activateSegment(
  manifest: SegmentManifest,
  activeSegmentId: string,
  now: () => Date = () => new Date()
): SegmentManifest {
  return {
    ...manifest,
    activeSegmentId,
    updatedAt: now().toISOString()
  };
}

export function commitSegment(
  manifest: SegmentManifest,
  input: CommitSegmentInput
): SegmentManifest {
  const now = input.now ?? (() => new Date());
  const committedSegments = manifest.committedSegments.includes(input.segmentId)
    ? [...manifest.committedSegments]
    : [...manifest.committedSegments, input.segmentId];

  const sequenceState = { ...manifest.lastCommittedSequenceByNode };

  if (input.nodeId && typeof input.sequence === "number") {
    const current = sequenceState[input.nodeId] ?? 0;
    sequenceState[input.nodeId] = Math.max(current, input.sequence);
  }

  return {
    ...manifest,
    committedSegments,
    lastCommittedSequenceByNode: sequenceState,
    updatedAt: now().toISOString()
  };
}

export function isSegmentCommitted(manifest: SegmentManifest, segmentId: string): boolean {
  return manifest.committedSegments.includes(segmentId);
}

export function compactManifest(
  manifest: SegmentManifest,
  input: CompactManifestInput
): SegmentManifest {
  const now = input.now ?? (() => new Date());
  const retainPreCheckpointSegments = Math.max(0, input.retainPreCheckpointSegments ?? 0);
  const checkpointIndex = manifest.committedSegments.indexOf(input.checkpointSegmentId);

  if (checkpointIndex < 0) {
    return {
      ...manifest,
      updatedAt: now().toISOString()
    };
  }

  const start = Math.max(0, checkpointIndex - retainPreCheckpointSegments);
  const retained = manifest.committedSegments.slice(start);

  return {
    ...manifest,
    committedSegments: retained,
    updatedAt: now().toISOString()
  };
}

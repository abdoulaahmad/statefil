import { sortCanonicalOperations, type CanonicalOrderedOperation } from "./ordering";

export interface ReplayResult {
  appliedCount: number;
  skippedDuplicateCount: number;
  appliedOpIds: string[];
}

export interface ReplayOptions {
  onDuplicate?: (opId: string) => void;
}

export function replayOperations<T extends CanonicalOrderedOperation>(
  operations: T[],
  apply: (operation: T) => void,
  options: ReplayOptions = {}
): ReplayResult {
  const sorted = sortCanonicalOperations(operations);
  const seen = new Set<string>();
  const appliedOpIds: string[] = [];
  let skippedDuplicateCount = 0;

  for (const op of sorted) {
    if (seen.has(op.opId)) {
      skippedDuplicateCount += 1;
      options.onDuplicate?.(op.opId);
      continue;
    }

    seen.add(op.opId);
    apply(op);
    appliedOpIds.push(op.opId);
  }

  return {
    appliedCount: appliedOpIds.length,
    skippedDuplicateCount,
    appliedOpIds
  };
}

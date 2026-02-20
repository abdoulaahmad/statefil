import type { HLCTimestamp } from "../time";
import { HybridLogicalClock } from "../time";

export interface CanonicalOrderedOperation {
  opId: string;
  hlc: HLCTimestamp;
  actorNodeId: string;
  sequence: number;
}

export function compareCanonicalOperation(
  a: CanonicalOrderedOperation,
  b: CanonicalOrderedOperation
): number {
  const byHlc = HybridLogicalClock.compare(a.hlc, b.hlc);
  if (byHlc !== 0) {
    return byHlc;
  }

  const byNode = a.actorNodeId.localeCompare(b.actorNodeId);
  if (byNode !== 0) {
    return byNode;
  }

  return a.sequence - b.sequence;
}

export function sortCanonicalOperations<T extends CanonicalOrderedOperation>(ops: T[]): T[] {
  return [...ops].sort(compareCanonicalOperation);
}

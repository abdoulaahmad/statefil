import type { Mergeable } from "./types";
import type { LWWTieBreakConflictEvent } from "../events";

export interface LWWTimestamp {
  physical: number;
  logical: number;
  nodeId: string;
}

function compareTimestamp(a: LWWTimestamp, b: LWWTimestamp): number {
  if (a.physical !== b.physical) {
    return a.physical - b.physical;
  }
  if (a.logical !== b.logical) {
    return a.logical - b.logical;
  }
  return a.nodeId.localeCompare(b.nodeId);
}

export class LWWRegister<T> implements Mergeable<LWWRegister<T>> {
  private valueRef: T | undefined;
  private timestampRef: LWWTimestamp;
  private actorIdRef: string;
  private logicalCounter: number;
  private readonly onConflict?: (event: LWWTieBreakConflictEvent) => void;

  constructor(
    initial?: T,
    actorId = "actor-0",
    onConflict?: (event: LWWTieBreakConflictEvent) => void
  ) {
    this.valueRef = initial;
    this.actorIdRef = actorId;
    this.logicalCounter = 0;
    this.onConflict = onConflict;
    this.timestampRef = {
      physical: 0,
      logical: 0,
      nodeId: actorId
    };
  }

  get(): T | undefined {
    return this.valueRef;
  }

  set(value: T): void {
    this.logicalCounter += 1;
    this.valueRef = value;
    this.timestampRef = {
      physical: Date.now(),
      logical: this.logicalCounter,
      nodeId: this.actorIdRef
    };
  }

  // Test and replication helper: apply value with explicit timestamp.
  setWithClock(value: T, timestamp: LWWTimestamp, actorId = this.actorIdRef): void {
    const cmp = compareTimestamp(timestamp, this.timestampRef);
    if (cmp > 0 || (cmp === 0 && actorId > this.actorIdRef)) {
      if (cmp === 0 && actorId !== this.actorIdRef) {
        this.onConflict?.({
          type: "lww.tie-break",
          winnerActorId: actorId,
          loserActorId: this.actorIdRef
        });
      }
      this.valueRef = value;
      this.timestampRef = timestamp;
      this.actorIdRef = actorId;
      this.logicalCounter = Math.max(this.logicalCounter, timestamp.logical);
    }
  }

  timestamp(): LWWTimestamp {
    return this.timestampRef;
  }

  actorId(): string {
    return this.actorIdRef;
  }

  merge(other: LWWRegister<T>): LWWRegister<T> {
    const merged = new LWWRegister<T>(this.valueRef, this.actorIdRef, this.onConflict);
    merged.valueRef = this.valueRef;
    merged.timestampRef = this.timestampRef;
    merged.logicalCounter = Math.max(this.logicalCounter, this.timestampRef.logical);

    const cmp = compareTimestamp(other.timestampRef, merged.timestampRef);
    if (cmp > 0 || (cmp === 0 && other.actorIdRef > merged.actorIdRef)) {
      if (cmp === 0 && other.actorIdRef !== merged.actorIdRef) {
        merged.onConflict?.({
          type: "lww.tie-break",
          winnerActorId: other.actorIdRef,
          loserActorId: merged.actorIdRef
        });
      }
      merged.valueRef = other.valueRef;
      merged.timestampRef = other.timestampRef;
      merged.actorIdRef = other.actorIdRef;
      merged.logicalCounter = Math.max(merged.logicalCounter, other.timestampRef.logical);
    }

    return merged;
  }
}

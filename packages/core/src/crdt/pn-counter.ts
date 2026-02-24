import type { Mergeable } from "./types";

export interface PNCounterSnapshot {
  positive: Record<string, number>;
  negative: Record<string, number>;
}

const DEFAULT_NODE_ID = "node-0";

export class PNCounter implements Mergeable<PNCounter> {
  private readonly positive: Map<string, number>;
  private readonly negative: Map<string, number>;
  readonly nodeId: string;

  constructor(nodeId = DEFAULT_NODE_ID, snapshot?: PNCounterSnapshot) {
    this.nodeId = nodeId;
    this.positive = recordToMap(snapshot?.positive);
    this.negative = recordToMap(snapshot?.negative);
  }

  increment(amount = 1): void {
    assertPositiveAmount(amount);
    bump(this.positive, this.nodeId, amount);
  }

  decrement(amount = 1): void {
    assertPositiveAmount(amount);
    bump(this.negative, this.nodeId, amount);
  }

  value(): number {
    return sum(this.positive) - sum(this.negative);
  }

  snapshot(): PNCounterSnapshot {
    return {
      positive: mapToRecord(this.positive),
      negative: mapToRecord(this.negative)
    };
  }

  merge(other: PNCounter): PNCounter {
    return new PNCounter(this.nodeId, {
      positive: mapToRecord(mergeByMax(this.positive, other.positive)),
      negative: mapToRecord(mergeByMax(this.negative, other.negative))
    });
  }
}

export class SafePNCounter implements Mergeable<SafePNCounter> {
  private readonly counter: PNCounter;
  private readonly floorValue: number;
  readonly nodeId: string;

  constructor(nodeId = DEFAULT_NODE_ID, floor = 0, snapshot?: PNCounterSnapshot) {
    if (!Number.isFinite(floor)) {
      throw new Error("floor must be a finite number");
    }

    this.nodeId = nodeId;
    this.floorValue = floor;
    this.counter = new PNCounter(nodeId, snapshot);
  }

  increment(amount = 1): void {
    this.counter.increment(amount);
  }

  tryDecrement(amount = 1): boolean {
    assertPositiveAmount(amount);
    if (this.counter.value() - amount < this.floorValue) {
      return false;
    }
    this.counter.decrement(amount);
    return true;
  }

  decrement(amount = 1): void {
    if (!this.tryDecrement(amount)) {
      throw new Error("SafePNCounter floor would be violated");
    }
  }

  floor(): number {
    return this.floorValue;
  }

  rawValue(): number {
    return this.counter.value();
  }

  value(): number {
    return Math.max(this.floorValue, this.counter.value());
  }

  isFloorApplied(): boolean {
    return this.counter.value() < this.floorValue;
  }

  snapshot(): PNCounterSnapshot {
    return this.counter.snapshot();
  }

  merge(other: SafePNCounter): SafePNCounter {
    const mergedCounter = this.counter.merge(other.counter);
    return new SafePNCounter(
      this.nodeId,
      Math.max(this.floorValue, other.floorValue),
      mergedCounter.snapshot()
    );
  }
}

function recordToMap(source?: Record<string, number>): Map<string, number> {
  const map = new Map<string, number>();
  if (!source) {
    return map;
  }

  for (const [nodeId, value] of Object.entries(source)) {
    if (!Number.isFinite(value) || value < 0) {
      continue;
    }
    map.set(nodeId, value);
  }

  return map;
}

function mapToRecord(source: Map<string, number>): Record<string, number> {
  return Object.fromEntries(source.entries());
}

function bump(target: Map<string, number>, nodeId: string, amount: number): void {
  target.set(nodeId, (target.get(nodeId) ?? 0) + amount);
}

function sum(values: Map<string, number>): number {
  let total = 0;
  for (const value of values.values()) {
    total += value;
  }
  return total;
}

function mergeByMax(left: Map<string, number>, right: Map<string, number>): Map<string, number> {
  const merged = new Map<string, number>();
  const keys = new Set([...left.keys(), ...right.keys()]);

  for (const key of keys) {
    merged.set(key, Math.max(left.get(key) ?? 0, right.get(key) ?? 0));
  }

  return merged;
}

function assertPositiveAmount(amount: number): void {
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error("amount must be a positive finite number");
  }
}

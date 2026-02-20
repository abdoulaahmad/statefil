import type { MetricsCollector } from "../observability";

export interface HLCTimestamp {
  physical: number;
  logical: number;
  nodeId: string;
}

export interface HLCUpdateResult {
  timestamp: HLCTimestamp;
  skewRejected: boolean;
}

export interface HLCConfig {
  nodeId: string;
  maxFutureSkewMs?: number;
  now?: () => number;
  metrics?: MetricsCollector;
}

const DEFAULT_MAX_FUTURE_SKEW_MS = 2 * 60 * 1000;

export class HybridLogicalClock {
  private physical: number;
  private logical: number;
  private readonly nodeId: string;
  private readonly maxFutureSkewMs: number;
  private readonly now: () => number;
  private readonly metrics?: MetricsCollector;

  constructor(config: HLCConfig) {
    this.nodeId = config.nodeId;
    this.maxFutureSkewMs = config.maxFutureSkewMs ?? DEFAULT_MAX_FUTURE_SKEW_MS;
    this.now = config.now ?? Date.now;
    this.metrics = config.metrics;
    this.physical = 0;
    this.logical = 0;
  }

  tick(): HLCTimestamp {
    const wall = this.now();

    if (wall > this.physical) {
      this.physical = wall;
      this.logical = 0;
    } else {
      this.logical += 1;
    }

    return this.current();
  }

  update(received: HLCTimestamp): HLCUpdateResult {
    const wall = this.now();

    if (received.physical > wall + this.maxFutureSkewMs) {
      this.metrics?.increment("statefabric.clock_skew_reject_total");
      return {
        timestamp: this.tick(),
        skewRejected: true
      };
    }

    const nextPhysical = Math.max(this.physical, received.physical, wall);

    if (nextPhysical === this.physical && nextPhysical === received.physical) {
      this.logical = Math.max(this.logical, received.logical) + 1;
    } else if (nextPhysical === this.physical) {
      this.logical += 1;
    } else if (nextPhysical === received.physical) {
      this.logical = received.logical + 1;
    } else {
      this.logical = 0;
    }

    this.physical = nextPhysical;

    return {
      timestamp: this.current(),
      skewRejected: false
    };
  }

  current(): HLCTimestamp {
    return {
      physical: this.physical,
      logical: this.logical,
      nodeId: this.nodeId
    };
  }

  static compare(a: HLCTimestamp, b: HLCTimestamp): number {
    if (a.physical !== b.physical) {
      return a.physical - b.physical;
    }
    if (a.logical !== b.logical) {
      return a.logical - b.logical;
    }
    return a.nodeId.localeCompare(b.nodeId);
  }
}

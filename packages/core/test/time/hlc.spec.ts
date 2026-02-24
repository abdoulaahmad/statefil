import { describe, expect, it } from "vitest";
import { HybridLogicalClock, type HLCTimestamp } from "../../src/time";
import { InMemoryMetricsCollector } from "../../src/observability";

describe("HybridLogicalClock", () => {
  it("ticks forward and resets logical counter when wall time advances", () => {
    let wall = 1000;
    const clock = new HybridLogicalClock({
      nodeId: "node-a",
      now: () => wall
    });

    const t1 = clock.tick();
    const t2 = clock.tick();

    wall = 1001;
    const t3 = clock.tick();

    expect(t1.physical).toBe(1000);
    expect(t2.logical).toBeGreaterThan(t1.logical);
    expect(t3.physical).toBe(1001);
    expect(t3.logical).toBe(0);
  });

  it("rejects poisoned future timestamp above configured skew", () => {
    let wall = 10_000;
    const metrics = new InMemoryMetricsCollector();
    const clock = new HybridLogicalClock({
      nodeId: "node-a",
      now: () => wall,
      maxFutureSkewMs: 100,
      metrics
    });

    clock.tick();

    const poisoned: HLCTimestamp = {
      physical: 20_000,
      logical: 0,
      nodeId: "rogue"
    };

    const result = clock.update(poisoned);

    expect(result.skewRejected).toBe(true);
    expect(result.timestamp.physical).toBe(wall);
    expect(metrics.counter("statefabric.clock_skew_reject_total")).toBe(1);
    expect(metrics.observed("statefabric.clock_skew_reject_delta_ms")).toEqual([10_000]);
  });

  it("accepts a received timestamp exactly at configured skew boundary", () => {
    let wall = 10_000;
    const clock = new HybridLogicalClock({
      nodeId: "node-a",
      now: () => wall,
      maxFutureSkewMs: 100
    });

    clock.tick();

    const atBoundary: HLCTimestamp = {
      physical: 10_100,
      logical: 2,
      nodeId: "node-b"
    };

    const result = clock.update(atBoundary);

    expect(result.skewRejected).toBe(false);
    expect(result.timestamp.physical).toBe(10_100);
    expect(result.timestamp.logical).toBe(3);
  });

  it("normalizes invalid negative skew configuration to zero", () => {
    let wall = 10_000;
    const clock = new HybridLogicalClock({
      nodeId: "node-a",
      now: () => wall,
      maxFutureSkewMs: -1
    });

    clock.tick();
    const result = clock.update({
      physical: 10_001,
      logical: 0,
      nodeId: "node-b"
    });

    expect(result.skewRejected).toBe(true);
    expect(result.timestamp.physical).toBe(10_000);
  });

  it("accepts valid received timestamp and moves clock safely", () => {
    let wall = 5_000;
    const clock = new HybridLogicalClock({
      nodeId: "node-a",
      now: () => wall,
      maxFutureSkewMs: 2_000
    });

    clock.tick();

    const received: HLCTimestamp = {
      physical: 5_500,
      logical: 3,
      nodeId: "node-b"
    };

    const result = clock.update(received);

    expect(result.skewRejected).toBe(false);
    expect(result.timestamp.physical).toBe(5_500);
    expect(result.timestamp.logical).toBe(4);
  });
});

import { describe, expect, it } from "vitest";
import { InMemoryMetricsCollector } from "../../src";

describe("InMemoryMetricsCollector", () => {
  it("accumulates counters and observations", () => {
    const metrics = new InMemoryMetricsCollector();

    metrics.increment("a");
    metrics.increment("a", 2);
    metrics.observe("latency", 5);
    metrics.observe("latency", 10);

    expect(metrics.counter("a")).toBe(3);
    expect(metrics.observed("latency")).toEqual([5, 10]);
    expect(metrics.records().length).toBe(4);
  });
});

import { describe, expect, it } from "vitest";
import type {
  DurableRuntimeStore,
  RecoverySnapshot,
  RuntimeOperation,
  SegmentManifest,
  StoredSegmentGcReport
} from "@statefabric/core";
import { StateFabricClient } from "../src";

class InMemoryRuntimeStore implements DurableRuntimeStore {
  readonly segments = new Map<string, RuntimeOperation[]>();
  manifest: SegmentManifest | null = null;
  snapshot: RecoverySnapshot | null = null;

  async loadManifest(): Promise<SegmentManifest | null> {
    return this.manifest;
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    this.manifest = manifest;
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    this.snapshot = snapshot;
  }

  async appendToSegment(segmentId: string, operations: RuntimeOperation[]): Promise<void> {
    const existing = this.segments.get(segmentId) ?? [];
    existing.push(...operations);
    this.segments.set(segmentId, existing);
  }

  async readSegment(segmentId: string): Promise<RuntimeOperation[]> {
    return [...(this.segments.get(segmentId) ?? [])];
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    return this.snapshot;
  }
}

describe("StateFabricClient", () => {
  it("falls back to the scaffold runtime without a store", () => {
    const client = new StateFabricClient();

    expect(client.unified).toBeUndefined();
    expect(client.runtime).toBeDefined();
  });

  it("wires a durable runtime when a store is provided", async () => {
    const store = new InMemoryRuntimeStore();
    const client = new StateFabricClient({ store, nodeId: "node-a" });

    expect(client.unified).toBeDefined();
    await client.start();

    await client.unified!.eventual.setRegister("session", "u1", "alice");
    await client.unified!.eventual.incrementCounter("rate", "u1", 2);
    await client.flush();
    await client.stop();

    const second = new StateFabricClient({ store, nodeId: "node-a" });
    await second.start();

    expect(second.unified!.eventual.getRegister("session", "u1")).toBe("alice");
    expect(second.unified!.eventual.getCounter("rate", "u1")).toBe(2);
    await second.stop();
  });
});

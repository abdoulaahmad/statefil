import {
  DurableEventualRuntime,
  type SegmentGcOptions,
  type SegmentGcReport,
  type DurableEventualRuntimeOptions,
  type DurableRuntimeStore
} from "./durable-eventual-runtime";
import { InMemoryStrongRuntime, type StrongRuntime, type StrongRuntimeOptions } from "./strong-runtime";
import type { RecoveryResult } from "../log";

export interface UnifiedStateFabricRuntimeOptions {
  eventualStore: DurableRuntimeStore;
  eventual: DurableEventualRuntimeOptions;
  strongRuntime?: StrongRuntime;
  strongOptions?: StrongRuntimeOptions;
}

export class UnifiedStateFabricRuntime {
  readonly eventual: DurableEventualRuntime;
  readonly strong: StrongRuntime;

  constructor(options: UnifiedStateFabricRuntimeOptions) {
    this.eventual = new DurableEventualRuntime(options.eventualStore, options.eventual);
    this.strong = options.strongRuntime ?? new InMemoryStrongRuntime(options.strongOptions);
  }

  async start(): Promise<void> {
    await this.eventual.start();
  }

  async recover(): Promise<RecoveryResult> {
    return this.eventual.recover();
  }

  async flush(): Promise<void> {
    await this.eventual.flush();
  }

  async close(): Promise<void> {
    await this.eventual.close();
  }

  async gcOrphanSegments(options: SegmentGcOptions = {}): Promise<SegmentGcReport> {
    return this.eventual.gcOrphanSegments(options);
  }
}

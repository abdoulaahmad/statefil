import {
  UnifiedStateFabricRuntime,
  createScaffoldRuntime,
  type DurableEventualRuntimeOptions,
  type DurableRuntimeStore,
  type StateFabricRuntime,
  type StrongRuntime,
  type StrongRuntimeOptions
} from "@statefabric/core";

export interface StateFabricClientOptions {
  mode?: "serverless" | "cluster";
  /** Required for durable state; without it the client falls back to the scaffold runtime. */
  store?: DurableRuntimeStore;
  nodeId?: string;
  eventual?: Omit<DurableEventualRuntimeOptions, "nodeId">;
  strongRuntime?: StrongRuntime;
  strongOptions?: StrongRuntimeOptions;
}

export class StateFabricClient {
  /** Scaffold runtime when no store is configured; the real unified runtime otherwise. */
  readonly runtime: StateFabricRuntime | UnifiedStateFabricRuntime;
  /** Real durable runtime when constructed with a store. */
  readonly unified?: UnifiedStateFabricRuntime;

  constructor(options: StateFabricClientOptions = {}) {
    if (options.store) {
      this.unified = new UnifiedStateFabricRuntime({
        eventualStore: options.store,
        eventual: {
          ...options.eventual,
          nodeId: options.nodeId ?? defaultNodeId()
        },
        strongRuntime: options.strongRuntime,
        strongOptions: options.strongOptions
      });
      this.runtime = this.unified;
    } else {
      this.runtime = createScaffoldRuntime();
    }
  }

  async start(): Promise<void> {
    if (this.unified) {
      await this.unified.start();
      return;
    }
    await this.runtime.start();
  }

  async stop(signal?: AbortSignal): Promise<void> {
    if (this.unified) {
      await this.unified.close();
      return;
    }
    await (this.runtime as StateFabricRuntime).stop(signal);
  }

  async flush(): Promise<void> {
    await this.unified?.flush();
  }
}

function defaultNodeId(): string {
  return `node-${Math.random().toString(36).slice(2, 10)}`;
}

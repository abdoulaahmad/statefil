import { createScaffoldRuntime, type StateFabricRuntime } from "@statefabric/core";

export interface StateFabricClientOptions {
  mode?: "serverless" | "cluster";
}

export class StateFabricClient {
  readonly runtime: StateFabricRuntime;

  constructor(_options: StateFabricClientOptions = {}) {
    this.runtime = createScaffoldRuntime();
  }
}

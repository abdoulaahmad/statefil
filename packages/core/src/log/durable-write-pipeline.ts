import type {
  DurableAppender,
  OperationEnvelope,
  StateApplier,
  WriteResult
} from "./types";

export class DurableWritePipeline {
  constructor(
    private readonly appender: DurableAppender,
    private readonly applier: StateApplier
  ) {}

  async write(operation: OperationEnvelope): Promise<WriteResult> {
    // Hard guarantee: durable append must happen before in-memory apply/ack.
    await this.appender.append(operation);
    this.applier.apply(operation);

    return {
      ack: true,
      opId: operation.opId
    };
  }
}

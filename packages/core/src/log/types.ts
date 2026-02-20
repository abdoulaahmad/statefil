export interface OperationEnvelope {
  opId: string;
  namespace: string;
  key: string;
  type: string;
  payload: unknown;
}

export interface DurableAppender {
  append(operation: OperationEnvelope): Promise<void>;
}

export interface StateApplier {
  apply(operation: OperationEnvelope): void;
}

export interface WriteResult {
  ack: true;
  opId: string;
}

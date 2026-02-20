export class NotImplementedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NotImplementedError";
  }
}

export class ConsensusUnavailableError extends Error {
  constructor(message = "Strong consistency requires available consensus") {
    super(message);
    this.name = "ConsensusUnavailableError";
  }
}

export class RetryableConflictError extends Error {
  readonly retryable = true;

  constructor(message = "Retryable transaction conflict") {
    super(message);
    this.name = "RetryableConflictError";
  }
}

export class CounterBoundsError extends Error {
  constructor(message = "Counter operation would violate configured bounds") {
    super(message);
    this.name = "CounterBoundsError";
  }
}

export class UnsupportedVersionError extends Error {
  constructor(message = "Unsupported document version") {
    super(message);
    this.name = "UnsupportedVersionError";
  }
}

export class MigrationPathError extends Error {
  constructor(message = "No migration path found") {
    super(message);
    this.name = "MigrationPathError";
  }
}

export class InvalidDocumentError extends Error {
  constructor(message = "Invalid encoded document") {
    super(message);
    this.name = "InvalidDocumentError";
  }
}

export interface ConsensusAdapter {
  isAvailable(): boolean;
  beforeCommit?(attempt: number): void | Promise<void>;
}

export class StaticConsensusAdapter implements ConsensusAdapter {
  private available: boolean;

  constructor(available = true) {
    this.available = available;
  }

  isAvailable(): boolean {
    return this.available;
  }

  setAvailable(available: boolean): void {
    this.available = available;
  }
}

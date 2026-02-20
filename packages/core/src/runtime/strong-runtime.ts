import {
  ConsensusUnavailableError,
  CounterBoundsError,
  RetryableConflictError
} from "../errors";
import { StaticConsensusAdapter, type ConsensusAdapter } from "./consensus-adapter";

export interface StrongRuntimeOptions {
  consensusAvailable?: boolean;
  consensusAdapter?: ConsensusAdapter;
  maxTransactionRetries?: number;
  onBeforeCommit?: (attempt: number) => void | Promise<void>;
}

export interface StrongTransactionContext {
  getRegister<T>(namespace: string, key: string): T | undefined;
  setRegister<T>(namespace: string, key: string, value: T): void;
  compareAndSwapRegister<T>(
    namespace: string,
    key: string,
    expected: T,
    next: T
  ): boolean;

  getCounter(namespace: string, key: string): number;
  incrementCounter(namespace: string, key: string, amount?: number): number;
  decrementCounter(namespace: string, key: string, amount?: number, min?: number): number;

  getMapEntry(namespace: string, key: string): Record<string, unknown> | undefined;
  setMapEntry(namespace: string, key: string, value: Record<string, unknown>): void;
  setMapField(namespace: string, key: string, field: string, value: unknown): void;
  deleteMapField(namespace: string, key: string, field: string): void;
}

export interface StrongRuntime {
  getRegister<T>(namespace: string, key: string): Promise<T | undefined>;
  setRegister<T>(namespace: string, key: string, value: T): Promise<void>;
  compareAndSwapRegister<T>(
    namespace: string,
    key: string,
    expected: T,
    next: T
  ): Promise<boolean>;

  getCounter(namespace: string, key: string): Promise<number>;
  incrementCounter(namespace: string, key: string, amount?: number): Promise<number>;
  decrementCounter(namespace: string, key: string, amount?: number, min?: number): Promise<number>;

  getMapEntry(namespace: string, key: string): Promise<Record<string, unknown> | undefined>;
  setMapEntry(namespace: string, key: string, value: Record<string, unknown>): Promise<void>;
  setMapField(namespace: string, key: string, field: string, value: unknown): Promise<void>;
  deleteMapField(namespace: string, key: string, field: string): Promise<void>;

  transaction<T>(fn: (tx: StrongTransactionContext) => T | Promise<T>): Promise<T>;
}

class AsyncMutex {
  private tail: Promise<void> = Promise.resolve();

  async runExclusive<T>(fn: () => Promise<T> | T): Promise<T> {
    const prev = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>(resolve => {
      release = resolve;
    });

    await prev;
    try {
      return await fn();
    } finally {
      release();
    }
  }
}

export class InMemoryStrongRuntime implements StrongRuntime {
  private readonly mutex = new AsyncMutex();
  private readonly consensusAdapter: ConsensusAdapter;
  private readonly maxTransactionRetries: number;
  private readonly onBeforeCommit?: (attempt: number) => void | Promise<void>;

  private registers = new Map<string, unknown>();
  private counters = new Map<string, number>();
  private maps = new Map<string, Map<string, unknown>>();

  constructor(options: StrongRuntimeOptions = {}) {
    this.consensusAdapter =
      options.consensusAdapter ?? new StaticConsensusAdapter(options.consensusAvailable ?? true);
    this.maxTransactionRetries = options.maxTransactionRetries ?? 0;
    this.onBeforeCommit = options.onBeforeCommit;
  }

  async getRegister<T>(namespace: string, key: string): Promise<T | undefined> {
    this.ensureConsensus();
    return this.mutex.runExclusive(async () => {
      return this.registers.get(composeKey(namespace, key)) as T | undefined;
    });
  }

  async setRegister<T>(namespace: string, key: string, value: T): Promise<void> {
    this.ensureConsensus();
    await this.mutex.runExclusive(async () => {
      this.registers.set(composeKey(namespace, key), value);
    });
  }

  async compareAndSwapRegister<T>(
    namespace: string,
    key: string,
    expected: T,
    next: T
  ): Promise<boolean> {
    this.ensureConsensus();
    return this.mutex.runExclusive(async () => {
      const mapKey = composeKey(namespace, key);
      const current = this.registers.get(mapKey) as T | undefined;
      if (!Object.is(current, expected)) {
        return false;
      }
      this.registers.set(mapKey, next);
      return true;
    });
  }

  async getCounter(namespace: string, key: string): Promise<number> {
    this.ensureConsensus();
    return this.mutex.runExclusive(async () => {
      return this.counters.get(composeKey(namespace, key)) ?? 0;
    });
  }

  async incrementCounter(namespace: string, key: string, amount = 1): Promise<number> {
    this.ensureConsensus();
    assertPositiveAmount(amount);

    return this.mutex.runExclusive(async () => {
      const mapKey = composeKey(namespace, key);
      const next = (this.counters.get(mapKey) ?? 0) + amount;
      this.counters.set(mapKey, next);
      return next;
    });
  }

  async decrementCounter(namespace: string, key: string, amount = 1, min = 0): Promise<number> {
    this.ensureConsensus();
    assertPositiveAmount(amount);

    return this.mutex.runExclusive(async () => {
      const mapKey = composeKey(namespace, key);
      const current = this.counters.get(mapKey) ?? 0;
      const next = current - amount;

      if (next < min) {
        throw new CounterBoundsError(
          `Counter ${namespace}:${key} would fall below min=${min} (current=${current}, amount=${amount})`
        );
      }

      this.counters.set(mapKey, next);
      return next;
    });
  }

  async getMapEntry(namespace: string, key: string): Promise<Record<string, unknown> | undefined> {
    this.ensureConsensus();
    return this.mutex.runExclusive(async () => {
      return mapToRecord(this.maps.get(composeKey(namespace, key)));
    });
  }

  async setMapEntry(namespace: string, key: string, value: Record<string, unknown>): Promise<void> {
    this.ensureConsensus();
    await this.mutex.runExclusive(async () => {
      this.maps.set(composeKey(namespace, key), recordToMap(value));
    });
  }

  async setMapField(namespace: string, key: string, field: string, value: unknown): Promise<void> {
    this.ensureConsensus();
    await this.mutex.runExclusive(async () => {
      const mapKey = composeKey(namespace, key);
      const target = this.ensureMap(this.maps, mapKey);
      target.set(field, value);
    });
  }

  async deleteMapField(namespace: string, key: string, field: string): Promise<void> {
    this.ensureConsensus();
    await this.mutex.runExclusive(async () => {
      const mapKey = composeKey(namespace, key);
      const target = this.maps.get(mapKey);
      if (!target) {
        return;
      }
      target.delete(field);
      if (target.size === 0) {
        this.maps.delete(mapKey);
      }
    });
  }

  async transaction<T>(fn: (tx: StrongTransactionContext) => T | Promise<T>): Promise<T> {
    this.ensureConsensus();

    return this.mutex.runExclusive(async () => {
      let attempt = 0;

      while (true) {
        attempt += 1;
        const stagedRegisters = new Map(this.registers);
        const stagedCounters = new Map(this.counters);
        const stagedMaps = cloneMapOfMaps(this.maps);

        const tx: StrongTransactionContext = {
          getRegister: <V>(namespace: string, key: string): V | undefined => {
            return stagedRegisters.get(composeKey(namespace, key)) as V | undefined;
          },
          setRegister: <V>(namespace: string, key: string, value: V): void => {
            stagedRegisters.set(composeKey(namespace, key), value);
          },
          compareAndSwapRegister: <V>(
            namespace: string,
            key: string,
            expected: V,
            next: V
          ): boolean => {
            const mapKey = composeKey(namespace, key);
            const current = stagedRegisters.get(mapKey) as V | undefined;
            if (!Object.is(current, expected)) {
              return false;
            }
            stagedRegisters.set(mapKey, next);
            return true;
          },
          getCounter: (namespace: string, key: string): number => {
            return stagedCounters.get(composeKey(namespace, key)) ?? 0;
          },
          incrementCounter: (namespace: string, key: string, amount = 1): number => {
            assertPositiveAmount(amount);
            const mapKey = composeKey(namespace, key);
            const next = (stagedCounters.get(mapKey) ?? 0) + amount;
            stagedCounters.set(mapKey, next);
            return next;
          },
          decrementCounter: (namespace: string, key: string, amount = 1, min = 0): number => {
            assertPositiveAmount(amount);
            const mapKey = composeKey(namespace, key);
            const current = stagedCounters.get(mapKey) ?? 0;
            const next = current - amount;
            if (next < min) {
              throw new CounterBoundsError(
                `Counter ${namespace}:${key} would fall below min=${min} (current=${current}, amount=${amount})`
              );
            }
            stagedCounters.set(mapKey, next);
            return next;
          },
          getMapEntry: (namespace: string, key: string): Record<string, unknown> | undefined => {
            return mapToRecord(stagedMaps.get(composeKey(namespace, key)));
          },
          setMapEntry: (namespace: string, key: string, value: Record<string, unknown>): void => {
            stagedMaps.set(composeKey(namespace, key), recordToMap(value));
          },
          setMapField: (namespace: string, key: string, field: string, value: unknown): void => {
            const mapKey = composeKey(namespace, key);
            const target = this.ensureMap(stagedMaps, mapKey);
            target.set(field, value);
          },
          deleteMapField: (namespace: string, key: string, field: string): void => {
            const mapKey = composeKey(namespace, key);
            const target = stagedMaps.get(mapKey);
            if (!target) {
              return;
            }
            target.delete(field);
            if (target.size === 0) {
              stagedMaps.delete(mapKey);
            }
          }
        };

        try {
          const result = await fn(tx);
          await this.consensusAdapter.beforeCommit?.(attempt);
          await this.onBeforeCommit?.(attempt);
          this.ensureConsensus();

          this.registers = stagedRegisters;
          this.counters = stagedCounters;
          this.maps = stagedMaps;
          return result;
        } catch (error) {
          if (error instanceof RetryableConflictError && attempt <= this.maxTransactionRetries) {
            continue;
          }
          throw error;
        }
      }
    });
  }

  private ensureConsensus(): void {
    if (!this.consensusAdapter.isAvailable()) {
      throw new ConsensusUnavailableError();
    }
  }

  private ensureMap(
    container: Map<string, Map<string, unknown>>,
    mapKey: string
  ): Map<string, unknown> {
    if (!container.has(mapKey)) {
      container.set(mapKey, new Map());
    }
    return container.get(mapKey)!;
  }
}

function composeKey(namespace: string, key: string): string {
  return `${namespace}:${key}`;
}

function assertPositiveAmount(amount: number): void {
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new RangeError(`amount must be a positive finite number, got ${amount}`);
  }
}

function recordToMap(record: Record<string, unknown>): Map<string, unknown> {
  return new Map(Object.entries(record));
}

function mapToRecord(map: Map<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (!map) {
    return undefined;
  }

  const out: Record<string, unknown> = {};
  for (const [key, value] of map) {
    out[key] = value;
  }
  return out;
}

function cloneMapOfMaps(
  source: Map<string, Map<string, unknown>>
): Map<string, Map<string, unknown>> {
  const cloned = new Map<string, Map<string, unknown>>();
  for (const [key, map] of source) {
    cloned.set(key, new Map(map));
  }
  return cloned;
}

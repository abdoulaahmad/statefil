export interface EventualCounter {
  value(): number;
  increment(amount?: number): void;
  decrement(amount?: number): void;
}

export interface StrongCounter {
  value(): Promise<number>;
  increment(amount?: number): Promise<void>;
  decrement(amount?: number): Promise<void>;
}

export interface EventualSet<T> {
  add(value: T): void;
  remove(value: T): void;
  has(value: T): boolean;
  values(): T[];
}

export interface EventualRegister<T> {
  get(): T | undefined;
  set(value: T): void;
}

export interface StrongRegister<T> {
  get(): Promise<T | undefined>;
  set(value: T): Promise<void>;
  compareAndSwap(expected: T, next: T): Promise<boolean>;
}

export interface EventualMap {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  delete(key: string): void;
}

export interface StrongMap {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<void>;
}

export interface EventualApi {
  counter(ns: string, key: string): EventualCounter;
  set<T>(ns: string, key: string): EventualSet<T>;
  register<T>(ns: string, key: string): EventualRegister<T>;
  map(ns: string, key: string): EventualMap;
  batch<T>(fn: () => T | Promise<T>): Promise<T>;
}

export interface StrongApi {
  counter(ns: string, key: string): StrongCounter;
  register<T>(ns: string, key: string): StrongRegister<T>;
  map(ns: string, key: string): StrongMap;
  transaction<T>(fn: () => T | Promise<T>): Promise<T>;
}

export interface AdminApi {
  snapshot(): Promise<string>;
  health(): Promise<{ ready: boolean }>;
}

export interface StateFabricRuntime {
  eventual: EventualApi;
  strong: StrongApi;
  admin: AdminApi;
  start(): Promise<void>;
  stop(signal?: AbortSignal): Promise<void>;
}

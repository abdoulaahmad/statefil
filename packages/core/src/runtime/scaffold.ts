import { NotImplementedError } from "../errors";
import type {
  AdminApi,
  EventualApi,
  EventualCounter,
  EventualMap,
  EventualRegister,
  EventualSet,
  StateFabricRuntime,
  StrongApi,
  StrongCounter,
  StrongMap,
  StrongRegister
} from "../types/runtime";

function notImplemented(method: string): never {
  throw new NotImplementedError(`Scaffold method not implemented: ${method}`);
}

class ScaffoldEventualCounter implements EventualCounter {
  value(): number {
    return notImplemented("eventual.counter.value");
  }

  increment(_amount?: number): void {
    notImplemented("eventual.counter.increment");
  }

  decrement(_amount?: number): void {
    notImplemented("eventual.counter.decrement");
  }
}

class ScaffoldStrongCounter implements StrongCounter {
  async value(): Promise<number> {
    return notImplemented("strong.counter.value");
  }

  async increment(_amount?: number): Promise<void> {
    notImplemented("strong.counter.increment");
  }

  async decrement(_amount?: number): Promise<void> {
    notImplemented("strong.counter.decrement");
  }
}

class ScaffoldEventualSet<T> implements EventualSet<T> {
  add(_value: T): void {
    notImplemented("eventual.set.add");
  }

  remove(_value: T): void {
    notImplemented("eventual.set.remove");
  }

  has(_value: T): boolean {
    return notImplemented("eventual.set.has");
  }

  values(): T[] {
    return notImplemented("eventual.set.values");
  }
}

class ScaffoldEventualRegister<T> implements EventualRegister<T> {
  get(): T | undefined {
    return notImplemented("eventual.register.get");
  }

  set(_value: T): void {
    notImplemented("eventual.register.set");
  }
}

class ScaffoldStrongRegister<T> implements StrongRegister<T> {
  async get(): Promise<T | undefined> {
    return notImplemented("strong.register.get");
  }

  async set(_value: T): Promise<void> {
    notImplemented("strong.register.set");
  }

  async compareAndSwap(_expected: T, _next: T): Promise<boolean> {
    return notImplemented("strong.register.compareAndSwap");
  }
}

class ScaffoldEventualMap implements EventualMap {
  get(_key: string): unknown {
    return notImplemented("eventual.map.get");
  }

  set(_key: string, _value: unknown): void {
    notImplemented("eventual.map.set");
  }

  delete(_key: string): void {
    notImplemented("eventual.map.delete");
  }
}

class ScaffoldStrongMap implements StrongMap {
  async get(_key: string): Promise<unknown> {
    return notImplemented("strong.map.get");
  }

  async set(_key: string, _value: unknown): Promise<void> {
    notImplemented("strong.map.set");
  }

  async delete(_key: string): Promise<void> {
    notImplemented("strong.map.delete");
  }
}

class ScaffoldEventualApi implements EventualApi {
  counter(_ns: string, _key: string): EventualCounter {
    return new ScaffoldEventualCounter();
  }

  set<T>(_ns: string, _key: string): EventualSet<T> {
    return new ScaffoldEventualSet<T>();
  }

  register<T>(_ns: string, _key: string): EventualRegister<T> {
    return new ScaffoldEventualRegister<T>();
  }

  map(_ns: string, _key: string): EventualMap {
    return new ScaffoldEventualMap();
  }

  async batch<T>(fn: () => T | Promise<T>): Promise<T> {
    return await fn();
  }
}

class ScaffoldStrongApi implements StrongApi {
  counter(_ns: string, _key: string): StrongCounter {
    return new ScaffoldStrongCounter();
  }

  register<T>(_ns: string, _key: string): StrongRegister<T> {
    return new ScaffoldStrongRegister<T>();
  }

  map(_ns: string, _key: string): StrongMap {
    return new ScaffoldStrongMap();
  }

  async transaction<T>(_fn: () => T | Promise<T>): Promise<T> {
    return notImplemented("strong.transaction");
  }
}

class ScaffoldAdminApi implements AdminApi {
  async snapshot(): Promise<string> {
    return notImplemented("admin.snapshot");
  }

  async health(): Promise<{ ready: boolean }> {
    return notImplemented("admin.health");
  }
}

export class ScaffoldStateFabricRuntime implements StateFabricRuntime {
  eventual: EventualApi = new ScaffoldEventualApi();
  strong: StrongApi = new ScaffoldStrongApi();
  admin: AdminApi = new ScaffoldAdminApi();

  async start(): Promise<void> {
    notImplemented("runtime.start");
  }

  async stop(_signal?: AbortSignal): Promise<void> {
    notImplemented("runtime.stop");
  }
}

export function createScaffoldRuntime(): StateFabricRuntime {
  return new ScaffoldStateFabricRuntime();
}

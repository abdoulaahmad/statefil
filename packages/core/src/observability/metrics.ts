export type MetricTags = Record<string, string | number | boolean>;

export interface MetricsCollector {
  increment(name: string, value?: number, tags?: MetricTags): void;
  observe(name: string, value: number, tags?: MetricTags): void;
}

interface MetricRecord {
  name: string;
  value: number;
  tags?: MetricTags;
}

export class InMemoryMetricsCollector implements MetricsCollector {
  private readonly counters = new Map<string, number>();
  private readonly observations = new Map<string, number[]>();
  private readonly events: MetricRecord[] = [];

  increment(name: string, value = 1, tags?: MetricTags): void {
    const current = this.counters.get(name) ?? 0;
    this.counters.set(name, current + value);
    this.events.push({ name, value, tags });
  }

  observe(name: string, value: number, tags?: MetricTags): void {
    if (!this.observations.has(name)) {
      this.observations.set(name, []);
    }
    this.observations.get(name)!.push(value);
    this.events.push({ name, value, tags });
  }

  counter(name: string): number {
    return this.counters.get(name) ?? 0;
  }

  observed(name: string): number[] {
    return [...(this.observations.get(name) ?? [])];
  }

  records(): MetricRecord[] {
    return [...this.events];
  }
}

export class NoopMetricsCollector implements MetricsCollector {
  increment(_name: string, _value?: number, _tags?: MetricTags): void {}

  observe(_name: string, _value: number, _tags?: MetricTags): void {}
}

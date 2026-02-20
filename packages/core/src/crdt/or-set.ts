import type { Mergeable } from "./types";

export class ORSet<T> implements Mergeable<ORSet<T>> {
  private readonly elements: Map<T, Set<string>>;
  private readonly tombstones: Map<T, Set<string>>;
  private nextTagId: number;

  constructor(
    elements?: Map<T, Set<string>>,
    tombstones?: Map<T, Set<string>>,
    nextTagId = 0
  ) {
    this.elements = elements ? cloneSetMap(elements) : new Map();
    this.tombstones = tombstones ? cloneSetMap(tombstones) : new Map();
    this.nextTagId = nextTagId;
  }

  add(element: T, tag?: string): void {
    const resolvedTag = tag ?? this.makeTag();
    if (!this.elements.has(element)) {
      this.elements.set(element, new Set());
    }

    const tombstones = this.tombstones.get(element);
    if (tombstones?.has(resolvedTag)) {
      return;
    }

    this.elements.get(element)!.add(resolvedTag);
  }

  remove(element: T): void {
    const existing = this.elements.get(element);
    if (!existing || existing.size === 0) {
      return;
    }

    if (!this.tombstones.has(element)) {
      this.tombstones.set(element, new Set());
    }

    for (const tag of existing) {
      this.tombstones.get(element)!.add(tag);
    }

    this.elements.delete(element);
  }

  has(element: T): boolean {
    const tags = this.elements.get(element);
    return !!tags && tags.size > 0;
  }

  values(): T[] {
    const out: T[] = [];
    for (const [element, tags] of this.elements) {
      if (tags.size > 0) {
        out.push(element);
      }
    }
    return out;
  }

  merge(other: ORSet<T>): ORSet<T> {
    const merged = new ORSet<T>();

    // Merge tombstones from both replicas first (prevents delete resurrection).
    mergeSetMapInPlace(merged.tombstones, this.tombstones);
    mergeSetMapInPlace(merged.tombstones, other.tombstones);

    mergeElementsFilteringTombstones(merged.elements, merged.tombstones, this.elements);
    mergeElementsFilteringTombstones(merged.elements, merged.tombstones, other.elements);

    // Remove keys that ended up with no live tags.
    for (const [element, tags] of merged.elements) {
      if (tags.size === 0) {
        merged.elements.delete(element);
      }
    }

    merged.nextTagId = Math.max(this.nextTagId, other.nextTagId);
    return merged;
  }

  private makeTag(): string {
    this.nextTagId += 1;
    return `tag-${this.nextTagId}`;
  }
}

function cloneSetMap<T>(source: Map<T, Set<string>>): Map<T, Set<string>> {
  const cloned = new Map<T, Set<string>>();
  for (const [key, values] of source) {
    cloned.set(key, new Set(values));
  }
  return cloned;
}

function mergeSetMapInPlace<T>(target: Map<T, Set<string>>, source: Map<T, Set<string>>): void {
  for (const [key, values] of source) {
    if (!target.has(key)) {
      target.set(key, new Set());
    }
    const targetValues = target.get(key)!;
    for (const value of values) {
      targetValues.add(value);
    }
  }
}

function mergeElementsFilteringTombstones<T>(
  target: Map<T, Set<string>>,
  tombstones: Map<T, Set<string>>,
  source: Map<T, Set<string>>
): void {
  for (const [element, tags] of source) {
    if (!target.has(element)) {
      target.set(element, new Set());
    }
    const targetTags = target.get(element)!;
    const elementTombstones = tombstones.get(element);

    for (const tag of tags) {
      if (!elementTombstones?.has(tag)) {
        targetTags.add(tag);
      }
    }
  }
}

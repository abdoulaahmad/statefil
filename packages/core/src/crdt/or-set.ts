import type { Mergeable } from "./types";

export type ORSetLiveTagWatermark = Record<string, number>;

export class ORSet<T> implements Mergeable<ORSet<T>> {
  private readonly elements: Map<T, Set<string>>;
  private readonly tombstones: Map<T, Set<string>>;
  private readonly actorId: string;
  private nextTagId: number;

  constructor(
    elements?: Map<T, Set<string>>,
    tombstones?: Map<T, Set<string>>,
    nextTagId = 0,
    actorId = "actor-0"
  ) {
    this.elements = elements ? cloneSetMap(elements) : new Map();
    this.tombstones = tombstones ? cloneSetMap(tombstones) : new Map();
    this.nextTagId = nextTagId;
    this.actorId = actorId;
  }

  add(element: T, tag?: string): void {
    const resolvedTag = tag ?? this.makeTag();
    const parsedTag = parseOrTag(resolvedTag);
    if (parsedTag && parsedTag.actorId === this.actorId) {
      this.nextTagId = Math.max(this.nextTagId, parsedTag.sequence);
    }
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

  tombstoneCount(): number {
    let count = 0;
    for (const tags of this.tombstones.values()) {
      count += tags.size;
    }
    return count;
  }

  // Compacts tombstones that are causally stable according to the supplied
  // actor->sequence watermark. Tags not parseable as "<actor>:<sequence>"
  // are retained for safety.
  compactTombstones(liveTagWatermark: ORSetLiveTagWatermark): number {
    let removed = 0;

    for (const [element, tombstones] of this.tombstones) {
      const liveTags = this.elements.get(element);
      for (const tag of [...tombstones]) {
        if (liveTags?.has(tag)) {
          continue;
        }

        const parsed = parseOrTag(tag);
        if (!parsed) {
          continue;
        }

        const watermark = liveTagWatermark[parsed.actorId];
        if (typeof watermark !== "number" || !Number.isFinite(watermark)) {
          continue;
        }

        if (parsed.sequence <= Math.floor(watermark)) {
          tombstones.delete(tag);
          removed += 1;
        }
      }

      if (tombstones.size === 0) {
        this.tombstones.delete(element);
      }
    }

    return removed;
  }

  merge(other: ORSet<T>): ORSet<T> {
    const merged = new ORSet<T>(
      undefined,
      undefined,
      Math.max(this.nextTagId, other.nextTagId),
      this.actorId
    );

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

    return merged;
  }

  private makeTag(): string {
    this.nextTagId += 1;
    return `${this.actorId}:${this.nextTagId}`;
  }
}

function parseOrTag(tag: string): { actorId: string; sequence: number } | null {
  const parts = tag.split(":");
  if (parts.length !== 2) {
    return null;
  }

  const actorId = parts[0];
  const sequence = Number(parts[1]);
  if (!actorId || !Number.isFinite(sequence) || sequence < 0) {
    return null;
  }

  return {
    actorId,
    sequence: Math.floor(sequence)
  };
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

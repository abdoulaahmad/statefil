import {
  InvalidDocumentError,
  MigrationPathError,
  UnsupportedVersionError
} from "../errors";

export interface VersionedEnvelope<TData> {
  schema: string;
  version: string;
  data: TData;
}

export interface MigrationStep<TData> {
  from: string;
  to: string;
  migrate(data: TData): TData;
}

export class MigrationRegistry<TData> {
  private readonly edges = new Map<string, MigrationStep<TData>[]>();

  register(step: MigrationStep<TData>): void {
    if (!this.edges.has(step.from)) {
      this.edges.set(step.from, []);
    }
    this.edges.get(step.from)!.push(step);
  }

  hasPath(from: string, to: string): boolean {
    if (from === to) {
      return true;
    }

    return this.findPath(from, to) !== null;
  }

  migrate(from: string, to: string, data: TData): TData {
    if (from === to) {
      return data;
    }

    const path = this.findPath(from, to);
    if (!path) {
      throw new MigrationPathError(`No migration path from ${from} to ${to}`);
    }

    let current = data;
    for (const step of path) {
      current = step.migrate(current);
    }
    return current;
  }

  private findPath(from: string, to: string): MigrationStep<TData>[] | null {
    type QueueItem = {
      version: string;
      path: MigrationStep<TData>[];
    };

    const visited = new Set<string>([from]);
    const queue: QueueItem[] = [{ version: from, path: [] }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const nextSteps = this.edges.get(current.version) ?? [];

      for (const step of nextSteps) {
        if (visited.has(step.to)) {
          continue;
        }

        const path = [...current.path, step];
        if (step.to === to) {
          return path;
        }

        visited.add(step.to);
        queue.push({ version: step.to, path });
      }
    }

    return null;
  }
}

export interface VersionedJsonCodecOptions<TData> {
  schema: string;
  currentVersion: string;
  migrations?: MigrationRegistry<TData>;
}

export class VersionedJsonCodec<TData> {
  private readonly schema: string;
  private readonly currentVersion: string;
  private readonly migrations?: MigrationRegistry<TData>;

  constructor(options: VersionedJsonCodecOptions<TData>) {
    this.schema = options.schema;
    this.currentVersion = options.currentVersion;
    this.migrations = options.migrations;
  }

  encode(data: TData): string {
    const envelope: VersionedEnvelope<TData> = {
      schema: this.schema,
      version: this.currentVersion,
      data
    };
    return JSON.stringify(envelope);
  }

  decode(raw: string): TData {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new InvalidDocumentError("Document is not valid JSON");
    }

    if (!isVersionedEnvelope(parsed)) {
      throw new InvalidDocumentError("Document is missing schema/version/data envelope fields");
    }

    if (parsed.schema !== this.schema) {
      throw new InvalidDocumentError(
        `Schema mismatch: expected ${this.schema}, got ${parsed.schema}`
      );
    }

    if (parsed.version === this.currentVersion) {
      return parsed.data as TData;
    }

    if (!this.migrations) {
      throw new UnsupportedVersionError(
        `No migrations configured for ${this.schema} ${parsed.version} -> ${this.currentVersion}`
      );
    }

    if (!this.migrations.hasPath(parsed.version, this.currentVersion)) {
      throw new UnsupportedVersionError(
        `Unsupported version transition ${parsed.version} -> ${this.currentVersion}`
      );
    }

    return this.migrations.migrate(parsed.version, this.currentVersion, parsed.data as TData);
  }

  current(): string {
    return this.currentVersion;
  }
}

function isVersionedEnvelope(value: unknown): value is VersionedEnvelope<unknown> {
  if (!value || typeof value !== "object") {
    return false;
  }

  const envelope = value as Record<string, unknown>;
  return (
    typeof envelope.schema === "string" &&
    typeof envelope.version === "string" &&
    Object.prototype.hasOwnProperty.call(envelope, "data")
  );
}

import {
  decodeManifestDocument,
  decodeOperationDocument,
  decodeSnapshotDocument,
  encodeManifestDocument,
  encodeOperationDocument,
  encodeSnapshotDocument,
  type DurableRuntimeStore,
  type RecoverySnapshot,
  type RuntimeOperation,
  type SegmentManifest,
  type StoredSegmentGcReport
} from "@statefabric/core";

export interface S3AdapterConfig {
  bucket: string;
  prefix: string;
}

export interface S3ClientLike {
  putObject(input: { Bucket: string; Key: string; Body: string }): Promise<void>;
  getObject(input: { Bucket: string; Key: string }): Promise<{ Body: string }>;
  deleteObject?(input: { Bucket: string; Key: string }): Promise<void>;
  listObjects?(input: { Bucket: string; Prefix: string }): Promise<{ Keys: string[] }>;
}

export class S3PersistenceAdapter {
  private readonly client: S3ClientLike;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(client: S3ClientLike, config: S3AdapterConfig) {
    this.client = client;
    this.bucket = config.bucket;
    this.prefix = normalizePrefix(config.prefix);
  }

  async appendSegment(segmentId: string, content: string): Promise<void> {
    const key = this.segmentKey(segmentId);
    const existing = (await this.readObjectIfExists(key)) ?? "";

    await this.client.putObject({
      Bucket: this.bucket,
      Key: key,
      Body: `${existing}${content}`
    });
  }

  async readSegment(segmentId: string): Promise<string | null> {
    return this.readObjectIfExists(this.segmentKey(segmentId));
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    await this.client.putObject({
      Bucket: this.bucket,
      Key: this.manifestKey(),
      Body: encodeManifestDocument(manifest)
    });
  }

  async loadManifest(): Promise<SegmentManifest | null> {
    const raw = await this.readObjectIfExists(this.manifestKey());
    if (!raw) {
      return null;
    }
    return decodeManifestDocument(raw);
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    await this.client.putObject({
      Bucket: this.bucket,
      Key: this.snapshotKey(),
      Body: encodeSnapshotDocument(snapshot)
    });
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    const raw = await this.readObjectIfExists(this.snapshotKey());
    if (!raw) {
      return null;
    }
    return decodeSnapshotDocument(raw);
  }

  async appendSegmentOperations(segmentId: string, operations: RuntimeOperation[]): Promise<void> {
    if (operations.length === 0) {
      return;
    }

    const lines = operations.map(operation => encodeOperationDocument(operation)).join("\n");
    await this.appendSegment(segmentId, `${lines}\n`);
  }

  async readSegmentOperations(segmentId: string): Promise<RuntimeOperation[]> {
    const raw = await this.readSegment(segmentId);
    if (!raw) {
      return [];
    }

    return raw
      .split("\n")
      .map(line => line.trim())
      .filter(Boolean)
      .map(line => decodeOperationDocument(line));
  }

  async listSegments(): Promise<string[]> {
    if (!this.client.listObjects) {
      return [];
    }

    const prefix = `${this.prefix}segments/`;
    const { Keys } = await this.client.listObjects({
      Bucket: this.bucket,
      Prefix: prefix
    });

    return Keys
      .filter(key => key.startsWith(prefix) && key.endsWith(".jsonl"))
      .map(key => key.slice(prefix.length, -".jsonl".length))
      .sort();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    if (!this.client.deleteObject) {
      return;
    }

    try {
      await this.client.deleteObject({
        Bucket: this.bucket,
        Key: this.segmentKey(segmentId)
      });
    } catch (error) {
      const code = (error as { code?: string }).code;
      if (code === "NoSuchKey" || code === "NotFound") {
        return;
      }
      throw error;
    }
  }

  async saveSegmentGcReport(report: StoredSegmentGcReport): Promise<void> {
    await this.client.putObject({
      Bucket: this.bucket,
      Key: this.gcReportKey(report.executionId),
      Body: JSON.stringify(report)
    });
  }

  async loadSegmentGcReport(executionId: string): Promise<StoredSegmentGcReport | null> {
    const raw = await this.readObjectIfExists(this.gcReportKey(executionId));
    if (!raw) {
      return null;
    }
    return JSON.parse(raw) as StoredSegmentGcReport;
  }

  private manifestKey(): string {
    return `${this.prefix}manifest.json`;
  }

  private snapshotKey(): string {
    return `${this.prefix}snapshots/latest.json`;
  }

  private segmentKey(segmentId: string): string {
    return `${this.prefix}segments/${segmentId}.jsonl`;
  }

  private gcReportKey(executionId: string): string {
    return `${this.prefix}gc-reports/${executionId}.json`;
  }

  private async readObjectIfExists(key: string): Promise<string | null> {
    try {
      const result = await this.client.getObject({
        Bucket: this.bucket,
        Key: key
      });

      return result.Body;
    } catch (error) {
      const code = (error as { code?: string }).code;
      if (code === "NoSuchKey" || code === "NotFound") {
        return null;
      }
      throw error;
    }
  }
}

export class S3DurableRuntimeStore implements DurableRuntimeStore {
  constructor(private readonly adapter: S3PersistenceAdapter) {}

  async loadManifest(): Promise<SegmentManifest | null> {
    return this.adapter.loadManifest();
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    await this.adapter.saveManifest(manifest);
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    await this.adapter.saveLatestSnapshot(snapshot);
  }

  async appendToSegment(segmentId: string, operations: RuntimeOperation[]): Promise<void> {
    await this.adapter.appendSegmentOperations(segmentId, operations);
  }

  async readSegment(segmentId: string): Promise<RuntimeOperation[]> {
    return this.adapter.readSegmentOperations(segmentId);
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    return this.adapter.loadLatestSnapshot();
  }

  async listSegments(): Promise<string[]> {
    return this.adapter.listSegments();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    await this.adapter.deleteSegment(segmentId);
  }

  async loadSegmentGcReport(executionId: string): Promise<StoredSegmentGcReport | null> {
    return this.adapter.loadSegmentGcReport(executionId);
  }

  async saveSegmentGcReport(report: StoredSegmentGcReport): Promise<void> {
    await this.adapter.saveSegmentGcReport(report);
  }
}

function normalizePrefix(prefix: string): string {
  if (!prefix) {
    return "";
  }
  return prefix.endsWith("/") ? prefix : `${prefix}/`;
}

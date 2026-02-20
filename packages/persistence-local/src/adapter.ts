import { appendFile, mkdir, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
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

export interface LocalAdapterConfig {
  rootDir: string;
}

export class LocalPersistenceAdapter {
  private readonly rootDir: string;

  constructor(config: LocalAdapterConfig) {
    this.rootDir = config.rootDir;
  }

  async appendSegment(segmentId: string, content: string): Promise<void> {
    await this.ensureLayout();
    await appendFile(this.segmentPath(segmentId), content, "utf8");
  }

  async readSegment(segmentId: string): Promise<string | null> {
    await this.ensureLayout();
    return this.readFileIfExists(this.segmentPath(segmentId));
  }

  async saveManifest(manifest: SegmentManifest): Promise<void> {
    await this.ensureLayout();
    await writeFile(this.manifestPath(), encodeManifestDocument(manifest), "utf8");
  }

  async loadManifest(): Promise<SegmentManifest | null> {
    await this.ensureLayout();
    const raw = await this.readFileIfExists(this.manifestPath());
    if (!raw) {
      return null;
    }
    return decodeManifestDocument(raw);
  }

  async saveLatestSnapshot(snapshot: RecoverySnapshot): Promise<void> {
    await this.ensureLayout();
    await writeFile(this.snapshotPath(), encodeSnapshotDocument(snapshot), "utf8");
  }

  async loadLatestSnapshot(): Promise<RecoverySnapshot | null> {
    await this.ensureLayout();
    const raw = await this.readFileIfExists(this.snapshotPath());
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
    await this.ensureLayout();
    const files = await readdir(this.segmentsDir());
    return files
      .filter(file => file.endsWith(".jsonl"))
      .map(file => file.slice(0, -".jsonl".length))
      .sort();
  }

  async deleteSegment(segmentId: string): Promise<void> {
    await this.ensureLayout();
    try {
      await unlink(this.segmentPath(segmentId));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return;
      }
      throw error;
    }
  }

  async saveSegmentGcReport(report: StoredSegmentGcReport): Promise<void> {
    await this.ensureLayout();
    await writeFile(this.gcReportPath(report.executionId), JSON.stringify(report), "utf8");
  }

  async loadSegmentGcReport(executionId: string): Promise<StoredSegmentGcReport | null> {
    await this.ensureLayout();
    const raw = await this.readFileIfExists(this.gcReportPath(executionId));
    if (!raw) {
      return null;
    }
    return JSON.parse(raw) as StoredSegmentGcReport;
  }

  private async ensureLayout(): Promise<void> {
    await mkdir(this.segmentsDir(), { recursive: true });
    await mkdir(this.snapshotsDir(), { recursive: true });
    await mkdir(this.gcReportsDir(), { recursive: true });
  }

  private segmentsDir(): string {
    return path.join(this.rootDir, "segments");
  }

  private snapshotsDir(): string {
    return path.join(this.rootDir, "snapshots");
  }

  private segmentPath(segmentId: string): string {
    return path.join(this.segmentsDir(), `${segmentId}.jsonl`);
  }

  private manifestPath(): string {
    return path.join(this.rootDir, "manifest.json");
  }

  private snapshotPath(): string {
    return path.join(this.snapshotsDir(), "latest.json");
  }

  private gcReportsDir(): string {
    return path.join(this.rootDir, "gc-reports");
  }

  private gcReportPath(executionId: string): string {
    return path.join(this.gcReportsDir(), `${executionId}.json`);
  }

  private async readFileIfExists(filePath: string): Promise<string | null> {
    try {
      return await readFile(filePath, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return null;
      }
      throw error;
    }
  }
}

export class LocalDurableRuntimeStore implements DurableRuntimeStore {
  constructor(private readonly adapter: LocalPersistenceAdapter) {}

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

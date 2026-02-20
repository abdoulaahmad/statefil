import { InvalidDocumentError } from "../errors";
import type { SegmentManifest } from "../log";
import type { RecoverySnapshot } from "../log/recovery";
import type { RuntimeOperation } from "../runtime/durable-eventual-runtime";
import { MigrationRegistry, VersionedJsonCodec } from "./versioned-json-codec";

export const MANIFEST_DOCUMENT_SCHEMA = "statefabric.runtime.manifest";
export const SNAPSHOT_DOCUMENT_SCHEMA = "statefabric.runtime.snapshot";
export const OPERATION_DOCUMENT_SCHEMA = "statefabric.runtime.operation";

export const MANIFEST_DOCUMENT_VERSION = "1.1";
export const SNAPSHOT_DOCUMENT_VERSION = "1.1";
export const OPERATION_DOCUMENT_VERSION = "1.1";

const LEGACY_MIGRATION_ENCODED_AT = "1970-01-01T00:00:00.000Z";

interface DocumentMetadata {
  encodedAt: string;
  migratedFrom?: string;
}

interface WrappedDocumentData<TRecord> {
  record: TRecord;
  metadata: DocumentMetadata;
}

type ManifestCodecData = SegmentManifest | WrappedDocumentData<SegmentManifest>;
type SnapshotCodecData = RecoverySnapshot | WrappedDocumentData<RecoverySnapshot>;
type OperationCodecData = RuntimeOperation | WrappedDocumentData<RuntimeOperation>;

export interface RuntimeStoreCodecs {
  manifest: VersionedJsonCodec<ManifestCodecData>;
  snapshot: VersionedJsonCodec<SnapshotCodecData>;
  operation: VersionedJsonCodec<OperationCodecData>;
}

export interface RuntimeStoreCodecOptions {
  manifestMigrations?: MigrationRegistry<ManifestCodecData>;
  snapshotMigrations?: MigrationRegistry<SnapshotCodecData>;
  operationMigrations?: MigrationRegistry<OperationCodecData>;
}

export function createRuntimeStoreCodecs(options: RuntimeStoreCodecOptions = {}): RuntimeStoreCodecs {
  return {
    manifest: new VersionedJsonCodec<ManifestCodecData>({
      schema: MANIFEST_DOCUMENT_SCHEMA,
      currentVersion: MANIFEST_DOCUMENT_VERSION,
      migrations: options.manifestMigrations ?? createManifestMigrations()
    }),
    snapshot: new VersionedJsonCodec<SnapshotCodecData>({
      schema: SNAPSHOT_DOCUMENT_SCHEMA,
      currentVersion: SNAPSHOT_DOCUMENT_VERSION,
      migrations: options.snapshotMigrations ?? createSnapshotMigrations()
    }),
    operation: new VersionedJsonCodec<OperationCodecData>({
      schema: OPERATION_DOCUMENT_SCHEMA,
      currentVersion: OPERATION_DOCUMENT_VERSION,
      migrations: options.operationMigrations ?? createOperationMigrations()
    })
  };
}

const defaultCodecs = createRuntimeStoreCodecs();

export function encodeManifestDocument(manifest: SegmentManifest): string {
  return defaultCodecs.manifest.encode(wrapRecord(manifest));
}

export function decodeManifestDocument(raw: string): SegmentManifest {
  const decoded = decodeWithLegacyFallback(
    raw,
    defaultCodecs.manifest,
    isManifestCodecData,
    "manifest"
  );
  return unwrapRecord(decoded, isSegmentManifest, "manifest");
}

export function encodeSnapshotDocument(snapshot: RecoverySnapshot): string {
  return defaultCodecs.snapshot.encode(wrapRecord(snapshot));
}

export function decodeSnapshotDocument(raw: string): RecoverySnapshot {
  const decoded = decodeWithLegacyFallback(
    raw,
    defaultCodecs.snapshot,
    isSnapshotCodecData,
    "snapshot"
  );
  return unwrapRecord(decoded, isRecoverySnapshot, "snapshot");
}

export function encodeOperationDocument(operation: RuntimeOperation): string {
  return defaultCodecs.operation.encode(wrapRecord(operation));
}

export function decodeOperationDocument(raw: string): RuntimeOperation {
  const decoded = decodeWithLegacyFallback(
    raw,
    defaultCodecs.operation,
    isOperationCodecData,
    "operation"
  );
  return unwrapRecord(decoded, isRuntimeOperation, "operation");
}

function createManifestMigrations(): MigrationRegistry<ManifestCodecData> {
  const migrations = new MigrationRegistry<ManifestCodecData>();
  migrations.register({
    from: "1.0",
    to: "1.1",
    migrate: data => {
      if (isSegmentManifest(data)) {
        return wrapRecord(data, LEGACY_MIGRATION_ENCODED_AT, "1.0");
      }
      if (isWrappedDocumentData(data, isSegmentManifest)) {
        return data;
      }
      throw new InvalidDocumentError("Invalid manifest payload for 1.0 -> 1.1 migration");
    }
  });
  return migrations;
}

function createSnapshotMigrations(): MigrationRegistry<SnapshotCodecData> {
  const migrations = new MigrationRegistry<SnapshotCodecData>();
  migrations.register({
    from: "1.0",
    to: "1.1",
    migrate: data => {
      if (isRecoverySnapshot(data)) {
        return wrapRecord(data, LEGACY_MIGRATION_ENCODED_AT, "1.0");
      }
      if (isWrappedDocumentData(data, isRecoverySnapshot)) {
        return data;
      }
      throw new InvalidDocumentError("Invalid snapshot payload for 1.0 -> 1.1 migration");
    }
  });
  return migrations;
}

function createOperationMigrations(): MigrationRegistry<OperationCodecData> {
  const migrations = new MigrationRegistry<OperationCodecData>();
  migrations.register({
    from: "1.0",
    to: "1.1",
    migrate: data => {
      if (isRuntimeOperation(data)) {
        return wrapRecord(data, LEGACY_MIGRATION_ENCODED_AT, "1.0");
      }
      if (isWrappedDocumentData(data, isRuntimeOperation)) {
        return data;
      }
      throw new InvalidDocumentError("Invalid operation payload for 1.0 -> 1.1 migration");
    }
  });
  return migrations;
}

function decodeWithLegacyFallback<T>(
  raw: string,
  codec: VersionedJsonCodec<T>,
  isLegacy: (value: unknown) => value is T,
  label: string
): T {
  try {
    return codec.decode(raw);
  } catch (error) {
    if (!(error instanceof InvalidDocumentError)) {
      throw error;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw error;
    }

    if (isLegacy(parsed)) {
      return parsed;
    }

    throw new InvalidDocumentError(`Invalid ${label} document`);
  }
}

function wrapRecord<TRecord>(
  record: TRecord,
  encodedAt = new Date().toISOString(),
  migratedFrom?: string
): WrappedDocumentData<TRecord> {
  return {
    record,
    metadata: {
      encodedAt,
      ...(migratedFrom ? { migratedFrom } : {})
    }
  };
}

function unwrapRecord<TRecord>(
  data: TRecord | WrappedDocumentData<TRecord>,
  isRecord: (value: unknown) => value is TRecord,
  label: string
): TRecord {
  if (isRecord(data)) {
    return data;
  }
  if (isWrappedDocumentData(data, isRecord)) {
    return data.record;
  }
  throw new InvalidDocumentError(`Invalid ${label} document payload`);
}

function isManifestCodecData(value: unknown): value is ManifestCodecData {
  return isSegmentManifest(value) || isWrappedDocumentData(value, isSegmentManifest);
}

function isSnapshotCodecData(value: unknown): value is SnapshotCodecData {
  return isRecoverySnapshot(value) || isWrappedDocumentData(value, isRecoverySnapshot);
}

function isOperationCodecData(value: unknown): value is OperationCodecData {
  return isRuntimeOperation(value) || isWrappedDocumentData(value, isRuntimeOperation);
}

function isWrappedDocumentData<TRecord>(
  value: unknown,
  isRecord: (record: unknown) => record is TRecord
): value is WrappedDocumentData<TRecord> {
  if (!value || typeof value !== "object") {
    return false;
  }

  const record = value as Record<string, unknown>;
  const metadata = record.metadata;
  return (
    isRecord(record.record) &&
    !!metadata &&
    typeof metadata === "object" &&
    typeof (metadata as { encodedAt?: unknown }).encodedAt === "string"
  );
}

function isSegmentManifest(value: unknown): value is SegmentManifest {
  if (!value || typeof value !== "object") {
    return false;
  }

  const record = value as Record<string, unknown>;
  const nodeSeq = record.lastCommittedSequenceByNode;
  const committed = record.committedSegments;

  return (
    record.version === "1.0" &&
    typeof record.activeSegmentId === "string" &&
    Array.isArray(committed) &&
    committed.every(item => typeof item === "string") &&
    !!nodeSeq &&
    typeof nodeSeq === "object" &&
    Object.values(nodeSeq).every(item => typeof item === "number") &&
    typeof record.updatedAt === "string"
  );
}

function isRecoverySnapshot(value: unknown): value is RecoverySnapshot {
  if (!value || typeof value !== "object") {
    return false;
  }

  const record = value as Record<string, unknown>;
  const checkpoint = record.checkpoint;

  return (
    typeof record.snapshotId === "string" &&
    !!checkpoint &&
    typeof checkpoint === "object" &&
    typeof (checkpoint as { lastSegmentId?: unknown }).lastSegmentId === "string"
  );
}

function isRuntimeOperation(value: unknown): value is RuntimeOperation {
  if (!value || typeof value !== "object") {
    return false;
  }

  const record = value as Record<string, unknown>;
  const payload = record.payload;
  const hlc = record.hlc;

  const hasValidType =
    record.type === "register.set" || record.type === "counter.increment";

  return (
    typeof record.opId === "string" &&
    typeof record.namespace === "string" &&
    typeof record.key === "string" &&
    hasValidType &&
    !!payload &&
    typeof payload === "object" &&
    typeof record.actorNodeId === "string" &&
    typeof record.sequence === "number" &&
    !!hlc &&
    typeof hlc === "object" &&
    typeof (hlc as Record<string, unknown>).nodeId === "string" &&
    typeof (hlc as Record<string, unknown>).physical === "number" &&
    typeof (hlc as Record<string, unknown>).logical === "number"
  );
}

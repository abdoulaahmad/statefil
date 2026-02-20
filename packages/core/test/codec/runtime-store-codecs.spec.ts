import { describe, expect, it } from "vitest";
import { UnsupportedVersionError } from "../../src/errors";
import {
  MANIFEST_DOCUMENT_VERSION,
  OPERATION_DOCUMENT_VERSION,
  MANIFEST_DOCUMENT_SCHEMA,
  OPERATION_DOCUMENT_SCHEMA,
  SNAPSHOT_DOCUMENT_SCHEMA,
  SNAPSHOT_DOCUMENT_VERSION,
  decodeManifestDocument,
  decodeOperationDocument,
  decodeSnapshotDocument,
  encodeManifestDocument,
  encodeOperationDocument,
  encodeSnapshotDocument
} from "../../src/codec/runtime-store-codecs";

describe("runtime store codecs", () => {
  it("encodes documents with 1.1 envelope version", () => {
    const manifestRaw = encodeManifestDocument({
      version: "1.0",
      activeSegmentId: "seg-1",
      committedSegments: [],
      lastCommittedSequenceByNode: {},
      updatedAt: "2026-02-20T00:00:00.000Z"
    });
    const snapshotRaw = encodeSnapshotDocument({
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-1" }
    });
    const operationRaw = encodeOperationDocument({
      opId: "op-1",
      namespace: "session",
      key: "u1",
      type: "register.set",
      payload: { value: "alice" },
      actorNodeId: "node-a",
      sequence: 1,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    });

    const manifestEnvelope = JSON.parse(manifestRaw) as { version: string };
    const snapshotEnvelope = JSON.parse(snapshotRaw) as { version: string };
    const operationEnvelope = JSON.parse(operationRaw) as { version: string };

    expect(manifestEnvelope.version).toBe(MANIFEST_DOCUMENT_VERSION);
    expect(snapshotEnvelope.version).toBe(SNAPSHOT_DOCUMENT_VERSION);
    expect(operationEnvelope.version).toBe(OPERATION_DOCUMENT_VERSION);
  });

  it("migrates 1.0 enveloped manifest/snapshot/operation payloads", () => {
    const manifestV10Envelope = JSON.stringify({
      schema: MANIFEST_DOCUMENT_SCHEMA,
      version: "1.0",
      data: {
        version: "1.0",
        activeSegmentId: "seg-3",
        committedSegments: ["seg-1", "seg-2"],
        lastCommittedSequenceByNode: { "node-a": 4 },
        updatedAt: "2026-02-20T00:00:00.000Z"
      }
    });
    const snapshotV10Envelope = JSON.stringify({
      schema: SNAPSHOT_DOCUMENT_SCHEMA,
      version: "1.0",
      data: {
        snapshotId: "snap-1",
        checkpoint: { lastSegmentId: "seg-5" }
      }
    });
    const operationV10Envelope = JSON.stringify({
      schema: OPERATION_DOCUMENT_SCHEMA,
      version: "1.0",
      data: {
        opId: "op-2",
        namespace: "session",
        key: "user-1",
        type: "register.set",
        payload: { value: "alice" },
        actorNodeId: "node-a",
        sequence: 2,
        hlc: { physical: 100, logical: 0, nodeId: "node-a" }
      }
    });

    expect(decodeManifestDocument(manifestV10Envelope)).toEqual({
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: { "node-a": 4 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    });
    expect(decodeSnapshotDocument(snapshotV10Envelope)).toEqual({
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-5" }
    });
    expect(decodeOperationDocument(operationV10Envelope)).toEqual({
      opId: "op-2",
      namespace: "session",
      key: "user-1",
      type: "register.set",
      payload: { value: "alice" },
      actorNodeId: "node-a",
      sequence: 2,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    });
  });

  it("decodes legacy manifest documents without envelope", () => {
    const legacyRaw = JSON.stringify({
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: { "node-a": 4 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    });

    expect(decodeManifestDocument(legacyRaw)).toEqual({
      version: "1.0",
      activeSegmentId: "seg-3",
      committedSegments: ["seg-1", "seg-2"],
      lastCommittedSequenceByNode: { "node-a": 4 },
      updatedAt: "2026-02-20T00:00:00.000Z"
    });
  });

  it("decodes legacy snapshot and operation documents without envelope", () => {
    const snapshotRaw = JSON.stringify({
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-5" }
    });
    const operationRaw = JSON.stringify({
      opId: "op-2",
      namespace: "session",
      key: "user-1",
      type: "register.set",
      payload: { value: "alice" },
      actorNodeId: "node-a",
      sequence: 2,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    });

    expect(decodeSnapshotDocument(snapshotRaw)).toEqual({
      snapshotId: "snap-1",
      checkpoint: { lastSegmentId: "seg-5" }
    });
    expect(decodeOperationDocument(operationRaw)).toEqual({
      opId: "op-2",
      namespace: "session",
      key: "user-1",
      type: "register.set",
      payload: { value: "alice" },
      actorNodeId: "node-a",
      sequence: 2,
      hlc: { physical: 100, logical: 0, nodeId: "node-a" }
    });
  });

  it("rejects unsupported future manifest envelope versions", () => {
    const raw = JSON.stringify({
      schema: MANIFEST_DOCUMENT_SCHEMA,
      version: "9.0",
      data: {
        version: "1.0",
        activeSegmentId: "seg-1",
        committedSegments: [],
        lastCommittedSequenceByNode: {},
        updatedAt: "2026-02-20T00:00:00.000Z"
      }
    });

    expect(() => decodeManifestDocument(raw)).toThrow(UnsupportedVersionError);
  });
});

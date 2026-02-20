import { describe, expect, it } from "vitest";
import { InvalidDocumentError, UnsupportedVersionError } from "../../src/errors";
import { MigrationRegistry, VersionedJsonCodec } from "../../src/codec/versioned-json-codec";

interface ShapeV2 {
  count: number;
  label?: string;
}

describe("VersionedJsonCodec", () => {
  it("encodes and decodes current-version envelopes", () => {
    const codec = new VersionedJsonCodec<ShapeV2>({
      schema: "statefabric.test.shape",
      currentVersion: "2.0"
    });

    const encoded = codec.encode({ count: 2, label: "ok" });
    const decoded = codec.decode(encoded);

    expect(decoded).toEqual({ count: 2, label: "ok" });
  });

  it("migrates older envelopes when migration path exists", () => {
    const migrations = new MigrationRegistry<ShapeV2>();
    migrations.register({
      from: "1.0",
      to: "2.0",
      migrate: data => ({ ...data, label: data.label ?? "migrated" })
    });

    const codec = new VersionedJsonCodec<ShapeV2>({
      schema: "statefabric.test.shape",
      currentVersion: "2.0",
      migrations
    });

    const raw = JSON.stringify({
      schema: "statefabric.test.shape",
      version: "1.0",
      data: { count: 9 }
    });

    expect(codec.decode(raw)).toEqual({ count: 9, label: "migrated" });
  });

  it("throws on unsupported version transitions", () => {
    const codec = new VersionedJsonCodec<ShapeV2>({
      schema: "statefabric.test.shape",
      currentVersion: "2.0"
    });

    const raw = JSON.stringify({
      schema: "statefabric.test.shape",
      version: "3.0",
      data: { count: 5 }
    });

    expect(() => codec.decode(raw)).toThrow(UnsupportedVersionError);
  });

  it("throws on schema mismatches", () => {
    const codec = new VersionedJsonCodec<ShapeV2>({
      schema: "statefabric.test.shape",
      currentVersion: "2.0"
    });

    const raw = JSON.stringify({
      schema: "statefabric.other",
      version: "2.0",
      data: { count: 5 }
    });

    expect(() => codec.decode(raw)).toThrow(InvalidDocumentError);
  });
});

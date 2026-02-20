import { definePersistenceAdapterContract } from "../../core/test-support/persistence-adapter-contract";
import { S3PersistenceAdapter, type S3ClientLike } from "../src";

class InMemoryS3Client implements S3ClientLike {
  private readonly objects = new Map<string, string>();

  async putObject(input: { Bucket: string; Key: string; Body: string }): Promise<void> {
    this.objects.set(`${input.Bucket}/${input.Key}`, input.Body);
  }

  async getObject(input: { Bucket: string; Key: string }): Promise<{ Body: string }> {
    const key = `${input.Bucket}/${input.Key}`;
    const body = this.objects.get(key);
    if (typeof body === "undefined") {
      const error = new Error("NoSuchKey");
      (error as Error & { code?: string }).code = "NoSuchKey";
      throw error;
    }
    return { Body: body };
  }

  async deleteObject(input: { Bucket: string; Key: string }): Promise<void> {
    this.objects.delete(`${input.Bucket}/${input.Key}`);
  }

  async listObjects(input: { Bucket: string; Prefix: string }): Promise<{ Keys: string[] }> {
    const bucketPrefix = `${input.Bucket}/`;
    const prefix = `${bucketPrefix}${input.Prefix}`;
    const keys = [...this.objects.keys()]
      .filter(key => key.startsWith(prefix))
      .map(full => full.slice(bucketPrefix.length));
    return { Keys: keys };
  }
}

definePersistenceAdapterContract({
  name: "S3PersistenceAdapter",
  async createAdapter() {
    return {
      adapter: new S3PersistenceAdapter(new InMemoryS3Client(), {
        bucket: "bucket-a",
        prefix: "statefabric-contract"
      })
    };
  },
  appendSegment: (adapter, segmentId, content) => adapter.appendSegment(segmentId, content),
  readSegment: (adapter, segmentId) => adapter.readSegment(segmentId),
  listSegments: adapter => adapter.listSegments(),
  deleteSegment: (adapter, segmentId) => adapter.deleteSegment(segmentId),
  saveManifest: (adapter, manifest) => adapter.saveManifest(manifest),
  loadManifest: adapter => adapter.loadManifest(),
  saveLatestSnapshot: (adapter, snapshot) => adapter.saveLatestSnapshot(snapshot),
  loadLatestSnapshot: adapter => adapter.loadLatestSnapshot(),
  appendSegmentOperations: (adapter, segmentId, operations) =>
    adapter.appendSegmentOperations(segmentId, operations),
  readSegmentOperations: (adapter, segmentId) => adapter.readSegmentOperations(segmentId),
  saveSegmentGcReport: (adapter, report) => adapter.saveSegmentGcReport(report),
  loadSegmentGcReport: (adapter, executionId) => adapter.loadSegmentGcReport(executionId)
});

import { describe, expect, it } from "vitest";
import { ConflictEventStream } from "../../src";

describe("ConflictEventStream", () => {
  it("broadcasts emitted events to subscribers", () => {
    const stream = new ConflictEventStream();
    const seen: string[] = [];

    stream.subscribe(event => {
      seen.push(`${event.type}:${"opId" in event ? event.opId : event.winnerActorId}`);
    });

    stream.emit({ type: "replay.duplicate-operation", opId: "op-1" });
    stream.emit({ type: "lww.tie-break", winnerActorId: "b", loserActorId: "a" });

    expect(seen).toEqual(["replay.duplicate-operation:op-1", "lww.tie-break:b"]);
  });

  it("stops delivering events after unsubscribe", () => {
    const stream = new ConflictEventStream();
    const seen: string[] = [];

    const unsubscribe = stream.subscribe(event => {
      seen.push(event.type);
    });

    stream.emit({ type: "replay.duplicate-operation", opId: "op-1" });
    unsubscribe();
    stream.emit({ type: "replay.duplicate-operation", opId: "op-2" });

    expect(seen).toEqual(["replay.duplicate-operation"]);
  });
});

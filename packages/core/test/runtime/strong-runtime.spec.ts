import { describe, expect, it } from "vitest";
import {
  ConsensusUnavailableError,
  CounterBoundsError,
  InMemoryStrongRuntime,
  RetryableConflictError,
  StaticConsensusAdapter
} from "../../src";

describe("InMemoryStrongRuntime", () => {
  it("fails fast when consensus is unavailable", async () => {
    const runtime = new InMemoryStrongRuntime({ consensusAdapter: new StaticConsensusAdapter(false) });

    await expect(runtime.setRegister("payment", "balance", 10)).rejects.toBeInstanceOf(
      ConsensusUnavailableError
    );
  });

  it("supports compareAndSwap semantics", async () => {
    const runtime = new InMemoryStrongRuntime();

    await runtime.setRegister("payment", "balance", 10);

    const ok = await runtime.compareAndSwapRegister("payment", "balance", 10, 20);
    const fail = await runtime.compareAndSwapRegister("payment", "balance", 10, 30);

    expect(ok).toBe(true);
    expect(fail).toBe(false);
    await expect(runtime.getRegister<number>("payment", "balance")).resolves.toBe(20);
  });

  it("rolls back transaction when callback throws", async () => {
    const runtime = new InMemoryStrongRuntime();

    await runtime.setRegister("acct", "a", 5);

    await expect(
      runtime.transaction(async tx => {
        tx.setRegister("acct", "a", 9);
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");

    await expect(runtime.getRegister<number>("acct", "a")).resolves.toBe(5);
  });

  it("serializes concurrent transactions", async () => {
    const runtime = new InMemoryStrongRuntime();
    await runtime.setRegister("counter", "v", 0);

    await Promise.all(
      Array.from({ length: 50 }, async () => {
        await runtime.transaction(tx => {
          const current = tx.getRegister<number>("counter", "v") ?? 0;
          tx.setRegister("counter", "v", current + 1);
        });
      })
    );

    await expect(runtime.getRegister<number>("counter", "v")).resolves.toBe(50);
  });

  it("enforces non-negative counter bound", async () => {
    const runtime = new InMemoryStrongRuntime();
    await runtime.incrementCounter("inventory", "item-1", 2);

    await expect(runtime.decrementCounter("inventory", "item-1", 1, 0)).resolves.toBe(1);
    await expect(runtime.decrementCounter("inventory", "item-1", 2, 0)).rejects.toBeInstanceOf(
      CounterBoundsError
    );
    await expect(runtime.getCounter("inventory", "item-1")).resolves.toBe(1);
  });

  it("applies multi-key map update atomically in transaction", async () => {
    const runtime = new InMemoryStrongRuntime();

    await runtime.transaction(tx => {
      tx.setMapField("cart", "u1", "itemA", 1);
      tx.setMapField("cart", "u1", "itemB", 3);
      tx.setMapField("cart", "u2", "itemC", 2);
    });

    await expect(runtime.getMapEntry("cart", "u1")).resolves.toEqual({ itemA: 1, itemB: 3 });
    await expect(runtime.getMapEntry("cart", "u2")).resolves.toEqual({ itemC: 2 });
  });

  it("retries transaction on retryable conflict and eventually commits", async () => {
    let attempts = 0;
    const runtime = new InMemoryStrongRuntime({
      maxTransactionRetries: 2,
      onBeforeCommit: attempt => {
        attempts = attempt;
        if (attempt < 3) {
          throw new RetryableConflictError("transient conflict");
        }
      }
    });

    const result = await runtime.transaction(tx => {
      tx.setRegister("job", "state", "done");
      return "ok";
    });

    expect(result).toBe("ok");
    expect(attempts).toBe(3);
    await expect(runtime.getRegister<string>("job", "state")).resolves.toBe("done");
  });

  it("fails when retry budget is exhausted", async () => {
    const runtime = new InMemoryStrongRuntime({
      maxTransactionRetries: 1,
      onBeforeCommit: () => {
        throw new RetryableConflictError("still conflicting");
      }
    });

    await expect(
      runtime.transaction(tx => {
        tx.setRegister("job", "state", "done");
      })
    ).rejects.toBeInstanceOf(RetryableConflictError);
    await expect(runtime.getRegister<string>("job", "state")).resolves.toBeUndefined();
  });

  it("aborts transaction when consensus is lost at commit time", async () => {
    const adapter = new StaticConsensusAdapter(true);
    let flipped = false;

    const runtime = new InMemoryStrongRuntime({
      consensusAdapter: {
        isAvailable: () => adapter.isAvailable(),
        beforeCommit: () => {
          if (!flipped) {
            flipped = true;
            adapter.setAvailable(false);
          }
        }
      }
    });

    await runtime.setRegister("acct", "x", 1);

    await expect(
      runtime.transaction(tx => {
        tx.setRegister("acct", "x", 2);
      })
    ).rejects.toBeInstanceOf(ConsensusUnavailableError);

    adapter.setAvailable(true);
    await expect(runtime.getRegister<number>("acct", "x")).resolves.toBe(1);
  });
});

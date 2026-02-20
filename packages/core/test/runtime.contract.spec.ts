import { describe, expect, it } from "vitest";
import { createScaffoldRuntime, NotImplementedError } from "../src";

describe("runtime scaffold contract", () => {
  it("exposes eventual and strong APIs", () => {
    const runtime = createScaffoldRuntime();

    expect(runtime.eventual).toBeDefined();
    expect(runtime.strong).toBeDefined();
  });

  it("eventual API does not expose transaction", () => {
    const runtime = createScaffoldRuntime();

    expect("transaction" in runtime.eventual).toBe(false);
  });

  it("eventual register does not expose compareAndSwap", () => {
    const runtime = createScaffoldRuntime();
    const register = runtime.eventual.register<string>("session", "memory");

    expect("compareAndSwap" in register).toBe(false);
  });

  it("eventual.batch executes callback result (expected behavior, currently failing)", async () => {
    const runtime = createScaffoldRuntime();

    await expect(runtime.eventual.batch(async () => "ok")).resolves.toBe("ok");
  });

  it("scaffold methods throw NotImplementedError until implemented", async () => {
    const runtime = createScaffoldRuntime();

    await expect(runtime.start()).rejects.toBeInstanceOf(NotImplementedError);
  });
});

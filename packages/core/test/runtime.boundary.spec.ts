import { describe, expect, it } from "vitest";
import { createScaffoldRuntime } from "../src";

describe("runtime API consistency boundaries", () => {
  it("eventual API exposes batch but not transaction", () => {
    const runtime = createScaffoldRuntime();

    expect(typeof runtime.eventual.batch).toBe("function");
    expect("transaction" in runtime.eventual).toBe(false);
  });

  it("strong API exposes transaction", () => {
    const runtime = createScaffoldRuntime();

    expect(typeof runtime.strong.transaction).toBe("function");
  });

  it("strong register exposes compareAndSwap", () => {
    const runtime = createScaffoldRuntime();
    const register = runtime.strong.register<string>("payment", "balance");

    expect(typeof register.compareAndSwap).toBe("function");
  });
});

import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { appAtomRegistry } from "../rpc/atomRegistry";
import { resolveThreadDetailRef, waitForThreadShell } from "./entities";

const threadRef = scopeThreadRef(EnvironmentId.make("environment-1"), ThreadId.make("thread-1"));

describe("waitForThreadShell", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("resolves immediately for an existing shell", async () => {
    vi.spyOn(appAtomRegistry, "get").mockReturnValue({ id: threadRef.threadId });
    const subscribe = vi.spyOn(appAtomRegistry, "subscribe");
    await waitForThreadShell(threadRef);
    expect(subscribe).not.toHaveBeenCalled();
  });

  it("rejects and unsubscribes when the destination never arrives", async () => {
    vi.useFakeTimers();
    vi.spyOn(appAtomRegistry, "get").mockReturnValue(null);
    const unsubscribe = vi.fn();
    vi.spyOn(appAtomRegistry, "subscribe").mockReturnValue(unsubscribe);
    const result = expect(waitForThreadShell(threadRef, 50)).rejects.toThrow(
      "The thread did not appear in the desktop app.",
    );
    await vi.advanceTimersByTimeAsync(50);
    await result;
    expect(unsubscribe).toHaveBeenCalledOnce();
  });

  it("resolves on shell arrival and clears the timeout and subscription", async () => {
    vi.useFakeTimers();
    vi.spyOn(appAtomRegistry, "get").mockReturnValue(null);
    const unsubscribe = vi.fn();
    let notify!: (shell: unknown) => void;
    vi.spyOn(appAtomRegistry, "subscribe").mockImplementation((_atom, callback) => {
      notify = callback;
      return unsubscribe;
    });
    const result = waitForThreadShell(threadRef, 50);
    notify({ id: threadRef.threadId });
    await result;
    expect(unsubscribe).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rechecks for a shell that arrived while subscribing", async () => {
    vi.useFakeTimers();
    vi.spyOn(appAtomRegistry, "get")
      .mockReturnValueOnce(null)
      .mockReturnValue({ id: threadRef.threadId });
    const unsubscribe = vi.fn();
    vi.spyOn(appAtomRegistry, "subscribe").mockReturnValue(unsubscribe);
    const result = expect(waitForThreadShell(threadRef, 50)).resolves.toBeUndefined();
    await vi.advanceTimersByTimeAsync(50);
    await result;
    expect(unsubscribe).toHaveBeenCalledOnce();
  });
});

describe("resolveThreadDetailRef", () => {
  it("does not subscribe to a reserved draft thread before it enters the shell index", () => {
    expect(
      resolveThreadDetailRef(threadRef, {
        shellExists: false,
        waitForShell: true,
      }),
    ).toBeNull();
  });

  it("subscribes once the reserved draft thread enters the shell index", () => {
    expect(
      resolveThreadDetailRef(threadRef, {
        shellExists: true,
        waitForShell: true,
      }),
    ).toBe(threadRef);
  });

  it("keeps direct server-thread lookups enabled when the shell has not loaded it", () => {
    expect(
      resolveThreadDetailRef(threadRef, {
        shellExists: false,
        waitForShell: false,
      }),
    ).toBe(threadRef);
  });
});

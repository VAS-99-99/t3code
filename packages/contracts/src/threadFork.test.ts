import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import { ThreadForkError, ThreadForkInput, WS_METHODS, WsRpcGroup } from "./index.ts";

const decodeInput = Schema.decodeUnknownSync(ThreadForkInput);

describe("ThreadForkInput", () => {
  const base = { sourceThreadId: "source", destinationThreadId: "fork", commandId: "command-1" };

  it("takes caller-allocated ids and an optional title", () => {
    expect(decodeInput(base)).toEqual(base);
    expect(decodeInput({ ...base, title: "Try the other approach" }).title).toBe(
      "Try the other approach",
    );
  });

  it("rejects missing ids and blank titles", () => {
    expect(() => decodeInput({ sourceThreadId: "source" })).toThrow();
    expect(() => decodeInput({ ...base, commandId: undefined })).toThrow();
    expect(() => decodeInput({ ...base, title: "  " })).toThrow();
  });
});

describe("threads.fork RPC", () => {
  it("is part of the server RPC group and its failures survive the wire", () => {
    expect(WS_METHODS.threadsFork).toBe("threads.fork");
    expect(WsRpcGroup.requests.has(WS_METHODS.threadsFork)).toBe(true);
    const error = new ThreadForkError({
      reason: "unsupported-provider",
      sourceThreadId: "source" as never,
      destinationThreadId: "fork" as never,
      detail: "Provider 'cursor' cannot fork conversations.",
    });
    const wire = Schema.encodeSync(ThreadForkError)(error);
    expect(Schema.decodeUnknownSync(ThreadForkError)(wire)).toEqual(error);
    expect(error.message).toContain("cannot fork conversations");
  });
});

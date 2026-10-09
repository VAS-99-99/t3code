import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  EnvironmentId,
  ThreadForkError,
  ThreadId,
  type ThreadForkInput,
  type ThreadForkResult,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  createThreadForkIntents,
  forkThread,
  resolveThreadForkMenuState,
} from "./threadFork.logic";

const remoteEnv = EnvironmentId.make("env-remote");
const source = scopeThreadRef(remoteEnv, ThreadId.make("source"));

function allocator() {
  let n = 0;
  return () => {
    n += 1;
    return {
      commandId: CommandId.make(`command-${n}`),
      destinationThreadId: ThreadId.make(`destination-${n}`),
    };
  };
}

type Result = AtomCommandResult<ThreadForkResult, unknown>;
const ok = (destinationThreadId: string): Result =>
  AsyncResult.success({ destinationThreadId: ThreadId.make(destinationThreadId) });
const failed = (error: unknown): Result => AsyncResult.failure(Cause.fail(error));
const forkError = (reason: ThreadForkError["reason"], detail = "nope") =>
  new ThreadForkError({
    reason,
    sourceThreadId: source.threadId,
    destinationThreadId: ThreadId.make("whatever"),
    detail,
  });

describe("createThreadForkIntents", () => {
  it("reuses the same ids for every retry after a failure, then issues new ones after success", async () => {
    const intents = createThreadForkIntents(allocator());
    const seen: ThreadForkInput[] = [];
    const respond = (results: Result[]) => async (request: { input: ThreadForkInput }) => {
      seen.push(request.input);
      return results.shift()!;
    };
    const send = respond([
      failed(new Error("connection lost")),
      failed(forkError("provider-failed")),
      ok("destination-1"),
      ok("destination-2"),
    ]);

    await intents.run(source, send);
    await intents.run(source, send);
    await intents.run(source, send);
    await intents.run(source, send);

    expect(seen.map((input) => [input.commandId, input.destinationThreadId])).toEqual([
      ["command-1", "destination-1"],
      ["command-1", "destination-1"],
      ["command-1", "destination-1"],
      ["command-2", "destination-2"],
    ]);
  });

  it("allocates fresh ids after the server reports the destination is taken", async () => {
    const intents = createThreadForkIntents(allocator());
    const seen: string[] = [];
    const results = [failed(forkError("destination-conflict")), ok("destination-2")];
    const send = async (request: { input: ThreadForkInput }) => {
      seen.push(request.input.destinationThreadId);
      return results.shift()!;
    };

    await intents.run(source, send);
    await intents.run(source, send);

    expect(seen).toEqual(["destination-1", "destination-2"]);
  });

  it("ignores a click while a request for the same source is in flight", async () => {
    const intents = createThreadForkIntents(allocator());
    let release!: (result: Result) => void;
    const send = vi.fn(
      () =>
        new Promise<Result>((resolve) => {
          release = resolve;
        }),
    );

    const first = intents.run(source, send);
    const duplicate = await intents.run(source, send);
    release(ok("destination-1"));

    expect(duplicate).toBeNull();
    expect(await first).toMatchObject({ _tag: "Success" });
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("sends to the source thread's environment", async () => {
    const send = vi.fn(async (_request: { environmentId: EnvironmentId; input: ThreadForkInput }) =>
      ok("destination-1"),
    );

    await createThreadForkIntents(allocator()).run(source, send);

    expect(send.mock.calls[0]![0]).toMatchObject({
      environmentId: remoteEnv,
      input: { sourceThreadId: "source", destinationThreadId: "destination-1" },
    });
  });
});

describe("forkThread", () => {
  function setup(result: Result) {
    const order: string[] = [];
    const deps = {
      intents: createThreadForkIntents(allocator()),
      send: vi.fn(async () => result),
      waitForThreadShell: vi.fn(async () => void order.push("wait")),
      navigate: vi.fn(async () => void order.push("navigate")),
      reportFailure: vi.fn(),
    };
    return { deps, order };
  }

  it("opens the destination in the source environment once its shell exists", async () => {
    const { deps, order } = setup(ok("destination-9"));

    await forkThread(deps, source);

    const destination = scopeThreadRef(remoteEnv, ThreadId.make("destination-9"));
    expect(deps.navigate).toHaveBeenCalledWith(destination);
    expect(deps.waitForThreadShell).toHaveBeenCalledWith(destination);
    expect(order).toEqual(["wait", "navigate"]);
    expect(deps.reportFailure).not.toHaveBeenCalled();
  });

  it("shows the server's reason and stays put when the fork fails", async () => {
    const { deps } = setup(failed(forkError("source-not-forkable", "the thread is running.")));

    await forkThread(deps, source);

    expect(deps.reportFailure).toHaveBeenCalledWith(
      "Failed to fork thread",
      "the thread is running.",
    );
    expect(deps.navigate).not.toHaveBeenCalled();
  });

  it("reports a failed navigation after a successful fork", async () => {
    const { deps } = setup(ok("destination-9"));
    deps.navigate.mockRejectedValueOnce(new Error("route unavailable"));

    await forkThread(deps, source);

    expect(deps.reportFailure).toHaveBeenCalledWith(
      "Thread forked, but navigation failed",
      "route unavailable",
    );
  });

  it("stays on the source when the destination shell does not arrive", async () => {
    const { deps } = setup(ok("destination-9"));
    deps.waitForThreadShell.mockRejectedValueOnce(new Error("Thread did not appear."));

    await forkThread(deps, source);

    expect(deps.navigate).not.toHaveBeenCalled();
    expect(deps.reportFailure).toHaveBeenCalledWith(
      "Thread forked, but navigation failed",
      "Thread did not appear.",
    );
  });
});

describe("resolveThreadForkMenuState", () => {
  const completedTurn = {
    turnId: "turn-1",
    state: "completed",
    requestedAt: "2026-10-09T10:00:00Z",
    startedAt: "2026-10-09T10:00:01Z",
    completedAt: "2026-10-09T10:00:09Z",
    assistantMessageId: null,
  } as const;
  const thread = (overrides: Record<string, unknown> = {}) =>
    ({
      modelSelection: { instanceId: "claude", model: "m" },
      session: { status: "ready", activeTurnId: null },
      latestTurn: completedTurn,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      ...overrides,
    }) as never;
  const claude = { instanceId: "claude", supportsConversationFork: true } as never;

  it("hides Fork when the server or provider cannot fork", () => {
    expect(resolveThreadForkMenuState({ thread: thread(), providers: [] })).toBeUndefined();
    expect(
      resolveThreadForkMenuState({
        thread: thread(),
        providers: [{ instanceId: "claude" } as never],
      }),
    ).toBeUndefined();
    expect(
      resolveThreadForkMenuState({
        thread: thread({ modelSelection: { instanceId: "cursor", model: "m" } }),
        providers: [claude],
      }),
    ).toBeUndefined();
  });

  it("enables Fork for a settled thread whose last turn completed", () => {
    expect(resolveThreadForkMenuState({ thread: thread(), providers: [claude] })).toEqual({
      disabled: false,
    });
  });

  it.each([
    ["last turn failed", { latestTurn: { ...completedTurn, state: "error" } }],
    ["last turn interrupted", { latestTurn: { ...completedTurn, state: "interrupted" } }],
    ["running", { session: { status: "running", activeTurnId: "turn-2" } }],
    ["starting", { session: { status: "starting", activeTurnId: null } }],
    ["waiting on approval", { hasPendingApprovals: true }],
    ["waiting on input", { hasPendingUserInput: true }],
  ])("disables Fork when %s", (_name, overrides) => {
    expect(resolveThreadForkMenuState({ thread: thread(overrides), providers: [claude] })).toEqual({
      disabled: true,
    });
  });

  it("allows imported history and fork-of-fork history without a new turn", () => {
    expect(
      resolveThreadForkMenuState({ thread: thread({ latestTurn: null }), providers: [claude] }),
    ).toEqual({ disabled: false });
  });
});

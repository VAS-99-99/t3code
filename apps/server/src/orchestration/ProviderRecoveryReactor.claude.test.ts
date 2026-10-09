/**
 * The recovery journey through the real ClaudeAdapter: SDK messages in, the
 * adapter's own runtime events through ProviderService, ingestion, SQLite and
 * the reactor, and the continuation back out as the next SDK prompt.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { it } from "@effect/vitest";
import { ClaudeSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import { SYNTHETIC_CLAUDE_MODEL_CATALOG } from "../provider/ClaudeModelCatalog.testFixtures.ts";
import { makeClaudeAdapter } from "../provider/Layers/ClaudeAdapter.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  A,
  B,
  dropWorld,
  HOUR,
  iso,
  makeWorld,
  startServer,
  T0,
  type Quota,
} from "./providerRecoveryHarness.testUtils.ts";

const decodeClaudeSettings = Schema.decodeSync(ClaudeSettings);

/** The least of the SDK's Query the adapter drives: a message queue and inert controls. */
class ScriptedQuery implements AsyncIterable<SDKMessage> {
  private readonly queue: Array<SDKMessage> = [];
  private readonly waiters: Array<{
    readonly resolve: (result: IteratorResult<SDKMessage>) => void;
    readonly reject: (error: unknown) => void;
  }> = [];
  private failure: unknown;
  private done = false;
  readonly setModel = async () => {};
  readonly setPermissionMode = async () => {};
  readonly setMaxThinkingTokens = async () => {};
  readonly close = () => {
    this.done = true;
    for (const waiter of this.waiters.splice(0)) waiter.resolve({ done: true, value: undefined });
  };
  emit(message: SDKMessage): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ done: false, value: message });
    else this.queue.push(message);
  }
  /** The async iterator itself rejects, as the SDK's does when its connection drops. */
  fail(error: unknown): void {
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) waiter.reject(error);
  }
  [Symbol.asyncIterator](): AsyncIterator<SDKMessage> {
    return {
      next: () => {
        const value = this.queue.shift();
        if (value) return Promise.resolve({ done: false, value });
        if (this.failure !== undefined) return Promise.reject(this.failure);
        if (this.done) return Promise.resolve({ done: true, value: undefined });
        return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
      },
    };
  }
}

const promptText = (message: SDKUserMessage): string => {
  const content = message.message.content;
  return typeof content === "string"
    ? content
    : content.map((part) => (part.type === "text" ? part.text : "")).join("");
};

const failedResult = (errors: ReadonlyArray<string>, uuid: string) =>
  ({
    type: "result",
    subtype: errors.length > 0 ? "error_during_execution" : "success",
    is_error: true,
    errors,
    session_id: "sdk-session-1",
    uuid,
    duration_ms: 1,
    duration_api_ms: 1,
    num_turns: 1,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
    permission_denials: [],
  }) as unknown as SDKMessage;

const rejectedWindow = (resetsAtMs: number, uuid: string) =>
  ({
    type: "rate_limit_event",
    rate_limit_info: {
      status: "rejected",
      rateLimitType: "five_hour",
      utilization: 1,
      resetsAt: Math.floor(resetsAtMs / 1000),
    },
    session_id: "sdk-session-1",
    uuid,
  }) as unknown as SDKMessage;

/** Claude's first message of a session, the one that names the native session to resume. */
const sessionInit = () =>
  ({
    type: "system",
    subtype: "init",
    session_id: "sdk-session-1",
    uuid: "init-1",
    apiKeySource: "none",
    cwd: "/tmp/claude-recovery-test",
    tools: [],
    mcp_servers: [],
    model: "claude-sonnet-5-5",
    permissionMode: "default",
    slash_commands: [],
    output_style: "default",
  }) as unknown as SDKMessage;

const exhausted = (resetsAt: number): Quota => ({
  read: "fresh",
  windows: [{ id: "five_hour", usedPercent: 100, resetsAt }],
});

/** What the SDK's persisted session history holds; `unreadable` makes the read throw. */
type NativeHistory = { messages: ReadonlyArray<unknown> | "unreadable" };

const humanPrompt = (uuid: string) => ({
  type: "user",
  uuid,
  parent_tool_use_id: null,
  message: { role: "user", content: "original prompt" },
});
const toolUse = (id: string) => ({
  type: "assistant",
  uuid: `use-${id}`,
  parent_tool_use_id: null,
  message: { role: "assistant", content: [{ type: "tool_use", id, name: "Bash", input: {} }] },
});
const toolResult = (id: string) => ({
  type: "user",
  uuid: `result-${id}`,
  parent_tool_use_id: null,
  message: { role: "user", content: [{ type: "tool_result", tool_use_id: id, content: "ok" }] },
});

/** Starts the server with account A running the real adapter on a scripted SDK. */
const startClaudeServer = Effect.gen(function* () {
  const world = makeWorld();
  const history: NativeHistory = { messages: [humanPrompt("human-1")] };
  const queries: Array<ScriptedQuery> = [];
  const prompts: Array<string> = [];
  const adapter = yield* makeClaudeAdapter(decodeClaudeSettings({}), {
    instanceId: A,
    modelCatalog: Effect.succeed(SYNTHETIC_CLAUDE_MODEL_CATALOG),
    getSessionMessages: (async () => {
      if (history.messages === "unreadable") throw new Error("session file unreadable");
      return history.messages;
    }) as never,
    createQuery: (input) => {
      const query = new ScriptedQuery();
      queries.push(query);
      void (async () => {
        for await (const message of input.prompt) prompts.push(promptText(message));
      })();
      return query;
    },
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        ServerConfig.layerTest("/tmp/claude-recovery-test", "/tmp"),
        ServerSettings.ServerSettingsService.layerTest(),
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
  const server = yield* startServer(world, { adapters: { [A]: adapter } });
  const finish = server.close.pipe(Effect.andThen(Effect.sync(() => dropWorld(world))));
  return { world, server, queries, prompts, history, finish };
});

it.effect("an SDK failure resumes through the real adapter with a continuation prompt", () =>
  Effect.gen(function* () {
    const { server, queries, prompts, finish } = yield* startClaudeServer;
    yield* Effect.gen(function* () {
      const id = yield* server.createThread("t-claude-transient");
      yield* server.startTurn(id, "original prompt");
      yield* server.settle;
      expect(prompts).toEqual(["original prompt"]);

      queries[0]!.emit(failedResult(["read ECONNRESET"], "result-1"));
      yield* server.settle;
      expect(Option.getOrThrow(yield* server.intent(id))).toMatchObject({
        kind: "transient",
        status: "waiting",
        instanceId: A,
      });

      yield* server.advance(5_000);
      // The same SDK session gets one more prompt; the original is not sent again.
      expect(prompts).toEqual(["original prompt", "Continue where you left off."]);
      expect(queries).toHaveLength(1);
    }).pipe(Effect.ensuring(finish));
  }),
);

it.effect(
  "a window that parks the turn in the SDK is left alone, and a quota failure waits for the reset",
  () =>
    Effect.gen(function* () {
      const { world, server, queries, prompts, finish } = yield* startClaudeServer;
      yield* Effect.gen(function* () {
        world.quota.set(A, exhausted(T0 + HOUR));
        world.quota.set(B, exhausted(T0 + 2 * HOUR));
        const id = yield* server.createThread("t-claude-quota");
        yield* server.startTurn(id, "original prompt");
        yield* server.settle;

        // The SDK reports the window and keeps the turn open: nothing is started.
        queries[0]!.emit(rejectedWindow(T0 + HOUR, "limit-1"));
        yield* server.settle;
        yield* server.advance(30 * 60_000);
        expect(Option.isNone(yield* server.intent(id))).toBe(true);
        expect(prompts).toEqual(["original prompt"]);

        // When the SDK gives up on the turn, the wait runs to the reported reset.
        queries[0]!.emit(failedResult([], "result-limit"));
        yield* server.settle;
        const waiting = Option.getOrThrow(yield* server.intent(id));
        expect(waiting).toMatchObject({ kind: "quota", targetInstanceId: null });
        expect(waiting.wakeAt).toBe(iso(T0 + HOUR));

        yield* server.advance(HOUR - 30 * 60_000 - 1);
        expect(prompts).toEqual(["original prompt"]);
        yield* server.advance(1);
        expect(prompts).toEqual(["original prompt", "Continue where you left off."]);
      }).pipe(Effect.ensuring(finish));
    }),
);

it.effect("a quota failure moves the real adapter's session to an account with headroom", () =>
  Effect.gen(function* () {
    const { world, server, queries, prompts, finish } = yield* startClaudeServer;
    yield* Effect.gen(function* () {
      world.quota.set(A, exhausted(T0 + HOUR));
      world.quota.set(B, { read: "fresh", windows: [{ id: "five_hour", usedPercent: 5 }] });
      const id = yield* server.createThread("t-claude-switch");
      yield* server.startTurn(id, "original prompt");
      queries[0]!.emit(sessionInit());
      yield* server.settle;
      queries[0]!.emit(rejectedWindow(T0 + HOUR, "limit-1"));
      queries[0]!.emit(failedResult([], "result-limit"));
      yield* server.settle;
      yield* server.advance(0);

      // B copied the conversation from the cursor the real adapter reported.
      const fork = server.fakes[B]!.forkThread.mock.calls[0]![0];
      expect(fork.sourceResumeCursor).toMatchObject({ resume: "sdk-session-1" });
      expect(Option.getOrThrow(yield* server.binding(id)).providerInstanceId).toBe(B);
      expect(queries[0]!).toBeDefined();
      expect(prompts).toEqual(["original prompt"]);
      expect(server.fakes[B]!.sendTurn.mock.calls.map(([turn]) => turn.input)).toEqual([
        "Continue where you left off.",
      ]);
    }).pipe(Effect.ensuring(finish));
  }),
);

const CONTINUE = "Continue where you left off.";

/** A real iterator rejection (not a result message) on a started, initialised turn. */
const failIterator = (error: unknown) =>
  Effect.gen(function* () {
    const harness = yield* startClaudeServer;
    const { server, queries } = harness;
    const id = yield* server.createThread("t-claude-iterator");
    yield* server.startTurn(id, "original prompt");
    queries[0]!.emit(sessionInit());
    yield* server.settle;
    return { ...harness, id, fail: () => queries[0]!.fail(error) };
  });

const econnreset = () =>
  Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET", errno: -54 });

it.effect("a dropped SDK connection retries and resumes once native history is reconciled", () =>
  Effect.gen(function* () {
    const { server, queries, prompts, history, id, fail, finish } =
      yield* failIterator(econnreset());
    yield* Effect.gen(function* () {
      history.messages = [humanPrompt("human-1"), toolUse("tu-1"), toolResult("tu-1")];
      fail();
      yield* server.settle;
      expect(Option.getOrThrow(yield* server.intent(id))).toMatchObject({
        kind: "transient",
        status: "waiting",
      });
      yield* server.advance(5_000);
      // The session died with the stream, so the continuation opens a resumed query.
      expect(prompts).toEqual(["original prompt", CONTINUE]);
      expect(queries).toHaveLength(2);
    }).pipe(Effect.ensuring(finish));
  }),
);

it.effect("an accepted native tool call with no result parks the thread instead of resuming", () =>
  Effect.gen(function* () {
    const { server, prompts, history, id, fail, finish } = yield* failIterator(econnreset());
    yield* Effect.gen(function* () {
      // No tool.started was ever projected: only the SDK's own record shows the call.
      history.messages = [humanPrompt("human-1"), toolUse("tu-1")];
      fail();
      yield* server.settle;
      yield* server.advance(5_000);
      expect(prompts).toEqual(["original prompt"]);
      expect(Option.isNone(yield* server.intent(id))).toBe(true);
      expect((yield* server.recoveryRows(id)).map((row) => row.phase)).toContain("needs-attention");
    }).pipe(Effect.ensuring(finish));
  }),
);

it.effect("unreadable native history parks the thread instead of resuming", () =>
  Effect.gen(function* () {
    const { server, prompts, history, id, fail, finish } = yield* failIterator(econnreset());
    yield* Effect.gen(function* () {
      history.messages = "unreadable";
      fail();
      yield* server.settle;
      yield* server.advance(5_000);
      expect(prompts).toEqual(["original prompt"]);
      expect((yield* server.recoveryRows(id)).map((row) => row.phase)).toContain("needs-attention");
    }).pipe(Effect.ensuring(finish));
  }),
);

it.effect("an auth failure from the iterator is not retried", () =>
  Effect.gen(function* () {
    const { server, prompts, id, fail, finish } = yield* failIterator(
      new Error("401 unauthorized: invalid x-api-key sk-secret-123"),
    );
    yield* Effect.gen(function* () {
      fail();
      yield* server.settle;
      yield* server.advance(HOUR);
      expect(prompts).toEqual(["original prompt"]);
      expect(Option.isNone(yield* server.intent(id))).toBe(true);
    }).pipe(Effect.ensuring(finish));
  }),
);

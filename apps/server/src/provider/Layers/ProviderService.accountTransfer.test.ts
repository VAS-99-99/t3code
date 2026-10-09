// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { afterAll, expect } from "vite-plus/test";

import * as ServerConfig from "../../config.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { ProviderAdapterRequestError } from "../Errors.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import {
  makeFakeProviderAdapter,
  makeMultiInstanceRegistry,
} from "../testUtils/fakeProviderAdapter.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

const CLAUDE = ProviderDriverKind.make("claudeAgent");
const CODEX = ProviderDriverKind.make("codex");
const A = ProviderInstanceId.make("claude-a");
const B = ProviderInstanceId.make("claude-b");
const C = ProviderInstanceId.make("codex-c");

const cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "provider-transfer-test-"));
afterAll(() => NodeFS.rmSync(cwd, { recursive: true, force: true }));

const fakeA = makeFakeProviderAdapter({ provider: CLAUDE, instanceId: A });
const fakeB = makeFakeProviderAdapter({ provider: CLAUDE, instanceId: B });
const fakeNoFork = makeFakeProviderAdapter({
  provider: CLAUDE,
  instanceId: ProviderInstanceId.make("claude-nofork"),
  fork: false,
});
const fakeC = makeFakeProviderAdapter({ provider: CODEX, instanceId: C });
const disabled = new Map([[ProviderInstanceId.make("claude-off"), false]]);
const fakeOff = makeFakeProviderAdapter({
  provider: CLAUDE,
  instanceId: ProviderInstanceId.make("claude-off"),
});

const registry = makeMultiInstanceRegistry([fakeA, fakeB, fakeNoFork, fakeC, fakeOff], disabled);
const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
  Layer.provide(SqlitePersistenceMemory),
);
const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
const layer = Layer.mergeAll(
  makeProviderServiceLive().pipe(
    Layer.provide(NodeServices.layer),
    Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
    Layer.provide(directoryLayer),
    Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(Layer.provide(NodeServices.layer)),
    ),
    Layer.provideMerge(AnalyticsService.layerTest),
    Layer.provide(
      Layer.succeed(
        ProviderEventLoggers.ProviderEventLoggers,
        ProviderEventLoggers.NoOpProviderEventLoggers,
      ),
    ),
  ),
  directoryLayer,
);

const start = (name: string, instanceId = A, provider = CLAUDE) =>
  Effect.gen(function* () {
    const service = yield* ProviderService.ProviderService;
    const threadId = ThreadId.make(name);
    const session = yield* service.startSession(threadId, {
      threadId,
      provider,
      providerInstanceId: instanceId,
      cwd,
      modelSelection: createModelSelection(instanceId, "claude-sonnet-5-5"),
      runtimeMode: "full-access",
    });
    return { service, threadId, session };
  });

it.layer(layer)("ProviderService account transfer", (it) => {
  it.effect("moves the thread to a sibling account through that account's own fork", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const { service, threadId, session } = yield* start("transfer-ok");
      let staged: unknown;
      yield* service.transferConversation({
        threadId,
        toInstanceId: B,
        onForked: (cursor) => Effect.sync(() => void (staged = cursor)),
      });

      const forkCall = fakeB.forkThread.mock.calls[0]![0];
      assert.deepStrictEqual(forkCall.sourceResumeCursor, session.resumeCursor);
      assert.strictEqual(forkCall.destinationThreadId, threadId);
      assert.strictEqual(fakeA.forkThread.mock.calls.length, 0);
      expect(fakeA.stopSession).toHaveBeenCalledWith(threadId);
      assert.isFalse(fakeA.sessions.has(threadId));
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.strictEqual(binding.providerInstanceId, B);
      assert.strictEqual(binding.status, "stopped");
      assert.deepStrictEqual(binding.resumeCursor, staged);
      const payload = binding.runtimePayload as { modelSelection: { instanceId: string } };
      assert.strictEqual(payload.modelSelection.instanceId, B);

      // A later turn resumes the copied conversation on the new account only.
      yield* service.sendTurn({ threadId, input: "continue", interactionMode: "default" });
      assert.deepStrictEqual(fakeB.startSession.mock.calls.at(-1)![0].resumeCursor, staged);
      assert.strictEqual(fakeA.sendTurn.mock.calls.length, 0);
    }),
  );

  it.effect("refuses a running source before any native copy is made", () =>
    Effect.gen(function* () {
      const { service, threadId } = yield* start("transfer-running");
      yield* service.sendTurn({ threadId, input: "work", interactionMode: "default" });
      const forks = fakeB.forkThread.mock.calls.length;
      const exit = yield* Effect.exit(service.transferConversation({ threadId, toInstanceId: B }));
      assert.isTrue(Exit.isFailure(exit));
      assert.strictEqual(fakeB.forkThread.mock.calls.length, forks);
      assert.isTrue(fakeA.sessions.has(threadId));
    }),
  );

  it.effect("keeps the thread on its account when the target cannot copy the history", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const { service, threadId } = yield* start("transfer-isolated");
      fakeB.forkThread.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: CLAUDE,
            method: "thread/fork",
            detail: "Session was not found in this account's configuration.",
          }),
        ),
      );
      let forked = false;
      const exit = yield* Effect.exit(
        service.transferConversation({
          threadId,
          toInstanceId: B,
          onForked: () => Effect.sync(() => void (forked = true)),
        }),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.isFalse(forked);
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.strictEqual(binding.providerInstanceId, A);
      assert.isTrue(fakeA.sessions.has(threadId));
      expect(fakeA.stopSession).not.toHaveBeenCalledWith(threadId);
    }),
  );

  it.effect("rejects targets that cannot take the conversation", () =>
    Effect.gen(function* () {
      const { service, threadId } = yield* start("transfer-invalid");
      for (const toInstanceId of [
        A, // already there
        C, // different driver
        ProviderInstanceId.make("claude-off"), // disabled in settings
        ProviderInstanceId.make("claude-nofork"), // no native fork
      ]) {
        const exit = yield* Effect.exit(service.transferConversation({ threadId, toInstanceId }));
        assert.isTrue(Exit.isFailure(exit), toInstanceId);
      }
      assert.isTrue(fakeA.sessions.has(threadId));
    }),
  );

  it.effect("binds a native copy staged by an earlier attempt without copying again", () =>
    Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const { service, threadId } = yield* start("transfer-staged");
      const forks = fakeB.forkThread.mock.calls.length;
      yield* service.transferConversation({
        threadId,
        toInstanceId: B,
        stagedCursor: { resume: "already-copied" },
      });
      assert.strictEqual(fakeB.forkThread.mock.calls.length, forks);
      const binding = Option.getOrThrow(yield* directory.getBinding(threadId));
      assert.strictEqual(binding.providerInstanceId, B);
      assert.deepStrictEqual(binding.resumeCursor, { resume: "already-copied" });
    }),
  );

  it.effect("reads native history by resuming the bound session", () =>
    Effect.gen(function* () {
      const { service, threadId } = yield* start("read-history");
      yield* service.stopSession({ threadId });
      const snapshot = yield* service.readConversation(threadId);
      assert.strictEqual(snapshot.threadId, threadId);
      assert.isTrue(fakeA.sessions.has(threadId));
    }),
  );
});

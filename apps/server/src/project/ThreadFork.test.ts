// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it, vi } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  ComposerContextId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadForkError,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { ServerConfig } from "../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { OrchestrationCommandInvariantError } from "../orchestration/Errors.ts";
import { OrchestrationEngineLive } from "../orchestration/Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../orchestration/Layers/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../orchestration/ThreadPlanProgress.ts";
import { makeClaudeAdapter } from "../provider/Layers/ClaudeAdapter.ts";
import { SYNTHETIC_CLAUDE_MODEL_CATALOG } from "../provider/ClaudeModelCatalog.testFixtures.ts";
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "../provider/Layers/ProviderEventLoggers.ts";
import { makeProviderServiceLive } from "../provider/Layers/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "../provider/Layers/ProviderSessionDirectory.ts";
import * as ProviderAdapterRegistry from "../provider/Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import { makeAdapterRegistryMock } from "../provider/testUtils/providerAdapterRegistryMock.ts";
import { createAttachmentId } from "../attachmentStore.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as RepositoryIdentityResolver from "./RepositoryIdentityResolver.ts";
import * as ThreadFork from "./ThreadFork.ts";

const workspaceFor = (name: string) => `/tmp/thread-fork-workspace/${name}`;
const SOURCE_SESSION = "550e8400-e29b-41d4-a716-446655440010";
const CLAUDE = ProviderInstanceId.make("claudeAgent");
const CURSOR = ProviderInstanceId.make("cursor");
const BASE_TIME = Date.parse("2026-08-30T10:00:00.000Z");
const decodeClaudeSettings = Schema.decodeSync(ClaudeSettings);

const forkSession = vi.fn(async (_sessionId: string, _options?: unknown) => ({
  sessionId: `550e8400-e29b-41d4-a716-4466554401${String(forkSession.mock.calls.length).padStart(2, "0")}`,
}));
const createQuery = vi.fn((): never => {
  throw new Error("A fork must not open a Claude query.");
});

/** A live, idle SDK query. No prompt or model request is sent. */
function idleQuery() {
  let done = false;
  let next: ((value: IteratorResult<never>) => void) | undefined;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next: () =>
      done
        ? Promise.resolve({ done: true as const, value: undefined })
        : new Promise<IteratorResult<never>>((resolve) => {
            next = resolve;
          }),
    close() {
      done = true;
      next?.({ done: true, value: undefined });
    },
  };
}

const serverConfig = ServerConfig.layerTest(process.cwd(), { prefix: "t3-thread-fork-test-" });

// The real Claude adapter backs the fork; only the SDK's session functions are fixtures.
const registryLayer = Layer.effect(
  ProviderAdapterRegistry.ProviderAdapterRegistry,
  Effect.gen(function* () {
    const claude = yield* makeClaudeAdapter(decodeClaudeSettings({}), {
      modelCatalog: Effect.succeed(SYNTHETIC_CLAUDE_MODEL_CATALOG),
      forkSession,
      createQuery,
    });
    const { forkThread: _forkThread, ...withoutFork } = claude;
    return makeAdapterRegistryMock({
      [ProviderDriverKind.make("claudeAgent")]: claude,
      [ProviderDriverKind.make("cursor")]: {
        ...withoutFork,
        provider: ProviderDriverKind.make("cursor"),
        capabilities: { sessionModelSwitch: "in-session" },
        hasSession: () => Effect.succeed(false),
        listSessions: () => Effect.succeed([]),
        stopSession: () => Effect.void,
      },
    });
  }),
);
const providerLayer = makeProviderServiceLive().pipe(
  Layer.provideMerge(registryLayer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provide(AnalyticsService.layerTest),
  Layer.provide(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);

/** The whole stack over one database: in memory by default, a file for reopen tests. */
const makeForkLayer = <E>(
  sqlite: Layer.Layer<SqlClient.SqlClient, E>,
  config: typeof serverConfig | Layer.Layer<ServerConfig>,
) => {
  const runtimeRepository = ProviderSessionRuntime.layer.pipe(Layer.provide(sqlite));
  const persistence = Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
    runtimeRepository,
    ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepository)),
  ).pipe(
    Layer.provide(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provide(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(sqlite),
    Layer.provideMerge(config),
    Layer.provideMerge(NodeServices.layer),
  );
  return ThreadFork.layer.pipe(Layer.provideMerge(providerLayer), Layer.provideMerge(persistence));
};
const forkLayer = makeForkLayer(SqlitePersistenceMemory, serverConfig);

let tick = 0;
const at = () => DateTime.formatIso(DateTime.makeUnsafe(BASE_TIME + tick++ * 1_000));

const dispatch = Effect.fn("dispatch")(function* (
  command: Parameters<OrchestrationEngine.OrchestrationEngineService["Service"]["dispatch"]>[0],
) {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  return yield* engine.dispatch(command);
});

const bindSource = Effect.fn("bindSource")(function* (
  threadId: ThreadId,
  workspace: string,
  instanceId: ProviderInstanceId = CLAUDE,
) {
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  yield* directory.upsert({
    threadId,
    provider: ProviderDriverKind.make(instanceId),
    providerInstanceId: instanceId,
    status: "stopped",
    runtimeMode: "approval-required",
    resumeCursor: {
      threadId,
      resume: SOURCE_SESSION,
      turnCount: 2,
      turnStartMessageIds: ["native-user-1", "native-user-2"],
    },
    runtimePayload: {
      cwd: workspace,
      modelSelection: { instanceId, model: "claude-sonnet-4-5" },
    },
  });
});

const readStagedFork = Effect.fn("readStagedFork")(function* (threadId: ThreadId) {
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  return Option.getOrThrow(yield* directory.forks!.get(threadId));
});

type TurnEnd = "ready" | "interrupted" | "running";
type UserMessage = Extract<OrchestrationCommand, { type: "thread.message.user.append" }>["message"];

/** One user prompt and assistant answer, closed the way the provider would close it. */
const runTurn = Effect.fn("runTurn")(function* (
  threadId: ThreadId,
  index: number,
  options: {
    readonly attachment?: UserMessage;
    readonly end?: TurnEnd;
  } = {},
) {
  const turnId = TurnId.make(`${threadId}-turn-${index}`);
  const session = (status: TurnEnd, activeTurnId: TurnId | null) => ({
    threadId,
    status,
    providerName: "claudeAgent",
    providerInstanceId: CLAUDE,
    runtimeMode: "approval-required" as const,
    activeTurnId,
    lastError: null,
    updatedAt: at(),
  });
  yield* dispatch({
    type: "thread.message.user.append",
    commandId: CommandId.make(`${threadId}-append-${index}`),
    threadId,
    message: options.attachment ?? {
      messageId: MessageId.make(`${threadId}-user-${index}`),
      text: `Question ${index}`,
      attachments: [],
    },
    createdAt: at(),
  });
  yield* dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`${threadId}-running-${index}`),
    threadId,
    session: session("running", turnId),
    createdAt: at(),
  });
  yield* dispatch({
    type: "thread.message.assistant.delta",
    commandId: CommandId.make(`${threadId}-delta-${index}`),
    threadId,
    messageId: MessageId.make(`${threadId}-assistant-${index}`),
    delta: `Answer ${index}`,
    turnId,
    createdAt: at(),
  });
  yield* dispatch({
    type: "thread.message.assistant.complete",
    commandId: CommandId.make(`${threadId}-complete-${index}`),
    threadId,
    messageId: MessageId.make(`${threadId}-assistant-${index}`),
    turnId,
    createdAt: at(),
  });
  const end = options.end ?? "ready";
  if (end !== "running") {
    yield* dispatch({
      type: "thread.session.set",
      commandId: CommandId.make(`${threadId}-end-${index}`),
      threadId,
      session: session(end, null),
      createdAt: at(),
    });
  }
});

/** A project and a source thread with two completed turns; the first carries an image. */
const seedSource = Effect.fn("seedSource")(function* (
  name: string,
  options: {
    readonly turns?: number;
    readonly end?: TurnEnd;
    readonly instanceId?: ProviderInstanceId;
  } = {},
) {
  const config = yield* ServerConfig;
  const instanceId = options.instanceId ?? CLAUDE;
  const projectId = ProjectId.make(`project-${name}`);
  const sourceThreadId = ThreadId.make(`${name}-source`);
  const workspace = workspaceFor(name);
  yield* TestClock.setTime(BASE_TIME);
  yield* dispatch({
    type: "project.create",
    commandId: CommandId.make(`${name}-project`),
    projectId,
    title: "Project",
    workspaceRoot: workspace,
    defaultModelSelection: null,
    createdAt: at(),
  });
  yield* dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${name}-source-create`),
    threadId: sourceThreadId,
    projectId,
    title: "Source thread",
    modelSelection: { instanceId, model: "claude-sonnet-4-5" },
    runtimeMode: "approval-required",
    interactionMode: "plan",
    branch: "feature/work",
    worktreePath: workspace,
    createdAt: at(),
  });
  const attachmentId = createAttachmentId(sourceThreadId, ".png")!;
  NodeFS.writeFileSync(
    NodePath.join(config.attachmentsDir, `${attachmentId}.png`),
    new Uint8Array([1, 2, 3]),
  );
  const turns = options.turns ?? 2;
  for (let index = 1; index <= turns; index += 1) {
    yield* runTurn(sourceThreadId, index, {
      ...(index === 1
        ? {
            attachment: {
              messageId: MessageId.make(`${sourceThreadId}-user-1`),
              text: "Question 1 with a screenshot",
              attachments: [
                {
                  type: "image" as const,
                  id: attachmentId,
                  name: "shot.png",
                  mimeType: "image/png",
                  sizeBytes: 3,
                },
              ],
              context: {
                version: 1 as const,
                records: [
                  {
                    version: 1 as const,
                    contextId: ComposerContextId.make("image-context-1"),
                    label: "shot.png",
                    kind: "image" as const,
                    attachmentId,
                    name: "shot.png",
                    mimeType: "image/png",
                    sizeBytes: 3,
                  },
                ],
              },
            },
          }
        : {}),
      ...(index === turns && options.end !== undefined ? { end: options.end } : {}),
    });
  }
  yield* bindSource(sourceThreadId, workspace, instanceId);
  return { projectId, sourceThreadId, attachmentId };
});

const forkInput = (name: string, commandId = `${name}-fork`) => ({
  sourceThreadId: ThreadId.make(`${name}-source`),
  destinationThreadId: ThreadId.make(`${name}-destination`),
  commandId: CommandId.make(commandId),
});

const readThread = Effect.fn("readThread")(function* (threadId: ThreadId) {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  return Option.getOrUndefined(yield* snapshots.getThreadDetailById(threadId));
});

const requireThread = Effect.fn("requireThread")(function* (threadId: ThreadId) {
  return (yield* readThread(threadId)) as OrchestrationThread;
});

const readBinding = Effect.fn("readBinding")(function* (threadId: ThreadId) {
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  return Option.getOrUndefined(yield* directory.getBinding(threadId));
});

const threadEventTypes = Effect.fn("threadEventTypes")(function* (threadId: ThreadId) {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const events = Array.from(yield* Stream.runCollect(engine.readEvents(0)));
  return events.filter((event) => event.aggregateId === threadId).map((event) => event.type);
});

const failureOf = <A, R>(effect: Effect.Effect<A, ThreadForkError, R>) =>
  effect.pipe(
    Effect.flip,
    Effect.tap((error) => Effect.sync(() => expect(error).toBeInstanceOf(ThreadForkError))),
  );

const CAPTURED_TEXTS = ["Question 1 with a screenshot", "Answer 1", "Question 2", "Answer 2"];
const FORK_EVENTS = [
  "thread.created",
  "thread.message-sent",
  "thread.message-sent",
  "thread.message-sent",
  "thread.message-sent",
  "thread.settled",
];

/** Rejects the destination's creating command once, as if the write failed after the native fork. */
const failingCreateOnce = (
  engine: OrchestrationEngine.OrchestrationEngineService["Service"],
): OrchestrationEngine.OrchestrationEngineService["Service"] => {
  let failed = false;
  return OrchestrationEngine.OrchestrationEngineService.of({
    ...engine,
    dispatch: (command) =>
      command.type === "thread.create-with-history" && !failed
        ? Effect.sync(() => {
            failed = true;
          }).pipe(
            Effect.andThen(
              Effect.fail(
                new OrchestrationCommandInvariantError({
                  commandType: command.type,
                  detail: "Injected history failure.",
                }),
              ),
            ),
          )
        : engine.dispatch(command),
  });
};

const failFirstCreate = Effect.fn("failFirstCreate")(function* (
  input: ReturnType<typeof forkInput>,
) {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const scope = yield* Scope.make();
  const flaky = yield* Layer.build(Layer.fresh(ThreadFork.layer)).pipe(
    Effect.provideService(
      OrchestrationEngine.OrchestrationEngineService,
      failingCreateOnce(engine),
    ),
    Scope.provide(scope),
  );
  const error = yield* failureOf(
    ThreadFork.ThreadFork.pipe(
      Effect.flatMap((fork) => fork.fork(input)),
      Effect.provide(flaky),
    ),
  );
  yield* Scope.close(scope, Exit.void);
  return error;
});

it.layer(forkLayer)("ThreadFork", (it) => {
  it.effect(
    "finishes a queued same-request retry after another service publishes and the source disappears",
    () =>
      Effect.gen(function* () {
        yield* seedSource("queued-retry");
        const input = forkInput("queued-retry");
        const engine = yield* OrchestrationEngine.OrchestrationEngineService;
        const entered = Promise.withResolvers<void>();
        const release = Promise.withResolvers<void>();
        const calls = forkSession.mock.calls.length;
        forkSession.mockImplementationOnce(async () => {
          entered.resolve();
          await release.promise;
          return { sessionId: "550e8400-e29b-41d4-a716-446655440780" };
        });
        const publishing = OrchestrationEngine.OrchestrationEngineService.of({
          ...engine,
          dispatch: (command) =>
            engine.dispatch(command).pipe(
              Effect.tap(() =>
                command.type === "thread.create-with-history"
                  ? engine
                      .dispatch({
                        type: "thread.delete",
                        commandId: CommandId.make("queued-retry-delete-source"),
                        threadId: input.sourceThreadId,
                      })
                      .pipe(Effect.asVoid)
                  : Effect.void,
              ),
            ),
        });
        const scope = yield* Scope.make();
        const firstServices = yield* Layer.build(Layer.fresh(ThreadFork.layer)).pipe(
          Effect.provideService(OrchestrationEngine.OrchestrationEngineService, publishing),
          Scope.provide(scope),
        );
        const first = yield* ThreadFork.ThreadFork.pipe(Effect.provide(firstServices));
        const second = yield* ThreadFork.ThreadFork;
        const pendingFirst = yield* first.fork(input).pipe(Effect.forkChild);
        yield* Effect.promise(() => entered.promise);
        const pendingSecond = yield* second.fork(input).pipe(Effect.forkChild);
        for (let turn = 0; turn < 50; turn += 1) yield* Effect.yieldNow;
        release.resolve();
        expect(yield* Fiber.join(pendingFirst)).toEqual({
          destinationThreadId: input.destinationThreadId,
        });
        expect(yield* Fiber.join(pendingSecond)).toEqual({
          destinationThreadId: input.destinationThreadId,
        });
        expect(forkSession.mock.calls.length).toBe(calls + 1);
        expect(
          (yield* requireThread(input.destinationThreadId)).messages.map((m) => m.text),
        ).toEqual(CAPTURED_TEXTS);
        yield* Scope.close(scope, Exit.void);
      }),
  );

  it.effect(
    "forks a completed live Claude session with the normal ready-to-running persistence mapping",
    () =>
      Effect.gen(function* () {
        yield* seedSource("open-idle");
        const input = forkInput("open-idle");
        const provider = yield* ProviderService.ProviderService;
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        yield* directory.upsert({
          threadId: input.sourceThreadId,
          provider: ProviderDriverKind.make("claudeAgent"),
          runtimePayload: { cwd: process.cwd() },
        });
        createQuery.mockImplementationOnce(() => idleQuery() as never);
        const live = yield* provider.startSession(input.sourceThreadId, {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: CLAUDE,
          threadId: input.sourceThreadId,
          runtimeMode: "approval-required",
          cwd: process.cwd(),
        });
        expect(live.status).toBe("ready");
        expect(live.activeTurnId).toBeUndefined();
        const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry;
        const adapter = yield* registry.getByInstance(CLAUDE);
        expect(
          (yield* adapter.listSessions()).find(
            (session) => session.threadId === input.sourceThreadId,
          ),
        ).toMatchObject({ status: "ready" });
        expect(yield* readBinding(input.sourceThreadId)).toMatchObject({
          status: "running",
          runtimePayload: { activeTurnId: null },
        });
        expect((yield* requireThread(input.sourceThreadId)).latestTurn?.state).toBe("completed");
        const queries = createQuery.mock.calls.length;
        expect(yield* (yield* ThreadFork.ThreadFork).fork(input)).toEqual({
          destinationThreadId: input.destinationThreadId,
        });
        expect(createQuery.mock.calls.length).toBe(queries);
        expect(
          (yield* requireThread(input.destinationThreadId)).messages.map((m) => m.text),
        ).toEqual(CAPTURED_TEXTS);
        const binding = (yield* readBinding(input.destinationThreadId))!;
        createQuery.mockImplementationOnce(() => idleQuery() as never);
        const resumed = yield* provider.startSession(input.destinationThreadId, {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: CLAUDE,
          threadId: input.destinationThreadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });
        expect(resumed.resumeCursor).toEqual(binding.resumeCursor);
        expect(resumed.resumeCursor).not.toMatchObject({ resume: SOURCE_SESSION });
      }),
  );

  it.effect(
    "does not accept an ordinary create receipt as proof that this request published the fork",
    () =>
      Effect.gen(function* () {
        yield* seedSource("foreign-receipt");
        const input = forkInput("foreign-receipt");
        const source = yield* requireThread(input.sourceThreadId);
        const entered = Promise.withResolvers<void>();
        const release = Promise.withResolvers<void>();
        forkSession.mockImplementationOnce(async () => {
          entered.resolve();
          await release.promise;
          return { sessionId: "550e8400-e29b-41d4-a716-446655440779" };
        });
        const fork = yield* ThreadFork.ThreadFork;
        const pending = yield* failureOf(fork.fork(input)).pipe(Effect.forkChild);
        yield* Effect.promise(() => entered.promise);
        yield* dispatch({
          type: "thread.create",
          commandId: CommandId.make(`${input.commandId}:create`),
          threadId: input.destinationThreadId,
          projectId: source.projectId,
          title: "Foreign",
          modelSelection: source.modelSelection,
          runtimeMode: source.runtimeMode,
          interactionMode: source.interactionMode,
          branch: null,
          worktreePath: null,
          createdAt: at(),
        });
        yield* dispatch({
          type: "thread.message.user.append",
          commandId: CommandId.make("foreign-receipt-append"),
          threadId: input.destinationThreadId,
          message: {
            messageId: MessageId.make("foreign-receipt-message"),
            text: "Foreign history",
            attachments: [],
          },
          createdAt: at(),
        });
        release.resolve();
        expect((yield* Fiber.join(pending)).reason).toBe("destination-conflict");
        expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
        expect((yield* readStagedFork(input.destinationThreadId)).publishedSequence).toBeNull();
        expect(
          (yield* requireThread(input.destinationThreadId)).messages.map((m) => m.text),
        ).toEqual(["Foreign history"]);
        expect((yield* failureOf(fork.fork(input))).reason).toBe("destination-conflict");
      }),
  );

  it.effect("never binds or claims an ordinary destination created during native acceptance", () =>
    Effect.gen(function* () {
      yield* seedSource("foreign-native");
      const input = forkInput("foreign-native");
      const source = yield* requireThread(input.sourceThreadId);
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      forkSession.mockImplementationOnce(async () => {
        entered.resolve();
        await release.promise;
        return { sessionId: "550e8400-e29b-41d4-a716-446655440777" };
      });
      const fork = yield* ThreadFork.ThreadFork;
      const pending = yield* failureOf(fork.fork(input)).pipe(Effect.forkChild);
      yield* Effect.promise(() => entered.promise);
      yield* dispatch({
        type: "thread.create",
        commandId: CommandId.make("foreign-native-create"),
        threadId: input.destinationThreadId,
        projectId: source.projectId,
        title: "Foreign",
        modelSelection: source.modelSelection,
        runtimeMode: source.runtimeMode,
        interactionMode: source.interactionMode,
        branch: null,
        worktreePath: null,
        createdAt: at(),
      });
      yield* dispatch({
        type: "thread.message.user.append",
        commandId: CommandId.make("foreign-native-append"),
        threadId: input.destinationThreadId,
        message: {
          messageId: MessageId.make("foreign-native-message"),
          text: "Foreign conversation",
          attachments: [],
        },
        createdAt: at(),
      });
      release.resolve();
      expect((yield* Fiber.join(pending)).reason).toBe("history-failed");
      const foreign = yield* requireThread(input.destinationThreadId);
      expect(foreign.messages.map((m) => m.text)).toEqual(["Foreign conversation"]);
      expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
      const staged = yield* readStagedFork(input.destinationThreadId);
      expect(staged.publishedSequence).toBeNull();
      expect((yield* failureOf(fork.fork(input))).reason).toBe("destination-conflict");
      expect(yield* requireThread(input.destinationThreadId)).toEqual(foreign);
      expect(yield* readStagedFork(input.destinationThreadId)).toEqual(staged);
    }),
  );

  it.effect(
    "preserves a foreign binding created after native acceptance and before atomic publication",
    () =>
      Effect.gen(function* () {
        yield* seedSource("foreign-publication");
        const input = forkInput("foreign-publication");
        const engine = yield* OrchestrationEngine.OrchestrationEngineService;
        const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
        const foreignCursor = { resume: "550e8400-e29b-41d4-a716-446655440888" };
        const fixtureServices = yield* Effect.context<
          | ProviderSessionDirectory.ProviderSessionDirectory
          | ProjectionSnapshotQuery.ProjectionSnapshotQuery
        >();
        let foreign: OrchestrationThread | undefined;
        const racing = OrchestrationEngine.OrchestrationEngineService.of({
          ...engine,
          dispatch: (command) =>
            command.type !== "thread.create-with-history"
              ? engine.dispatch(command)
              : Effect.gen(function* () {
                  // The copy is already durable here, but there must still be no routable binding.
                  expect(
                    (yield* readStagedFork(input.destinationThreadId)).publishedSequence,
                  ).toBeNull();
                  expect(
                    yield* readBinding(input.destinationThreadId).pipe(Effect.orDie),
                  ).toBeUndefined();
                  const { messages: _messages, fork: _fork, type: _type, ...fields } = command;
                  yield* engine.dispatch({
                    ...fields,
                    type: "thread.create",
                    commandId: CommandId.make("foreign-publication-create"),
                    title: "Foreign",
                  });
                  yield* directory
                    .upsert({
                      threadId: input.destinationThreadId,
                      provider: ProviderDriverKind.make("claudeAgent"),
                      providerInstanceId: CLAUDE,
                      status: "stopped",
                      resumeCursor: foreignCursor,
                      runtimePayload: { cwd: "foreign-workspace" },
                    })
                    .pipe(Effect.orDie);
                  yield* engine.dispatch({
                    type: "thread.message.user.append",
                    commandId: CommandId.make("foreign-publication-append"),
                    threadId: input.destinationThreadId,
                    message: {
                      messageId: MessageId.make("foreign-publication-message"),
                      text: "Foreign history",
                      attachments: [],
                    },
                    createdAt: at(),
                  });
                  foreign = yield* requireThread(input.destinationThreadId).pipe(Effect.orDie);
                  return yield* engine.dispatch(command);
                }).pipe(Effect.provide(fixtureServices)),
        });
        const scope = yield* Scope.make();
        const services = yield* Layer.build(Layer.fresh(ThreadFork.layer)).pipe(
          Effect.provideService(OrchestrationEngine.OrchestrationEngineService, racing),
          Scope.provide(scope),
        );
        expect(
          (yield* failureOf(
            ThreadFork.ThreadFork.pipe(
              Effect.flatMap((fork) => fork.fork(input)),
              Effect.provide(services),
            ),
          )).reason,
        ).toBe("history-failed");
        yield* Scope.close(scope, Exit.void);
        const binding = yield* readBinding(input.destinationThreadId);
        expect(binding?.resumeCursor).toEqual(foreignCursor);
        expect((yield* failureOf((yield* ThreadFork.ThreadFork).fork(input))).reason).toBe(
          "destination-conflict",
        );
        expect(yield* readBinding(input.destinationThreadId)).toEqual(binding);
        expect(yield* requireThread(input.destinationThreadId)).toEqual(foreign);
        expect((yield* readStagedFork(input.destinationThreadId)).publishedSequence).toBeNull();
      }),
  );

  it.effect(
    "rolls back publication, history and receipt when binding insertion fails after native acceptance",
    () =>
      Effect.gen(function* () {
        yield* seedSource("binding-failure");
        const input = forkInput("binding-failure");
        const sql = yield* SqlClient.SqlClient;
        const forks = forkSession.mock.calls.length;
        yield* sql`CREATE TRIGGER fail_fork_binding BEFORE INSERT ON provider_session_runtime
        WHEN NEW.thread_id = 'binding-failure-destination' BEGIN SELECT RAISE(ABORT, 'Injected binding failure'); END`;
        expect((yield* failureOf((yield* ThreadFork.ThreadFork).fork(input))).reason).toBe(
          "history-failed",
        );
        expect(forkSession.mock.calls.length).toBe(forks + 1);
        expect(yield* readThread(input.destinationThreadId)).toBeUndefined();
        expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
        expect(yield* threadEventTypes(input.destinationThreadId)).toEqual([]);
        const receipts =
          yield* sql`SELECT status FROM orchestration_command_receipts WHERE command_id = ${`${input.commandId}:create`}`;
        expect(receipts).toEqual([]);
        const staged = yield* readStagedFork(input.destinationThreadId);
        expect(staged.publishedSequence).toBeNull();
        yield* sql`DROP TRIGGER fail_fork_binding`;
        yield* runTurn(input.sourceThreadId, 3, { end: "running" });
        expect(yield* (yield* ThreadFork.ThreadFork).fork(input)).toEqual({
          destinationThreadId: input.destinationThreadId,
        });
        expect(forkSession.mock.calls.length).toBe(forks + 1);
        expect(yield* readBinding(input.destinationThreadId)).toMatchObject(staged.binding);
        expect(
          (yield* requireThread(input.destinationThreadId)).messages.map((m) => m.text),
        ).toEqual(CAPTURED_TEXTS);
        expect((yield* readStagedFork(input.destinationThreadId)).publishedSequence).not.toBeNull();
      }),
  );

  it.effect("copies completed history into an independent thread without starting a turn", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig;
      const { projectId, sourceThreadId, attachmentId } = yield* seedSource("copy");
      const sourceBefore = yield* requireThread(sourceThreadId);
      const sourceBindingBefore = yield* readBinding(sourceThreadId);
      const input = { ...forkInput("copy"), title: "Branch of the work" };
      const forks = forkSession.mock.calls.length;

      const result = yield* (yield* ThreadFork.ThreadFork).fork(input);

      expect(result).toEqual({ destinationThreadId: input.destinationThreadId });
      const destination = yield* requireThread(input.destinationThreadId);
      expect(destination).toMatchObject({
        projectId,
        title: "Branch of the work",
        modelSelection: sourceBefore.modelSelection,
        runtimeMode: "approval-required",
        interactionMode: "plan",
        branch: "feature/work",
        worktreePath: workspaceFor("copy"),
        latestTurn: null,
        session: null,
        activities: [],
        checkpoints: [],
        proposedPlans: [],
        archivedAt: null,
        deletedAt: null,
      });
      expect(destination.messages.map(({ role, text }) => ({ role, text }))).toEqual(
        sourceBefore.messages.map(({ role, text }) => ({ role, text })),
      );
      expect(destination.messages).toHaveLength(4);

      const copied = destination.messages[0]!.attachments![0]!;
      expect(copied).toMatchObject({
        type: "image",
        name: "shot.png",
        mimeType: "image/png",
        sizeBytes: 3,
      });
      expect(copied.id).not.toBe(attachmentId);
      expect(copied.id.startsWith(`${input.destinationThreadId}-`)).toBe(true);
      expect(
        Array.from(NodeFS.readFileSync(NodePath.join(config.attachmentsDir, `${copied.id}.png`))),
      ).toEqual([1, 2, 3]);
      expect(NodeFS.existsSync(NodePath.join(config.attachmentsDir, `${attachmentId}.png`))).toBe(
        true,
      );
      expect(destination.messages[0]!.context?.records).toMatchObject([
        { kind: "image", attachmentId: copied.id },
      ]);

      // The original is untouched and the fork never started a turn.
      expect(yield* requireThread(sourceThreadId)).toEqual(sourceBefore);
      expect(yield* readBinding(sourceThreadId)).toEqual(sourceBindingBefore);
      expect(yield* threadEventTypes(input.destinationThreadId)).toEqual([
        "thread.created",
        "thread.message-sent",
        "thread.message-sent",
        "thread.message-sent",
        "thread.message-sent",
        "thread.settled",
      ]);
      expect(createQuery).not.toHaveBeenCalled();

      // The destination owns a different native session that the SDK created by forking.
      expect(forkSession.mock.calls.length).toBe(forks + 1);
      expect(forkSession.mock.lastCall).toEqual([SOURCE_SESSION, { dir: workspaceFor("copy") }]);
      const binding = (yield* readBinding(input.destinationThreadId))!;
      expect(binding).toMatchObject({
        provider: "claudeAgent",
        providerInstanceId: "claudeAgent",
        status: "stopped",
        runtimeMode: "approval-required",
        resumeCursor: { threadId: input.destinationThreadId, turnCount: 2 },
        runtimePayload: { cwd: workspaceFor("copy") },
      });
      const cursor = binding.resumeCursor as { resume: string };
      expect(cursor.resume).not.toBe(SOURCE_SESSION);
      expect(cursor.resume).toBe(
        (yield* Effect.promise(
          () => forkSession.mock.results.at(-1)!.value as Promise<{ sessionId: string }>,
        )).sessionId,
      );
    }),
  );

  it.effect("forks a fork that has not run a turn yet, keeping every session separate", () =>
    Effect.gen(function* () {
      yield* seedSource("chain");
      const fork = yield* ThreadFork.ThreadFork;
      const first = forkInput("chain");
      const second = {
        sourceThreadId: first.destinationThreadId,
        destinationThreadId: ThreadId.make("chain-grandchild"),
        commandId: CommandId.make("chain-second-fork"),
      };

      yield* fork.fork(first);
      yield* fork.fork(second);

      const original = yield* requireThread(first.sourceThreadId);
      const grandchild = yield* requireThread(second.destinationThreadId);
      expect(grandchild.messages.map(({ role, text }) => ({ role, text }))).toEqual(
        original.messages.map(({ role, text }) => ({ role, text })),
      );
      const copied = grandchild.messages[0]!.attachments![0]!;
      expect(copied.id.startsWith(`${second.destinationThreadId}-`)).toBe(true);
      const sessions = yield* Effect.forEach(
        [first.sourceThreadId, first.destinationThreadId, second.destinationThreadId],
        (threadId) =>
          readBinding(threadId).pipe(
            Effect.map((binding) => (binding!.resumeCursor as { resume: string }).resume),
          ),
      );
      expect(new Set(sessions).size).toBe(3);
    }),
  );

  it.effect("returns the same fork when the request is repeated", () =>
    Effect.gen(function* () {
      yield* seedSource("retry");
      const input = forkInput("retry");
      const fork = yield* ThreadFork.ThreadFork;
      const forks = forkSession.mock.calls.length;

      yield* fork.fork(input);
      const first = yield* requireThread(input.destinationThreadId);
      const bindingFirst = yield* readBinding(input.destinationThreadId);
      const eventsFirst = yield* threadEventTypes(input.destinationThreadId);
      const repeated = yield* fork.fork(input);

      expect(repeated).toEqual({ destinationThreadId: input.destinationThreadId });
      expect(forkSession.mock.calls.length).toBe(forks + 1);
      expect(yield* requireThread(input.destinationThreadId)).toEqual(first);
      expect(yield* readBinding(input.destinationThreadId)).toEqual(bindingFirst);
      expect(yield* threadEventTypes(input.destinationThreadId)).toEqual(eventsFirst);
    }),
  );

  it.effect("refuses a destination that already belongs to something else", () =>
    Effect.gen(function* () {
      const { sourceThreadId, projectId } = yield* seedSource("owner");
      const fork = yield* ThreadFork.ThreadFork;
      const forks = forkSession.mock.calls.length;
      const input = forkInput("owner");

      // An ordinary thread that happens to use the destination id.
      yield* dispatch({
        type: "thread.create",
        commandId: CommandId.make("owner-existing"),
        threadId: input.destinationThreadId,
        projectId,
        title: "Unrelated",
        modelSelection: { instanceId: CLAUDE, model: "claude-sonnet-4-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: at(),
      });
      const unrelated = yield* requireThread(input.destinationThreadId);
      expect((yield* failureOf(fork.fork(input))).reason).toBe("destination-conflict");
      expect(yield* requireThread(input.destinationThreadId)).toEqual(unrelated);

      // A fork made by another request, and the source itself.
      const other = forkInput("owner", "owner-other-fork");
      const otherDestination = ThreadId.make("owner-other-destination");
      yield* fork.fork({ ...other, destinationThreadId: otherDestination });
      for (const destinationThreadId of [otherDestination, sourceThreadId]) {
        const error = yield* failureOf(fork.fork({ ...input, destinationThreadId }));
        expect(error.reason).toBe("destination-conflict");
      }

      expect(forkSession.mock.calls.length).toBe(forks + 1);
      expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
    }),
  );

  it.effect("rejects providers without native forks before creating anything", () =>
    Effect.gen(function* () {
      yield* seedSource("unsupported", { instanceId: CURSOR });
      const input = forkInput("unsupported");
      const forks = forkSession.mock.calls.length;

      const error = yield* failureOf((yield* ThreadFork.ThreadFork).fork(input));

      expect(error.reason).toBe("unsupported-provider");
      expect(yield* readThread(input.destinationThreadId)).toBeUndefined();
      expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
      expect(forkSession.mock.calls.length).toBe(forks);
    }),
  );

  it.effect("rejects sources whose history boundary cannot be proven", () =>
    Effect.gen(function* () {
      const fork = yield* ThreadFork.ThreadFork;
      const forks = forkSession.mock.calls.length;
      const expectRejected = Effect.fn("expectRejected")(function* (
        name: string,
        reason: ThreadForkError["reason"],
      ) {
        const input = forkInput(name);
        const error = yield* failureOf(fork.fork(input));
        expect(error.reason).toBe(reason);
        expect(yield* readThread(input.destinationThreadId)).toBeUndefined();
        expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
      });

      yield* seedSource("running", { end: "running" });
      yield* expectRejected("running", "source-not-forkable");

      yield* seedSource("interrupted", { end: "interrupted" });
      yield* expectRejected("interrupted", "source-not-forkable");

      // A thread nothing has run in has no native session to fork.
      yield* seedSource("empty", { turns: 0 });
      yield* expectRejected("empty", "empty-history");

      yield* expectRejected("missing", "source-not-found");
      expect(forkSession.mock.calls.length).toBe(forks);
    }),
  );

  // The destination's creating command fails once, after the native fork is accepted and bound.
  it.effect(
    "shows no thread, and takes no writes, when the history fails after the native fork",
    () =>
      Effect.gen(function* () {
        const { sourceThreadId } = yield* seedSource("partial");
        const input = forkInput("partial");
        const forks = forkSession.mock.calls.length;

        const error = yield* failFirstCreate(input);

        expect(error.reason).toBe("history-failed");
        expect(forkSession.mock.calls.length).toBe(forks + 1);
        expect(yield* readThread(input.destinationThreadId)).toBeUndefined();
        expect(yield* threadEventTypes(input.destinationThreadId)).toEqual([]);
        expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
        const staged = yield* readStagedFork(input.destinationThreadId);
        expect(staged).toMatchObject({
          publishedSequence: null,
          binding: { resumeCursor: { threadId: input.destinationThreadId } },
        });
        // Nothing is there to type into: the half-made fork cannot take a message.
        const write = yield* dispatch({
          type: "thread.message.user.append",
          commandId: CommandId.make("partial-user-write"),
          threadId: input.destinationThreadId,
          message: {
            messageId: MessageId.make("partial-user-message"),
            text: "hi",
            attachments: [],
          },
          createdAt: at(),
        }).pipe(Effect.flip);
        expect(write._tag).toBe("OrchestrationCommandInvariantError");

        // The source starts another turn meanwhile; the retry still finishes the captured history.
        yield* runTurn(sourceThreadId, 3, { end: "running" });
        const result = yield* (yield* ThreadFork.ThreadFork).fork(input);

        expect(result).toEqual({ destinationThreadId: input.destinationThreadId });
        expect(forkSession.mock.calls.length).toBe(forks + 1);
        expect(yield* readBinding(input.destinationThreadId)).toMatchObject(staged.binding);
        expect((yield* readStagedFork(input.destinationThreadId)).publishedSequence).not.toBeNull();
        const destination = yield* requireThread(input.destinationThreadId);
        expect(destination.messages.map((message) => message.text)).toEqual(CAPTURED_TEXTS);
        expect(destination.messages[0]!.attachments).toHaveLength(1);
        expect(yield* threadEventTypes(input.destinationThreadId)).toEqual(FORK_EVENTS);
      }),
  );

  it.effect("returns a finished fork whatever the source does next, and keeps later edits", () =>
    Effect.gen(function* () {
      const { sourceThreadId } = yield* seedSource("done");
      const input = forkInput("done");
      const fork = yield* ThreadFork.ThreadFork;
      const forks = forkSession.mock.calls.length;
      yield* fork.fork(input);
      const binding = yield* readBinding(input.destinationThreadId);
      // The user keeps working in the fork.
      yield* dispatch({
        type: "thread.message.user.append",
        commandId: CommandId.make("done-user-edit"),
        threadId: input.destinationThreadId,
        message: { messageId: MessageId.make("done-user-message"), text: "More", attachments: [] },
        createdAt: at(),
      });
      const edited = yield* requireThread(input.destinationThreadId);
      const events = yield* threadEventTypes(input.destinationThreadId);
      const expectUnchanged = Effect.gen(function* () {
        expect(yield* fork.fork(input)).toEqual({ destinationThreadId: input.destinationThreadId });
        expect(yield* requireThread(input.destinationThreadId)).toEqual(edited);
        expect(yield* readBinding(input.destinationThreadId)).toEqual(binding);
        expect(yield* threadEventTypes(input.destinationThreadId)).toEqual(events);
        expect(forkSession.mock.calls.length).toBe(forks + 1);
      });

      yield* expectUnchanged;
      // The source starts another turn, then is deleted.
      yield* runTurn(sourceThreadId, 3, { end: "running" });
      yield* expectUnchanged;
      yield* dispatch({
        type: "thread.delete",
        commandId: CommandId.make("done-source-delete"),
        threadId: sourceThreadId,
      });
      yield* expectUnchanged;
    }),
  );

  it.effect("fails clearly, touching nothing, when a half-finished fork lost its source", () =>
    Effect.gen(function* () {
      const { sourceThreadId } = yield* seedSource("lost");
      const input = forkInput("lost");
      const forks = forkSession.mock.calls.length;
      yield* failFirstCreate(input);
      const bound = yield* readBinding(input.destinationThreadId);
      yield* dispatch({
        type: "thread.delete",
        commandId: CommandId.make("lost-source-delete"),
        threadId: sourceThreadId,
      });

      const error = yield* failureOf((yield* ThreadFork.ThreadFork).fork(input));

      expect(error.reason).toBe("history-failed");
      expect(error.detail).toContain("no longer available");
      expect(yield* readThread(input.destinationThreadId)).toBeUndefined();
      expect(yield* threadEventTypes(input.destinationThreadId)).toEqual([]);
      expect(yield* readBinding(input.destinationThreadId)).toEqual(bound);
      expect(forkSession.mock.calls.length).toBe(forks + 1);
    }),
  );

  // A turn is requested on the source through the real facade right after the snapshot is taken.
  it.effect("holds a turn requested mid-fork off the source until the native copy is made", () =>
    Effect.gen(function* () {
      const { sourceThreadId } = yield* seedSource("race");
      const input = forkInput("race");
      const provider = yield* ProviderService.ProviderService;
      const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
      let lateTurn: Fiber.Fiber<unknown, unknown> | undefined;
      let sourceReads = 0;
      const racing = ProjectionSnapshotQuery.ProjectionSnapshotQuery.of({
        ...snapshots,
        getThreadDetailById: (threadId, options) =>
          snapshots.getThreadDetailById(threadId, options).pipe(
            Effect.tap(() =>
              // The second read is the snapshot taken inside the source's exclusion.
              threadId === sourceThreadId && ++sourceReads === 2
                ? provider.sendTurn({ threadId: sourceThreadId, input: "arrives mid-fork" }).pipe(
                    Effect.forkDetach,
                    Effect.tap((fiber) => Effect.sync(() => void (lateTurn = fiber))),
                    // Plenty of scheduler turns for an unguarded send to reach Claude.
                    Effect.andThen(
                      Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow),
                    ),
                  )
                : Effect.void,
            ),
          ),
      });
      const scope = yield* Scope.make();
      const raced = yield* Layer.build(Layer.fresh(ThreadFork.layer)).pipe(
        Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, racing),
        Scope.provide(scope),
      );
      const queries = createQuery.mock.calls.length;

      const result = yield* ThreadFork.ThreadFork.pipe(
        Effect.flatMap((fork) => fork.fork(input)),
        Effect.provide(raced),
      );
      yield* Fiber.await(lateTurn!);
      yield* Scope.close(scope, Exit.void);

      expect(result).toEqual({ destinationThreadId: input.destinationThreadId });
      // The late turn reached Claude, but only after the native session was copied.
      expect(createQuery.mock.calls.length).toBe(queries + 1);
      expect(forkSession.mock.invocationCallOrder.at(-1)!).toBeLessThan(
        createQuery.mock.invocationCallOrder.at(-1)!,
      );
      expect(forkSession.mock.lastCall?.[0]).toBe(SOURCE_SESSION);
      const destination = yield* requireThread(input.destinationThreadId);
      expect(destination.messages.map((message) => message.text)).toEqual(CAPTURED_TEXTS);
    }),
  );
});

// A file database closed and opened again: the fork's recovery record has to survive on disk.
it.effect("finishes an interrupted fork from the database file after a restart", () =>
  Effect.gen(function* () {
    const configScope = yield* Scope.make();
    const configServices = yield* Layer.build(
      serverConfig.pipe(Layer.provide(NodeServices.layer)),
    ).pipe(Scope.provide(configScope));
    const config = yield* ServerConfig.pipe(Effect.provide(configServices));
    const stack = () =>
      Layer.fresh(
        makeForkLayer(
          makeSqlitePersistenceLive(config.dbPath).pipe(Layer.provide(NodeServices.layer)),
          Layer.succeed(ServerConfig, config),
        ),
      );
    const input = forkInput("reopen");
    const forks = forkSession.mock.calls.length;

    const firstScope = yield* Scope.make();
    const first = yield* Layer.build(stack()).pipe(Scope.provide(firstScope));
    const { sourceThreadId } = yield* seedSource("reopen").pipe(Effect.provide(first));
    const firstEngine = yield* OrchestrationEngine.OrchestrationEngineService.pipe(
      Effect.provide(first),
    );
    const failing = yield* Layer.build(Layer.fresh(ThreadFork.layer)).pipe(
      Effect.provideService(
        OrchestrationEngine.OrchestrationEngineService,
        failingCreateOnce(firstEngine),
      ),
      Effect.provide(first),
      Scope.provide(firstScope),
    );
    const error = yield* failureOf(
      ThreadFork.ThreadFork.pipe(
        Effect.flatMap((fork) => fork.fork(input)),
        Effect.provide(failing),
      ),
    );
    expect(error.reason).toBe("history-failed");
    const staged = yield* readStagedFork(input.destinationThreadId).pipe(Effect.provide(first));
    expect(staged.publishedSequence).toBeNull();
    expect(
      yield* readBinding(input.destinationThreadId).pipe(Effect.provide(first)),
    ).toBeUndefined();
    expect(
      yield* readThread(input.destinationThreadId).pipe(Effect.provide(first)),
    ).toBeUndefined();
    // The source starts another turn before the server goes down.
    yield* runTurn(sourceThreadId, 3, { end: "running" }).pipe(Effect.provide(first));
    yield* Scope.close(firstScope, Exit.void);
    expect(NodeFS.statSync(config.dbPath).size).toBeGreaterThan(0);

    const secondScope = yield* Scope.make();
    const second = yield* Layer.build(stack()).pipe(Scope.provide(secondScope));
    const finish = Effect.gen(function* () {
      expect(yield* readBinding(input.destinationThreadId)).toBeUndefined();
      expect(yield* readStagedFork(input.destinationThreadId)).toEqual(staged);
      expect(yield* readThread(input.destinationThreadId)).toBeUndefined();

      const result = yield* (yield* ThreadFork.ThreadFork).fork(input);

      expect(result).toEqual({ destinationThreadId: input.destinationThreadId });
      expect(forkSession.mock.calls.length).toBe(forks + 1);
      expect(yield* readBinding(input.destinationThreadId)).toMatchObject(staged.binding);
      expect((yield* readStagedFork(input.destinationThreadId)).publishedSequence).not.toBeNull();
      const destination = yield* requireThread(input.destinationThreadId);
      expect(destination.messages.map((message) => message.text)).toEqual(CAPTURED_TEXTS);
      expect(yield* threadEventTypes(input.destinationThreadId)).toEqual(FORK_EVENTS);
    });
    yield* finish.pipe(Effect.provide(second));
    yield* Scope.close(secondScope, Exit.void);
    yield* Scope.close(configScope, Exit.void);
  }),
);

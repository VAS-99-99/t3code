// @effect-diagnostics nodeBuiltinImport:off
/**
 * Test server for provider recovery journeys. Everything the recovery path
 * touches is real: the file-backed SQLite store, the orchestration engine and
 * projections, runtime ingestion, ProviderService with its session directory
 * and the intent store. Only the model adapters (scriptable fakes) and the
 * provider registry's quota snapshots are scripted.
 *
 * @module orchestration/providerRecoveryHarness.testUtils
 */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import * as ProviderRecoveryIntents from "../persistence/ProviderRecoveryIntents.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { makeProviderServiceLive } from "../provider/Layers/ProviderService.ts";
import * as ProviderEventLoggers from "../provider/Layers/ProviderEventLoggers.ts";
import { ProviderSessionDirectoryLive } from "../provider/Layers/ProviderSessionDirectory.ts";
import type { ProviderAdapterError } from "../provider/Errors.ts";
import type { ProviderAdapterShape } from "../provider/Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../provider/Services/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import {
  makeFakeProviderAdapter,
  makeMultiInstanceRegistry,
  type FakeProviderAdapter,
} from "../provider/testUtils/fakeProviderAdapter.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { ProviderRuntimeIngestionLive } from "./Layers/ProviderRuntimeIngestion.ts";
import * as ProviderRecoveryReactor from "./ProviderRecoveryReactor.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { ProviderRuntimeIngestionService } from "./Services/ProviderRuntimeIngestion.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";

export const T0 = Date.parse("2026-10-09T10:00:00.000Z");
export const HOUR = 3_600_000;
export const MODEL = "claude-sonnet-5-5";
export const CLAUDE = ProviderDriverKind.make("claudeAgent");
export const CODEX = ProviderDriverKind.make("codex");
export const A = ProviderInstanceId.make("claude-a");
export const B = ProviderInstanceId.make("claude-b");
export const C = ProviderInstanceId.make("claude-c");
export const D = ProviderInstanceId.make("codex-d");
export const PROJECT = ProjectId.make("project-1");

/** What an account's provider snapshot reports. Percent is per window id. */
export interface Quota {
  readonly windows: ReadonlyArray<{
    readonly id: "five_hour" | "seven_day";
    readonly usedPercent: number;
    /** Epoch ms the window reopens; omitted when the provider reports none. */
    readonly resetsAt?: number | undefined;
  }>;
  /** `stale` reads older than the freshness limit; `failed` is a failed probe. */
  readonly read: "fresh" | "stale" | "failed";
}

/** State that outlives a server process: the database file, settings and quota. */
export interface World {
  readonly dbPath: string;
  readonly cwd: string;
  readonly quota: Map<ProviderInstanceId, Quota>;
  readonly enabled: Map<ProviderInstanceId, boolean>;
  readonly models: Map<ProviderInstanceId, ReadonlyArray<string>>;
  /** Signed-in address each account reports; unset means the provider says nothing. */
  readonly emails: Map<ProviderInstanceId, string>;
  readonly probes: Array<ProviderInstanceId>;
}

export const makeWorld = (): World => {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-recovery-"));
  const cwd = NodePath.join(root, "workspace");
  NodeFS.mkdirSync(cwd, { recursive: true });
  return {
    dbPath: NodePath.join(root, "state.sqlite"),
    cwd,
    quota: new Map(),
    enabled: new Map(),
    models: new Map(),
    emails: new Map(),
    probes: [],
  };
};

export const dropWorld = (world: World) =>
  NodeFS.rmSync(NodePath.dirname(world.dbPath), { recursive: true, force: true });

export const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));

const providerSnapshot = (world: World, id: ProviderInstanceId, now: number): ServerProvider => {
  const quota = world.quota.get(id);
  const checkedAt = quota?.read === "stale" ? iso(now - 30 * 60_000) : iso(now);
  const usageLimits =
    quota === undefined
      ? undefined
      : quota.read === "failed"
        ? { checkedAt, windows: [], unavailable: { reason: "probeFailed" as const } }
        : {
            checkedAt,
            windows: quota.windows.map((window) => ({
              id: window.id,
              kind: window.id === "five_hour" ? ("session" as const) : ("weekly" as const),
              label: window.id,
              usedPercent: window.usedPercent,
              ...(window.resetsAt === undefined ? {} : { resetsAt: iso(window.resetsAt) }),
            })),
          };
  return {
    instanceId: id,
    driver: id === D ? CODEX : CLAUDE,
    displayName: String(id),
    enabled: world.enabled.get(id) ?? true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: {
      status: "authenticated",
      ...(world.emails.has(id) ? { email: world.emails.get(id)! } : {}),
    },
    checkedAt: iso(now),
    models: (world.models.get(id) ?? [MODEL]).map((slug) => ({
      slug,
      name: slug,
      isCustom: false,
      capabilities: null,
    })),
    slashCommands: [],
    skills: [],
    ...(usageLimits ? { usageLimits } : {}),
  };
};

export interface Server {
  readonly world: World;
  readonly fakes: Readonly<Record<string, FakeProviderAdapter>>;
  readonly providerService: ProviderService.ProviderService["Service"];
  readonly dispatch: (command: OrchestrationCommand) => Effect.Effect<unknown>;
  /** Creates the project (once) and a thread bound to `instanceId` with a live session. */
  readonly createThread: (
    threadId: string,
    options?: {
      readonly instanceId?: ProviderInstanceId;
      readonly automaticRecovery?: boolean;
    },
  ) => Effect.Effect<ThreadId>;
  /** Sends one real turn, the way the command reactor would. */
  readonly startTurn: (threadId: ThreadId, input?: string) => Effect.Effect<unknown>;
  /** Ends the live turn as a failure: runtime error first, then the failed completion. */
  readonly failTurn: (
    threadId: ThreadId,
    message: string,
    errorClass?: "transport_error" | "provider_error",
  ) => Effect.Effect<void>;
  readonly completeTurn: (threadId: ThreadId) => Effect.Effect<void>;
  /** The adapter reports a rejected window on the thread's live account. */
  readonly rejectWindow: (
    threadId: ThreadId,
    window: { readonly id: "five_hour" | "seven_day"; readonly resetsAt?: number },
  ) => Effect.Effect<void>;
  /** Lets in-flight fibers and queues finish; never advances the clock. */
  readonly settle: Effect.Effect<void>;
  /** Moves the test clock, then settles. */
  readonly advance: (ms: number) => Effect.Effect<void>;
  readonly intent: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<ProviderRecoveryIntents.ProviderRecoveryIntent>>;
  readonly recoveryRows: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<{ readonly summary: string; readonly phase: string }>>;
  readonly binding: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<ProviderSessionDirectory.ProviderRuntimeBinding>>;
  readonly shell: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Option.Option<{ readonly instanceId: string; readonly automaticRecovery?: boolean | undefined }>
  >;
  /** Stops this process: timers, fibers and the database connection go away. */
  readonly close: Effect.Effect<void>;
}

export const startServer = (
  world: World,
  options?: {
    readonly at?: number;
    /** Real adapters to route an instance to instead of its scriptable fake. */
    readonly adapters?: Partial<Record<string, ProviderAdapterShape<ProviderAdapterError>>>;
  },
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(options?.at ?? T0);
    const scope = yield* Scope.make();
    const fakes = {
      [A]: makeFakeProviderAdapter({ provider: CLAUDE, instanceId: A }),
      [B]: makeFakeProviderAdapter({ provider: CLAUDE, instanceId: B }),
      [C]: makeFakeProviderAdapter({ provider: CLAUDE, instanceId: C }),
      [D]: makeFakeProviderAdapter({ provider: CODEX, instanceId: D, promptless: true }),
    } as const;
    const registry = makeMultiInstanceRegistry(
      Object.values(fakes).map((fake) => ({
        instanceId: fake.instanceId,
        adapter: options?.adapters?.[fake.instanceId] ?? fake.adapter,
      })),
      world.enabled,
    );

    const sqlite = makeSqlitePersistenceLive(world.dbPath).pipe(Layer.provide(NodeServices.layer));
    const engineLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(sqlite),
    );
    const snapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(sqlite),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(ProviderSessionRuntime.layer),
      Layer.provide(sqlite),
    );
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(NodeServices.layer),
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
      Layer.provide(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );
    const registryMock: ProviderRegistry.ProviderRegistryShape = {
      getProviders: Effect.map(Clock.currentTimeMillis, (now) =>
        [A, B, C, D].map((id) => providerSnapshot(world, id, now)),
      ),
      refresh: () => Effect.succeed([]),
      refreshInstance: (id) =>
        Effect.suspend(() => {
          world.probes.push(id);
          return Effect.map(Clock.currentTimeMillis, (now) =>
            [A, B, C, D].map((entry) => providerSnapshot(world, entry, now)),
          );
        }),
      refreshWorkspaceSnapshot: () => Effect.succeed([]),
      getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
      setProviderMaintenanceActionState: () => Effect.succeed([]),
      streamChanges: Stream.empty,
    };
    const checkpointStore = Layer.effect(
      CheckpointStore.CheckpointStore,
      Effect.map(CheckpointStore.CheckpointStore, (store) => ({
        ...store,
        isGitRepository: () => Effect.succeed(false),
      })),
    ).pipe(Layer.provide(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))));

    const layer = Layer.mergeAll(ProviderRuntimeIngestionLive, ProviderRecoveryReactor.layer).pipe(
      Layer.provideMerge(ProviderRecoveryIntents.layer),
      Layer.provideMerge(Layer.succeed(ProviderRegistry.ProviderRegistry, registryMock)),
      Layer.provideMerge(providerLayer),
      Layer.provideMerge(directoryLayer),
      Layer.provideMerge(engineLayer),
      Layer.provideMerge(snapshotLayer),
      Layer.provideMerge(ThreadBackgroundLiveness.layer),
      Layer.provideMerge(ThreadPlanProgress.layer),
      Layer.provideMerge(ServerSettings.ServerSettingsService.layerTest()),
      Layer.provideMerge(checkpointStore),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
      Layer.provideMerge(sqlite),
      Layer.provideMerge(NodeServices.layer),
    );
    const context = yield* Layer.build(layer).pipe(Scope.provide(scope));
    const run = <A, E>(effect: Effect.Effect<A, E, never> | Effect.Effect<A, E, any>) =>
      (effect as Effect.Effect<A, E, never>).pipe(Effect.provide(context));

    const engine = yield* run(Effect.service(OrchestrationEngineService));
    const query = yield* run(Effect.service(ProjectionSnapshotQuery));
    const ingestion = yield* run(Effect.service(ProviderRuntimeIngestionService));
    const reactor = yield* run(Effect.service(ProviderRecoveryReactor.ProviderRecoveryReactor));
    const providerService = yield* run(Effect.service(ProviderService.ProviderService));
    const directory = yield* run(Effect.service(ProviderSessionDirectory.ProviderSessionDirectory));
    const intents = yield* run(Effect.service(ProviderRecoveryIntents.ProviderRecoveryIntents));

    yield* ingestion.start().pipe(Scope.provide(scope));
    yield* reactor.start().pipe(Scope.provide(scope));

    const yields = (count: number) =>
      Effect.forEach(Array.from({ length: count }), () => Effect.yieldNow, { discard: true });
    // Waits on the worker drains. A job blocked on purpose (a crash scenario) must not
    // hang the test, so each drain is raced against a bounded number of yields.
    const settle = Effect.gen(function* () {
      for (let round = 0; round < 60; round++) {
        yield* Effect.yieldNow;
        yield* Effect.raceFirst(Effect.andThen(ingestion.drain, reactor.drain), yields(20));
      }
    });
    const liveFake = (threadId: ThreadId) =>
      Object.values(fakes).find((fake) => fake.sessions.has(threadId));

    const server: Server = {
      world,
      fakes,
      providerService,
      dispatch: (command) => engine.dispatch(command).pipe(Effect.orDie),
      createThread: (name, options) =>
        Effect.gen(function* () {
          const threadId = ThreadId.make(name);
          const instanceId = options?.instanceId ?? A;
          const existingProject = yield* query.getProjectShellById(PROJECT);
          if (Option.isNone(existingProject)) {
            yield* engine.dispatch({
              type: "project.create",
              commandId: CommandId.make(`cmd-project-${name}`),
              projectId: PROJECT,
              title: "Recovery",
              workspaceRoot: world.cwd,
              createdAt: iso(T0),
            });
          }
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`cmd-thread-${name}`),
            threadId,
            projectId: PROJECT,
            title: name,
            modelSelection: { instanceId, model: MODEL },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: null,
            createdAt: iso(T0),
            ...(options?.automaticRecovery === undefined
              ? {}
              : { automaticRecovery: options.automaticRecovery }),
          });
          yield* run(
            providerService.startSession(threadId, {
              threadId,
              provider: instanceId === D ? CODEX : CLAUDE,
              providerInstanceId: instanceId,
              cwd: world.cwd,
              modelSelection: { instanceId, model: MODEL },
              runtimeMode: "full-access",
            }),
          );
          return threadId;
        }).pipe(Effect.orDie),
      startTurn: (threadId, input = "do the work") =>
        run(providerService.sendTurn({ threadId, input, interactionMode: "default" })).pipe(
          Effect.orDie,
        ),
      failTurn: (threadId, message, errorClass = "transport_error") =>
        Effect.gen(function* () {
          const fake = liveFake(threadId);
          if (!fake) return yield* Effect.die(new Error(`no live session for ${threadId}`));
          fake.emit({ type: "runtime.error", threadId, payload: { message, class: errorClass } });
          fake.settleTurn(threadId, "failed", message);
          yield* settle;
        }),
      completeTurn: (threadId) =>
        Effect.gen(function* () {
          liveFake(threadId)?.settleTurn(threadId, "completed");
          yield* settle;
        }),
      rejectWindow: (threadId, window) =>
        Effect.gen(function* () {
          const fake = liveFake(threadId);
          if (!fake) return yield* Effect.die(new Error(`no live session for ${threadId}`));
          fake.emit({
            type: "account.rate-limits.updated",
            threadId,
            payload: {
              limits: {
                blocked: true,
                windows: [
                  {
                    id: window.id,
                    kind: window.id === "five_hour" ? "session" : "weekly",
                    label: window.id,
                    usedPercent: 100,
                    ...(window.resetsAt === undefined ? {} : { resetsAt: iso(window.resetsAt) }),
                  },
                ],
              },
            },
          });
          yield* settle;
        }),
      settle,
      // The reactor sleeps in short slices, so the clock moves in slices too: each
      // one gives the timer fiber a chance to re-arm before the next.
      advance: (ms) =>
        Effect.gen(function* () {
          for (let left = ms; ;) {
            const step = Math.min(left, 5 * 60_000);
            yield* TestClock.adjust(`${step} millis`);
            yield* yields(40);
            left -= step;
            if (left <= 0) break;
          }
          yield* settle;
        }),
      intent: (threadId) => run(intents.get(threadId)).pipe(Effect.orDie),
      recoveryRows: (threadId) =>
        run(query.getThreadDetailById(threadId)).pipe(
          Effect.orDie,
          Effect.map((detail) =>
            Option.isNone(detail)
              ? []
              : detail.value.activities
                  .filter(
                    (activity) => activity.kind === ProviderRecoveryReactor.RECOVERY_ACTIVITY_KIND,
                  )
                  .map((activity) => ({
                    summary: activity.summary,
                    phase: String((activity.payload as { phase?: unknown }).phase),
                  })),
          ),
        ),
      binding: (threadId) => run(directory.getBinding(threadId)).pipe(Effect.orDie),
      shell: (threadId) =>
        run(query.getThreadShellById(threadId)).pipe(
          Effect.orDie,
          Effect.map(
            Option.map((thread) => ({
              instanceId: String(thread.modelSelection.instanceId),
              automaticRecovery: thread.automaticRecovery,
            })),
          ),
        ),
      close: Scope.close(scope, Exit.void),
    };
    return server;
  });

/**
 * ProviderRecoveryReactor - resumes a thread's stopped turn after a provider
 * outage or an exhausted usage window, without replaying what already ran.
 *
 * A failed turn on an opted-in thread becomes a persisted intent
 * (`provider_recovery_intents`): what failed, on which account, and when to
 * look again. A timer wakes it; the wake reconciles persisted turn state and
 * the provider's own history first, then either resumes the same session or
 * moves the conversation to another enabled account of the same driver whose
 * fresh quota allows it. Deleting the intent is cancellation, and it also
 * fences any timer that fires late.
 *
 * Status shows through `provider.recovery` activity rows. Stop, a new
 * message, deletion, archiving or turning the thread's flag off cancel the intent.
 *
 * @module orchestration/ProviderRecoveryReactor
 */
import {
  CommandId,
  EventId,
  type ModelSelection,
  type OrchestrationEvent,
  type OrchestrationThreadShell,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as NodeUtil from "node:util";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ProviderRecoveryIntents from "../persistence/ProviderRecoveryIntents.ts";
import { USAGE_LIMIT_EXCEEDED_CODE } from "../provider/Layers/codexUsageLimits.ts";
import {
  backoffDelayMs,
  chooseRecoveryAccount,
  classifyFailure,
  QUOTA_UNKNOWN_RECHECK_MS,
  RECOVERY_MAX_ATTEMPTS,
  type RecoveryAccountDecision,
} from "../provider/providerRecoveryPolicy.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import { forkParked } from "../serverActivation.ts";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";

type ProviderRecoveryIntent = ProviderRecoveryIntents.ProviderRecoveryIntent;

/** Activity kind every status row uses; `payload.phase` says which. */
export const RECOVERY_ACTIVITY_KIND = "provider.recovery";
const CONTINUE_PROMPT = "Continue where you left off.";
/**
 * Timers sleep in short slices. A laptop that slept through the reset would otherwise wake
 * its monotonic timer late by the length of the sleep; a slice bounds that to minutes.
 */
const MAX_SLEEP_MS = 10 * 60_000;
/** Tool kinds that act outside the conversation; reading kinds are safe to repeat. */
const EXTERNAL_TOOL_TYPES = new Set([
  "command_execution",
  "file_change",
  "mcp_tool_call",
  "dynamic_tool_call",
  "collab_agent_tool_call",
]);
const IN_FLIGHT_STATUS = /^(?:in[_-]?progress|running|pending)$/i;

export class ProviderRecoveryReactor extends Context.Service<
  ProviderRecoveryReactor,
  {
    /** Restores persisted intents, then reacts to provider and orchestration events. */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    /** Resolves when queued work is done. Tests only. */
    readonly drain: Effect.Effect<void>;
  }
>()("t3/orchestration/ProviderRecoveryReactor") {}

type Job =
  | { readonly _tag: "runtime"; readonly event: ProviderRuntimeEvent }
  | { readonly _tag: "wake"; readonly threadId: ThreadId; readonly token: string };

/** What the adapter reported about the windows it saw run out during the failed turn. */
interface QuotaEvidence {
  readonly windows: ReadonlyArray<{
    readonly id: string;
    readonly kind: "session" | "weekly" | "monthly" | "other";
    readonly label: string;
    readonly usedPercent: number;
    readonly resetsAt?: string | undefined;
  }>;
}

const formatWait = (ms: number): string => {
  if (ms < 60_000) return `${Math.max(1, Math.round(ms / 1000))}s`;
  const minutes = Math.round(ms / 60_000);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return minutes % 60 === 0 ? `${hours}h` : `${hours}h ${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return hours % 24 === 0 ? `${days}d` : `${days}d ${hours % 24}h`;
};

const hasInFlightItem = (items: ReadonlyArray<unknown>): boolean =>
  items.some(
    (item) =>
      typeof item === "object" &&
      item !== null &&
      typeof (item as { status?: unknown }).status === "string" &&
      IN_FLIGHT_STATUS.test((item as { status: string }).status),
  );

/** What the provider reports about who is signed in; undefined when it says nothing. */
const accountIdentity = (provider: ServerProvider): string | undefined =>
  provider.usageLimits?.credentialFingerprint ?? provider.auth.profileId ?? provider.auth.email;

const messageOf = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : String(error);
};

export const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const providerService = yield* ProviderService.ProviderService;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const intents = yield* ProviderRecoveryIntents.ProviderRecoveryIntents;
  const crypto = yield* Crypto.Crypto;

  const timers = new Map<ThreadId, Fiber.Fiber<void>>();
  /** Per-turn evidence from the stream; dropped when the next turn starts or this one is settled. */
  const lastError = new Map<ThreadId, { readonly message: string; readonly transport: boolean }>();
  const quotaEvidence = new Map<ThreadId, QuotaEvidence>();
  /** A parked window re-reports itself as its wait shrinks; one look per window is enough. */
  const lastQuotaLook = new Map<ThreadId, { readonly key: string; readonly at: number }>();
  /** The turn this process started for a resuming intent; other turns' endings are stale. */
  const attemptTurns = new Map<ThreadId, string>();
  /** Bumped by every cancel so a job that started earlier cannot re-arm what was cancelled. */
  const cancelEpoch = new Map<ThreadId, number>();
  let reactorScope: Scope.Scope | undefined;
  let rowCounter = 0;
  // Set once the worker exists; the worker's jobs and the timers that feed it refer to each other.
  let enqueueWake: (threadId: ThreadId, token: string) => Effect.Effect<void> = () => Effect.void;

  const nowMs = Clock.currentTimeMillis;
  const iso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
  const epochOf = (threadId: ThreadId) => cancelEpoch.get(threadId) ?? 0;
  const warn = (message: string, threadId: ThreadId) => (cause: Cause.Cause<unknown>) =>
    Cause.hasInterruptsOnly(cause)
      ? Effect.interrupt
      : Effect.logWarning(message, { threadId, cause: Cause.pretty(cause) });

  const announce = (
    threadId: ThreadId,
    tone: "info" | "error",
    summary: string,
    payload: Record<string, unknown>,
  ) =>
    Effect.gen(function* () {
      const now = yield* nowMs;
      const createdAt = iso(now);
      // Rows sort by time, then id: a counter keeps rows announced in the same
      // millisecond in the order they were announced.
      const id = EventId.make(
        `${String(now).padStart(15, "0")}-${String(++rowCounter).padStart(6, "0")}-${yield* crypto.randomUUIDv4}`,
      );
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make(`server:provider-recovery:${id}`),
        threadId,
        activity: {
          id,
          tone,
          kind: RECOVERY_ACTIVITY_KIND,
          summary,
          payload,
          turnId: null,
          createdAt,
        },
        createdAt,
      });
    }).pipe(Effect.catchCause(warn("provider recovery status row failed", threadId)));

  /** Only a live thread that opted in is ever recovered. */
  const recoverable = (threadId: ThreadId) =>
    snapshots.getThreadShellById(threadId).pipe(
      Effect.orElseSucceed(() => Option.none<OrchestrationThreadShell>()),
      Effect.map((shell) => Option.filter(shell, (thread) => thread.automaticRecovery === true)),
    );

  const disarm = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const timer = timers.get(threadId);
      timers.delete(threadId);
      if (timer) yield* Fiber.interrupt(timer);
    });

  const forgetRuntime = (threadId: ThreadId) => {
    lastError.delete(threadId);
    quotaEvidence.delete(threadId);
    lastQuotaLook.delete(threadId);
    attemptTurns.delete(threadId);
  };

  /** Cancellation runs outside the worker, so it also beats a wake that is already queued. */
  const cancel = (threadId: ThreadId, input: { readonly interrupt: boolean }) =>
    Effect.gen(function* () {
      cancelEpoch.set(threadId, epochOf(threadId) + 1);
      yield* disarm(threadId);
      const existing = Option.getOrUndefined(yield* intents.get(threadId));
      const removed = yield* intents.remove(threadId);
      forgetRuntime(threadId);
      if (!removed) return;
      yield* announce(threadId, "info", "Automatic recovery cancelled.", { phase: "cancelled" });
      if (input.interrupt && existing?.status === "resuming") {
        // The continuation may already be running; Stop must reach it too.
        yield* providerService
          .interruptTurn({ threadId })
          .pipe(Effect.catchCause(warn("provider recovery interrupt failed", threadId)));
      }
    }).pipe(Effect.catchCause(warn("provider recovery cancel failed", threadId)));

  /** Gives up on an intent and says why, without retrying. */
  const stopRecovery = (
    threadId: ThreadId,
    token: string | undefined,
    summary: string,
    phase: string,
  ) =>
    Effect.gen(function* () {
      yield* disarm(threadId);
      const removed = yield* intents.remove(threadId, token);
      forgetRuntime(threadId);
      if (removed || token === undefined) {
        yield* announce(threadId, "error", summary, { phase });
      }
    });

  const gaveUp = (threadId: ThreadId, token: string | undefined) =>
    stopRecovery(
      threadId,
      token,
      `Gave up after ${RECOVERY_MAX_ATTEMPTS} automatic attempts. Send a message to continue.`,
      "gave-up",
    );

  const arm = (intent: ProviderRecoveryIntent): Effect.Effect<void> =>
    Effect.gen(function* () {
      yield* disarm(intent.threadId);
      if (reactorScope === undefined) return;
      const now = yield* nowMs;
      const target =
        intent.wakeAt === null ? now + QUOTA_UNKNOWN_RECHECK_MS : Date.parse(intent.wakeAt);
      const delay = Math.min(MAX_SLEEP_MS, Math.max(0, target - now));
      const fiber = yield* Effect.sleep(Duration.millis(delay)).pipe(
        Effect.andThen(Effect.suspend(() => enqueueWake(intent.threadId, intent.token))),
        Effect.forkIn(reactorScope),
      );
      timers.set(intent.threadId, fiber);
    });

  /**
   * Persists and arms one intent unless the thread was cancelled since `epoch`. Callers take
   * `epoch` when they begin the work being retried, never when they schedule its follow-up.
   */
  const schedule = (epoch: number, intent: ProviderRecoveryIntent) =>
    Effect.gen(function* () {
      if (epochOf(intent.threadId) !== epoch) return false;
      yield* intents.put(intent);
      if (epochOf(intent.threadId) !== epoch) {
        // A cancel landed while the row was being written.
        yield* intents.remove(intent.threadId, intent.token);
        return false;
      }
      yield* arm(intent);
      return true;
    });

  const newIntent = (
    thread: OrchestrationThreadShell,
    fields: {
      readonly kind: ProviderRecoveryIntent["kind"];
      readonly instanceId: ProviderInstanceId;
      readonly reason: string;
      readonly attempt: number;
      readonly wakeAt: string | null;
      readonly turnId: string | null;
      readonly targetInstanceId?: ProviderInstanceId | null;
      readonly excluded?: ProviderRecoveryIntent["excluded"] | undefined;
      readonly createdAt?: string | undefined;
    },
  ) =>
    Effect.gen(function* () {
      const now = iso(yield* nowMs);
      const binding = yield* directory
        .getBinding(thread.id)
        .pipe(Effect.orElseSucceed(() => Option.none()));
      return {
        threadId: thread.id,
        token: yield* crypto.randomUUIDv4,
        kind: fields.kind,
        status: "waiting",
        attempt: fields.attempt,
        wakeAt: fields.wakeAt,
        turnId: fields.turnId,
        instanceId: fields.instanceId,
        sessionIdentity: Option.isSome(binding) ? (binding.value.resumeCursor ?? null) : null,
        targetInstanceId: fields.targetInstanceId ?? null,
        stagedCursor: null,
        excluded: fields.excluded ?? [],
        reason: fields.reason,
        createdAt: fields.createdAt ?? now,
        updatedAt: now,
      } satisfies ProviderRecoveryIntent;
    });

  // --- account choice -------------------------------------------------------

  const providerLabel = (providers: ReadonlyArray<ServerProvider>, id: ProviderInstanceId) =>
    providers.find((provider) => provider.instanceId === id)?.displayName ?? String(id);

  /**
   * Fresh quota for the current account and every enabled sibling that offers
   * the thread's model. Each is probed first: a stale or failed read never
   * counts as headroom, and the policy treats it as unknown.
   */
  const decideAccount = (input: {
    readonly thread: OrchestrationThreadShell;
    readonly current: ProviderInstanceId;
    readonly blocked: boolean;
    readonly evidence?: QuotaEvidence | undefined;
    readonly excluded: ReadonlyArray<ProviderInstanceId>;
  }) =>
    Effect.gen(function* () {
      const model = input.thread.modelSelection.model;
      const listed = yield* providerRegistry.getProviders;
      const currentSnapshot = listed.find((provider) => provider.instanceId === input.current);
      const currentIdentity = currentSnapshot ? accountIdentity(currentSnapshot) : undefined;
      const siblings = listed.filter(
        (provider) =>
          provider.instanceId !== input.current &&
          // Two configurations signed in as one account share a quota: no other account.
          (currentIdentity === undefined || accountIdentity(provider) !== currentIdentity) &&
          provider.driver === currentSnapshot?.driver &&
          provider.enabled &&
          provider.installed &&
          provider.status !== "error" &&
          provider.status !== "disabled" &&
          provider.availability !== "unavailable" &&
          provider.models.some((entry) => entry.slug === model) &&
          !input.excluded.includes(provider.instanceId),
      );
      yield* Effect.forEach(
        [input.current, ...siblings.map((provider) => provider.instanceId)],
        (instanceId) => providerRegistry.refreshInstance(instanceId),
        { concurrency: 4, discard: true },
      );
      const fresh = yield* providerRegistry.getProviders;
      const limitsOf = (instanceId: ProviderInstanceId) =>
        fresh.find((provider) => provider.instanceId === instanceId)?.usageLimits;
      const now = yield* nowMs;
      const currentLimits = limitsOf(input.current);
      // Windows the adapter just saw run out outrank the snapshot's older bars;
      // each carries its own reset, so a window that has since reset reads as open.
      const seen = input.evidence?.windows ?? [];
      const merged =
        seen.length === 0
          ? currentLimits
          : {
              checkedAt: iso(now),
              windows: [
                ...(currentLimits?.windows ?? []).filter(
                  (window) => !seen.some((entry) => entry.id === window.id),
                ),
                ...seen,
              ],
            };
      return chooseRecoveryAccount({
        current: { instanceId: input.current, usageLimits: merged },
        others: siblings.map((provider) => ({
          instanceId: provider.instanceId,
          usageLimits: limitsOf(provider.instanceId),
        })),
        now,
        currentBlocked: input.blocked,
      });
    });

  // --- reconciliation -------------------------------------------------------

  /** Reasons the stopped turn cannot be continued without risking a repeated action. */
  const uncertainty = (thread: OrchestrationThreadShell, turnId: string | null) =>
    Effect.gen(function* () {
      const forTurn = turnId ?? thread.latestTurn?.turnId ?? null;
      const detail = yield* snapshots
        .getThreadDetailById(thread.id)
        .pipe(Effect.orElseSucceed(() => Option.none()));
      if (Option.isNone(detail)) return "its recorded activity could not be read";
      const unfinished = new Set<string>();
      for (const activity of Option.isSome(detail) ? detail.value.activities : []) {
        if (forTurn === null || activity.turnId !== forTurn) continue;
        const payload = (activity.payload ?? {}) as { toolCallId?: string; itemType?: string };
        if (!payload.toolCallId || !EXTERNAL_TOOL_TYPES.has(payload.itemType ?? "")) continue;
        if (activity.kind === "tool.started") unfinished.add(payload.toolCallId);
        if (activity.kind === "tool.completed") unfinished.delete(payload.toolCallId);
      }
      if (unfinished.size > 0) return "a tool call started but its result was never recorded";
      // The provider's own record must be readable and must not end mid-item.
      const native = yield* providerService.readConversation(thread.id).pipe(Effect.exit);
      if (Exit.isFailure(native)) {
        if (Cause.hasInterruptsOnly(native.cause)) return yield* Effect.interrupt;
        return `its history could not be read (${messageOf(native.cause)})`;
      }
      const last = native.value.turns.at(-1);
      return last !== undefined && hasInFlightItem(last.items)
        ? "the provider's own history ends inside an unfinished item"
        : undefined;
    });

  const awaitIdle = (threadId: ThreadId) =>
    providerService.listSessions().pipe(
      Effect.flatMap((sessions) => {
        const live = sessions.find((session) => session.threadId === threadId);
        return live !== undefined && (live.status !== "ready" || live.activeTurnId != null)
          ? Effect.fail("busy" as const)
          : Effect.void;
      }),
      Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
      Effect.orElseSucceed(() => undefined),
    );

  const continueTurn = (thread: OrchestrationThreadShell, instanceId: ProviderInstanceId) =>
    Effect.gen(function* () {
      const capabilities = yield* providerService.getCapabilities(instanceId);
      return yield* providerService.sendTurn({
        threadId: thread.id,
        ...(capabilities.promptlessTurnContinuation === true
          ? { continuation: true }
          : { input: CONTINUE_PROMPT }),
        interactionMode: thread.interactionMode,
      });
    });

  const announceQuotaWait = (threadId: ThreadId, wakeAt: string | null, now: number) =>
    wakeAt === null
      ? announce(
          threadId,
          "info",
          "Usage limit reached. The provider did not say when it resets; checking again every 5 minutes.",
          { phase: "quota-wait", resumeAt: null, nextCheckAt: iso(now + QUOTA_UNKNOWN_RECHECK_MS) },
        )
      : announce(
          threadId,
          "info",
          `Usage limit reached. Resuming when it resets in ${formatWait(Date.parse(wakeAt) - now)}.`,
          { phase: "quota-wait", resumeAt: wakeAt },
        );

  /** What a re-armed intent keeps from the one it replaces. */
  const carry = (prior: ProviderRecoveryIntent) => ({
    instanceId: prior.instanceId,
    reason: prior.reason,
    attempt: prior.attempt,
    turnId: prior.turnId,
    excluded: prior.excluded,
    createdAt: prior.createdAt,
  });

  /** Arms the next backoff for a transient failure, or gives up at the attempt cap. */
  const scheduleTransient = (
    thread: OrchestrationThreadShell,
    input: {
      readonly epoch: number;
      readonly prior: ProviderRecoveryIntent | undefined;
      readonly instanceId: ProviderInstanceId;
      readonly turnId: string | null;
      readonly message: string;
      readonly retryAfterMs: number | undefined;
    },
  ) =>
    Effect.gen(function* () {
      const { epoch } = input;
      const attempt = (input.prior?.attempt ?? 0) + 1;
      if (attempt > RECOVERY_MAX_ATTEMPTS) return yield* gaveUp(thread.id, input.prior?.token);
      const delay = backoffDelayMs(attempt, input.retryAfterMs);
      const wakeAt = iso((yield* nowMs) + delay);
      const next = yield* newIntent(thread, {
        kind: "transient",
        instanceId: input.instanceId,
        reason: input.message,
        attempt,
        wakeAt,
        turnId: input.turnId,
        excluded: input.prior?.excluded,
        createdAt: input.prior?.createdAt,
      });
      if (yield* schedule(epoch, next)) {
        yield* announce(
          thread.id,
          "info",
          `Provider unreachable. Retrying in ${formatWait(delay)} (attempt ${attempt} of ${RECOVERY_MAX_ATTEMPTS}).`,
          { phase: "retry-wait", attempt, maxAttempts: RECOVERY_MAX_ATTEMPTS, resumeAt: wakeAt },
        );
      }
    });

  /** Quota still out at the wake: wait again, announcing only a changed wait. */
  const rewait = (
    thread: OrchestrationThreadShell,
    prior: ProviderRecoveryIntent,
    decision: Extract<RecoveryAccountDecision, { action: "wait" }>,
    epoch: number,
  ) =>
    Effect.gen(function* () {
      const wakeAt = decision.until === null ? null : iso(decision.until);
      const target = decision.instanceId === prior.instanceId ? null : decision.instanceId;
      const next = yield* newIntent(thread, {
        ...carry(prior),
        kind: "quota",
        wakeAt,
        targetInstanceId: target,
      });
      const changed = prior.wakeAt !== wakeAt || prior.targetInstanceId !== target;
      if ((yield* schedule(epoch, next)) && changed) {
        yield* announceQuotaWait(thread.id, wakeAt, yield* nowMs);
      }
    });

  /** The target account could not take the conversation: keep it where it is and wait. */
  const transferFailed = (
    thread: OrchestrationThreadShell,
    prior: ProviderRecoveryIntent,
    target: ProviderInstanceId,
    reason: string,
    epoch: number,
  ) =>
    Effect.gen(function* () {
      const providers = yield* providerRegistry.getProviders;
      yield* announce(
        thread.id,
        "error",
        `Could not move this conversation to ${providerLabel(providers, target)}. Waiting for ${providerLabel(providers, prior.instanceId)} instead.`,
        { phase: "switch-failed", toInstanceId: target, reason },
      );
      const excluded = [...prior.excluded, target];
      const decision = yield* decideAccount({
        thread,
        current: prior.instanceId,
        blocked: true,
        excluded,
      });
      const wakeAt =
        decision.action === "wait" && decision.until !== null ? iso(decision.until) : null;
      const next = yield* newIntent(thread, {
        ...carry(prior),
        kind: "quota",
        wakeAt,
        excluded,
      });
      if (yield* schedule(epoch, next)) {
        yield* announceQuotaWait(thread.id, wakeAt, yield* nowMs);
      }
    });

  // --- the wake -------------------------------------------------------------

  const wake = (threadId: ThreadId, token: string): Effect.Effect<void> =>
    Effect.gen(function* () {
      const found = Option.getOrUndefined(yield* intents.get(threadId));
      if (found === undefined || found.token !== token || found.status !== "waiting") return;
      const epoch = epochOf(threadId);
      const now = yield* nowMs;
      if (found.wakeAt !== null && Date.parse(found.wakeAt) > now) {
        return yield* arm(found); // one slice of a longer wait
      }
      const threadOption = yield* recoverable(threadId);
      if (Option.isNone(threadOption)) {
        yield* intents.remove(threadId, token);
        return;
      }
      const thread = threadOption.value;
      const session = thread.session;
      const latest = thread.latestTurn?.turnId;
      const binding = Option.getOrUndefined(
        yield* directory.getBinding(threadId).pipe(Effect.orElseSucceed(() => Option.none())),
      );
      // A user Stop deletes the intent itself, so an interrupted session here is
      // this reactor's own interrupt of a parked turn, not a reason to give up.
      const movedOn =
        (found.turnId !== null && latest !== undefined && latest !== found.turnId) ||
        binding === undefined ||
        binding.providerInstanceId !== found.instanceId ||
        !NodeUtil.isDeepStrictEqual(binding.resumeCursor ?? null, found.sessionIdentity);
      if (movedOn) {
        // The user, another turn or another account moved the conversation on.
        return yield* stopRecovery(
          threadId,
          token,
          "Automatic recovery stopped: this conversation changed after the failure.",
          "stopped",
        );
      }
      if (session?.status === "running" || session?.status === "starting") {
        if (!(found.kind === "quota" && found.targetInstanceId !== null)) {
          // The provider's own session is still working this turn (for example a
          // Claude turn parked on a window). Starting another would duplicate it.
          yield* intents.remove(threadId, token);
          return;
        }
        // A parked turn gives way only to an account that can run now.
        yield* providerService
          .interruptTurn({ threadId })
          .pipe(Effect.catchCause(warn("provider recovery interrupt failed", threadId)));
        yield* awaitIdle(threadId);
      }

      let target = found.instanceId;
      if (found.kind === "quota") {
        const decision = yield* decideAccount({
          thread,
          current: found.instanceId,
          blocked: false,
          excluded: found.excluded,
        });
        if (decision.action === "wait") return yield* rewait(thread, found, decision, epoch);
        target = decision.instanceId;
      }

      const unsure = yield* uncertainty(thread, found.turnId);
      if (unsure !== undefined) {
        return yield* stopRecovery(
          threadId,
          token,
          `Not retrying automatically: ${unsure}. Repeating it could duplicate an action that already ran. Send a message to continue.`,
          "needs-attention",
        );
      }

      if (!(yield* intents.claim(threadId, token))) return;

      if (target !== found.instanceId) {
        const providers = yield* providerRegistry.getProviders;
        yield* announce(
          threadId,
          "info",
          `Usage limit reached on ${providerLabel(providers, found.instanceId)}. Moving this conversation to ${providerLabel(providers, target)}.`,
          { phase: "switching", fromInstanceId: found.instanceId, toInstanceId: target },
        );
        const transferred = yield* providerService
          .transferConversation({
            threadId,
            toInstanceId: target,
            ...(found.stagedCursor != null ? { stagedCursor: found.stagedCursor } : {}),
            onForked: (cursor) => intents.stage(threadId, token, cursor).pipe(Effect.asVoid),
          })
          .pipe(Effect.exit);
        if (Exit.isFailure(transferred)) {
          if (Cause.hasInterruptsOnly(transferred.cause)) {
            return yield* Effect.interrupt;
          }
          return yield* transferFailed(thread, found, target, messageOf(transferred.cause), epoch);
        }
        const moved: ModelSelection = { ...thread.modelSelection, instanceId: target };
        yield* engine.dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make(`server:provider-recovery:${yield* crypto.randomUUIDv4}`),
          threadId,
          modelSelection: moved,
        });
      } else {
        yield* announce(
          threadId,
          "info",
          `Resuming this turn (attempt ${found.attempt} of ${RECOVERY_MAX_ATTEMPTS}).`,
          { phase: "resuming", attempt: found.attempt, maxAttempts: RECOVERY_MAX_ATTEMPTS },
        );
      }

      // Cancelled while moving or reconciling: nothing may start now.
      const still = Option.getOrUndefined(yield* intents.get(threadId));
      if (still?.token !== token || epochOf(threadId) !== epoch) return;
      const sent = yield* continueTurn(thread, target).pipe(Effect.exit);
      if (Exit.isFailure(sent)) {
        if (Cause.hasInterruptsOnly(sent.cause)) return yield* Effect.interrupt;
        // A lost reply does not prove the provider refused the continuation.
        return yield* stopRecovery(
          threadId,
          token,
          "The provider did not confirm whether it accepted the continuation. It was not repeated. Send a message to continue.",
          "needs-attention",
        );
      }
      if (epochOf(threadId) === epoch && Option.isSome(yield* intents.get(threadId))) {
        attemptTurns.set(threadId, sent.value.turnId);
      } else {
        // Stop landed while the turn was starting.
        yield* providerService
          .interruptTurn({ threadId })
          .pipe(Effect.catchCause(warn("provider recovery interrupt failed", threadId)));
      }
    }).pipe(Effect.catchCause(warn("provider recovery wake failed", threadId)));

  // --- reacting to provider events ------------------------------------------

  const planQuota = (input: {
    readonly epoch: number;
    readonly thread: OrchestrationThreadShell;
    readonly instanceId: ProviderInstanceId;
    readonly evidence: QuotaEvidence;
    readonly prior: ProviderRecoveryIntent | undefined;
    readonly parked: boolean;
    readonly message: string;
    readonly turnId: string | null;
  }) =>
    Effect.gen(function* () {
      const { epoch } = input;
      const attempt = (input.prior?.attempt ?? 0) + 1;
      if (attempt > RECOVERY_MAX_ATTEMPTS)
        return yield* gaveUp(input.thread.id, input.prior?.token);
      const decision = yield* decideAccount({
        thread: input.thread,
        current: input.instanceId,
        blocked: true,
        evidence: input.evidence,
        excluded: input.prior?.excluded ?? [],
      });
      // A turn parked inside the provider's own session keeps waiting by itself;
      // only a ready alternative is worth interrupting it for.
      if (input.parked && decision.action !== "switch") return;
      const common = {
        kind: "quota",
        instanceId: input.instanceId,
        reason: input.message,
        attempt,
        turnId: input.turnId,
        excluded: input.prior?.excluded ?? [],
        createdAt: input.prior?.createdAt,
      } as const;
      if (decision.action === "wait") {
        const wakeAt = decision.until === null ? null : iso(decision.until);
        const intent = yield* newIntent(input.thread, {
          ...common,
          wakeAt,
          targetInstanceId: decision.instanceId === input.instanceId ? null : decision.instanceId,
        });
        if (yield* schedule(epoch, intent)) {
          yield* announceQuotaWait(input.thread.id, wakeAt, yield* nowMs);
        }
        return;
      }
      // Something can run now: wake at once and let the wake do the checked move.
      const intent = yield* newIntent(input.thread, {
        ...common,
        wakeAt: iso(yield* nowMs),
        targetInstanceId: decision.action === "switch" ? decision.instanceId : null,
      });
      yield* schedule(epoch, intent);
    });

  const onTurnFailed = (event: Extract<ProviderRuntimeEvent, { type: "turn.completed" }>) =>
    Effect.gen(function* () {
      const epoch = epochOf(event.threadId);
      const threadId = event.threadId;
      const instanceId = event.providerInstanceId;
      const threadOption = yield* recoverable(threadId);
      const prior = Option.getOrUndefined(yield* intents.get(threadId));
      if (Option.isNone(threadOption) || instanceId === undefined) {
        if (prior) yield* intents.remove(threadId, prior.token);
        return forgetRuntime(threadId);
      }
      const thread = threadOption.value;
      const turn = event.turnId === undefined ? null : String(event.turnId);
      if (prior?.status === "waiting") return forgetRuntime(threadId); // already planned
      if (prior?.status === "resuming" && turn !== null && attemptTurns.get(threadId) !== turn) {
        return; // the original turn ending late, not the resumed one
      }
      const failure = lastError.get(threadId);
      const message = failure?.message ?? event.payload.errorMessage ?? "Turn failed";
      const evidence = quotaEvidence.get(threadId);
      forgetRuntime(threadId);
      if (evidence !== undefined) {
        return yield* planQuota({
          epoch,
          thread,
          instanceId,
          evidence,
          prior,
          parked: false,
          message,
          turnId: turn,
        });
      }
      const verdict = classifyFailure({
        message,
        errorClass: failure?.transport === true ? "transport_error" : undefined,
      });
      if (verdict.kind === "terminal") {
        if (prior) {
          yield* stopRecovery(
            threadId,
            prior.token,
            `Automatic recovery stopped: ${message}`,
            "stopped",
          );
        }
        return;
      }
      yield* scheduleTransient(thread, {
        epoch,
        prior,
        instanceId,
        turnId: turn,
        message,
        retryAfterMs: verdict.retryAfterMs,
      });
    });

  /** A window ran dry mid-turn. Act now only if another account can take over. */
  const onQuotaBlocked = (
    event: Extract<ProviderRuntimeEvent, { type: "account.rate-limits.updated" }>,
  ) =>
    Effect.gen(function* () {
      const epoch = epochOf(event.threadId);
      const threadId = event.threadId;
      const instanceId = event.providerInstanceId;
      if (instanceId === undefined) return;
      const evidence: QuotaEvidence = { windows: event.payload.limits.windows };
      quotaEvidence.set(threadId, evidence);
      const now = yield* nowMs;
      const key = evidence.windows
        .map((window) => `${window.id}@${window.resetsAt ?? ""}`)
        .join("|");
      const last = lastQuotaLook.get(threadId);
      if (last?.key === key && now - last.at < QUOTA_UNKNOWN_RECHECK_MS) return;
      lastQuotaLook.set(threadId, { key, at: now });
      const threadOption = yield* recoverable(threadId);
      if (Option.isNone(threadOption)) return;
      const prior = Option.getOrUndefined(yield* intents.get(threadId));
      const session = threadOption.value.session;
      if (prior !== undefined || session?.status !== "running" || session.activeTurnId == null) {
        return;
      }
      yield* planQuota({
        epoch,
        thread: threadOption.value,
        instanceId,
        evidence,
        prior,
        parked: true,
        message: "Usage limit reached",
        turnId: String(session.activeTurnId),
      });
    });

  const onRuntime = (event: ProviderRuntimeEvent) =>
    Effect.gen(function* () {
      const threadId = event.threadId;
      switch (event.type) {
        case "turn.started":
          lastError.delete(threadId);
          quotaEvidence.delete(threadId);
          lastQuotaLook.delete(threadId);
          return;
        case "runtime.error":
          lastError.set(threadId, {
            message: event.payload.message,
            transport: event.payload.class === "transport_error",
          });
          if (event.payload.code === USAGE_LIMIT_EXCEEDED_CODE) {
            // Codex's typed stop: the bars (if any) arrive from its rate-limit updates.
            quotaEvidence.set(threadId, quotaEvidence.get(threadId) ?? { windows: [] });
          }
          return;
        case "account.rate-limits.updated":
          if (event.payload.limits.blocked === true) yield* onQuotaBlocked(event);
          return;
        case "turn.completed":
          if (event.payload.state === "failed") {
            yield* onTurnFailed(event);
          } else if (event.payload.state === "completed") {
            const prior = Option.getOrUndefined(yield* intents.get(threadId));
            if (prior?.status === "resuming") yield* intents.remove(threadId, prior.token);
            forgetRuntime(threadId);
          }
          return;
        default:
          return;
      }
    }).pipe(Effect.catchCause(warn("provider recovery event failed", event.threadId)));

  const worker = yield* makeDrainableWorker((job: Job) =>
    job._tag === "wake" ? wake(job.threadId, job.token) : onRuntime(job.event),
  );
  enqueueWake = (threadId, token) =>
    worker.enqueue({ _tag: "wake", threadId, token }).pipe(Effect.asVoid);

  const onDomainEvent = (event: OrchestrationEvent) => {
    switch (event.type) {
      case "thread.turn-interrupt-requested":
        // The Stop button, from desktop and from mobile alike. Session stops
        // belong to cleanup flows (settle, archive) and are not a user cancel.
        return cancel(event.payload.threadId, { interrupt: true });
      case "thread.turn-start-requested":
      case "thread.deleted":
      case "thread.archived":
        return cancel(event.payload.threadId, { interrupt: false });
      case "thread.recovery-set":
        return event.payload.enabled
          ? Effect.void
          : cancel(event.payload.threadId, { interrupt: false });
      default:
        return Effect.void;
    }
  };

  const RELEVANT = new Set([
    "turn.started",
    "turn.completed",
    "runtime.error",
    "account.rate-limits.updated",
  ]);

  /** Re-arms what a previous process left behind; a turn caught mid-resume cannot be vouched for. */
  const restore = Effect.gen(function* () {
    for (const intent of yield* intents.list()) {
      const thread = yield* recoverable(intent.threadId);
      if (Option.isNone(thread)) {
        yield* intents.remove(intent.threadId, intent.token);
      } else if (intent.status === "resuming") {
        yield* stopRecovery(
          intent.threadId,
          intent.token,
          "Automatic recovery was interrupted by a restart while resuming. Whether the provider accepted the continuation cannot be confirmed, so it was not repeated. Send a message to continue.",
          "needs-attention",
        );
      } else {
        yield* arm(intent);
      }
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.logWarning("provider recovery restore failed", { cause: Cause.pretty(cause) }),
    ),
  );

  const start: ProviderRecoveryReactor["Service"]["start"] = Effect.fn(
    "ProviderRecoveryReactor.start",
  )(function* () {
    const scope = yield* Effect.scope;
    reactorScope = scope;
    const events = yield* engine.subscribeDomainEvents;
    const runtimeEvents = providerService.streamEvents.pipe(
      Stream.filter((event) => RELEVANT.has(event.type)),
    );
    // Parked until the server is active: a standby process must not resume turns.
    yield* forkParked(Stream.runForEach(events, onDomainEvent));
    yield* forkParked(
      Stream.runForEach(runtimeEvents, (event) => worker.enqueue({ _tag: "runtime", event })),
    );
    yield* forkParked(restore);
  });

  return { start, drain: worker.drain } satisfies ProviderRecoveryReactor["Service"];
});

export const layer = Layer.effect(ProviderRecoveryReactor, make);

/** Production wiring: builds the reactor and starts it with the layer's scope. */
export const layerStarted = Layer.effectDiscard(
  Effect.gen(function* () {
    const reactor = yield* make;
    yield* reactor.start();
  }),
);

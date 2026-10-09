/**
 * Scriptable provider adapter and multi-instance registry for recovery tests.
 * Sessions, resume cursors and turns behave like a real adapter; nothing talks
 * to a model. Tests drive outcomes by emitting runtime events.
 *
 * @module provider/testUtils/fakeProviderAdapter
 */
import {
  EventId,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";

import { ProviderUnsupportedError, type ProviderAdapterError } from "../Errors.ts";
import type {
  ProviderAdapterShape,
  ProviderForkThreadInput,
  ProviderForkThreadResult,
} from "../Services/ProviderAdapter.ts";
import type { ProviderAdapterRegistryShape } from "../Services/ProviderAdapterRegistry.ts";

export function makeFakeProviderAdapter(input: {
  readonly provider: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly promptless?: boolean;
  readonly fork?: boolean;
}) {
  const { provider, instanceId } = input;
  const sessions = new Map<ThreadId, ProviderSession>();
  const events = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  let turnCounter = 0;
  let eventCounter = 0;
  const now = () => "2026-10-09T10:00:00.000Z";

  const startSession = vi.fn((start: ProviderSessionStartInput) =>
    Effect.sync(() => {
      const session: ProviderSession = {
        provider,
        providerInstanceId: instanceId,
        status: "ready",
        runtimeMode: start.runtimeMode,
        threadId: start.threadId,
        resumeCursor: start.resumeCursor ?? { resume: `${instanceId}-session-${start.threadId}` },
        cwd: start.cwd ?? process.cwd(),
        createdAt: now(),
        updatedAt: now(),
      };
      sessions.set(session.threadId, session);
      return session;
    }),
  );
  const sendTurn = vi.fn(
    (turn: ProviderSendTurnInput): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.sync(() => {
        const turnId = TurnId.make(`${instanceId}-turn-${++turnCounter}`);
        const session = sessions.get(turn.threadId);
        if (session) {
          sessions.set(turn.threadId, { ...session, status: "running", activeTurnId: turnId });
        }
        emit({ type: "turn.started", threadId: turn.threadId, turnId, payload: {} });
        return { threadId: turn.threadId, turnId };
      }),
  );
  const interruptTurn = vi.fn((threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
    Effect.sync(() => {
      const current = sessions.get(threadId);
      if (!current) return;
      const { activeTurnId, ...rest } = current;
      sessions.set(threadId, { ...rest, status: "ready" });
      emit({
        type: "turn.aborted",
        threadId,
        ...(activeTurnId ? { turnId: activeTurnId } : {}),
        payload: { reason: "interrupted" },
      });
    }),
  );
  const stopSession = vi.fn((threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
    Effect.sync(() => void sessions.delete(threadId)),
  );
  const readThread = vi.fn((threadId: ThreadId) =>
    Effect.succeed({
      threadId,
      turns: [] as ReadonlyArray<{ id: TurnId; items: ReadonlyArray<unknown> }>,
    }),
  );
  const forkThread = vi.fn(
    (
      _fork: ProviderForkThreadInput,
    ): Effect.Effect<ProviderForkThreadResult, ProviderAdapterError> =>
      Effect.succeed({
        resumeCursor: { resume: `${instanceId}-fork-${forkThread.mock.calls.length}` },
      }),
  );

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
      ...(input.promptless ? { promptlessTurnContinuation: true } : {}),
      ...(input.fork === false ? {} : { supportsConversationFork: true }),
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: () => Effect.void,
    respondToUserInput: () => Effect.void,
    stopSession,
    listSessions: () => Effect.sync(() => Array.from(sessions.values())),
    hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
    readThread,
    rollbackThread: (threadId) => Effect.succeed({ threadId, turns: [] }),
    ...(input.fork === false ? {} : { forkThread }),
    stopAll: () => Effect.sync(() => sessions.clear()),
    get streamEvents() {
      return Stream.fromPubSub(events);
    },
  };

  /** Publishes an adapter event; id, provider and time are filled in. */
  function emit(
    event: { readonly type: string; readonly threadId: ThreadId } & Record<string, unknown>,
  ) {
    Effect.runSync(
      PubSub.publish(events, {
        eventId: EventId.make(`${instanceId}-event-${++eventCounter}`),
        provider,
        createdAt: now(),
        ...event,
      } as unknown as ProviderRuntimeEvent),
    );
  }

  /** Ends the running turn the way the adapter does: the session settles first. */
  const settleTurn = (threadId: ThreadId, state: "completed" | "failed", errorMessage?: string) => {
    const session = sessions.get(threadId);
    const turnId = session?.activeTurnId;
    if (session) {
      const { activeTurnId: _active, ...rest } = session;
      sessions.set(threadId, { ...rest, status: "ready" });
    }
    emit({
      type: "turn.completed",
      threadId,
      ...(turnId ? { turnId } : {}),
      payload: { state, ...(errorMessage ? { errorMessage } : {}) },
    });
  };

  return {
    adapter,
    instanceId,
    sessions,
    emit,
    settleTurn,
    startSession,
    sendTurn,
    interruptTurn,
    stopSession,
    readThread,
    forkThread,
  };
}

export type FakeProviderAdapter = ReturnType<typeof makeFakeProviderAdapter>;

/** Registry of several configured instances; flip `enabled` to simulate settings edits. */
export function makeMultiInstanceRegistry(
  entries: ReadonlyArray<{
    readonly instanceId: ProviderInstanceId;
    readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
  }>,
  enabled: Map<ProviderInstanceId, boolean> = new Map(),
): ProviderAdapterRegistryShape {
  const byId = new Map(entries.map((entry) => [entry.instanceId, entry.adapter]));
  const unsupported = (instanceId: ProviderInstanceId) =>
    new ProviderUnsupportedError({ provider: ProviderDriverKind.make(instanceId) });
  return {
    getByInstance: (instanceId) => {
      const adapter = byId.get(instanceId);
      return adapter ? Effect.succeed(adapter) : Effect.fail(unsupported(instanceId));
    },
    getInstanceInfo: (instanceId) => {
      const adapter = byId.get(instanceId);
      return adapter
        ? Effect.succeed({
            instanceId,
            driverKind: adapter.provider,
            displayName: undefined,
            enabled: enabled.get(instanceId) ?? true,
            continuationIdentity: {
              driverKind: adapter.provider,
              continuationKey: `${adapter.provider}:instance:${instanceId}`,
            },
          })
        : Effect.fail(unsupported(instanceId));
    },
    listInstances: () => Effect.succeed(Array.from(byId.keys())),
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
      PubSub.subscribe(pubsub),
    ),
  };
}

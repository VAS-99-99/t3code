import { scopedThreadKey, scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  type AtomCommandResult,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  OrchestrationThreadShell,
  ScopedThreadRef,
  ServerProvider,
  ThreadForkInput,
  ThreadForkResult,
} from "@t3tools/contracts";

/**
 * Fork menu state for a thread: undefined hides the item (the server or
 * provider cannot fork), `disabled` greys it while the source has no safely
 * completed history to copy. The server re-checks all of this; this only
 * keeps the menu from offering what is certain to fail.
 */
export function resolveThreadForkMenuState(input: {
  readonly thread: Pick<
    OrchestrationThreadShell,
    "modelSelection" | "session" | "latestTurn" | "hasPendingApprovals" | "hasPendingUserInput"
  >;
  readonly providers: ReadonlyArray<
    Pick<ServerProvider, "instanceId" | "supportsConversationFork">
  >;
}): { readonly disabled: boolean } | undefined {
  const { thread } = input;
  const provider = input.providers.find(
    (candidate) => candidate.instanceId === thread.modelSelection.instanceId,
  );
  if (provider?.supportsConversationFork !== true) return undefined;
  const { session, latestTurn } = thread;
  const busy =
    session?.status === "starting" ||
    session?.status === "running" ||
    session?.activeTurnId != null ||
    thread.hasPendingApprovals ||
    thread.hasPendingUserInput;
  return {
    disabled: busy || (latestTurn !== null && latestTurn.state !== "completed"),
  };
}

type ForkIds = Pick<ThreadForkInput, "commandId" | "destinationThreadId">;
type ForkSend = (request: {
  readonly environmentId: EnvironmentId;
  readonly input: ThreadForkInput;
}) => Promise<AtomCommandResult<ThreadForkResult, unknown>>;

function isDestinationConflict(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { _tag?: unknown })._tag === "ThreadForkError" &&
    (error as { reason?: unknown }).reason === "destination-conflict"
  );
}

/**
 * One fork intent per source thread. The command and destination ids are
 * allocated once and reused by every retry until the fork succeeds, so a lost
 * response or a reconnect retries the same fork instead of creating another.
 * A click while a request is in flight is ignored. A destination-conflict
 * means the ids are taken by something else, so the next attempt gets new ones.
 */
export function createThreadForkIntents(allocate: () => ForkIds) {
  const intents = new Map<string, ForkIds>();
  const pending = new Set<string>();
  return {
    /** Resolves null when a request for this source is already in flight. */
    async run(
      sourceRef: ScopedThreadRef,
      send: ForkSend,
    ): Promise<AtomCommandResult<ThreadForkResult, unknown> | null> {
      const key = scopedThreadKey(sourceRef);
      if (pending.has(key)) return null;
      const ids = intents.get(key) ?? allocate();
      intents.set(key, ids);
      pending.add(key);
      try {
        // The source thread's own environment owns the fork, not the active one.
        const result = await send({
          environmentId: sourceRef.environmentId,
          input: { sourceThreadId: sourceRef.threadId, ...ids },
        });
        if (
          result._tag === "Success" ||
          (result._tag === "Failure" && isDestinationConflict(squashAtomCommandFailure(result)))
        ) {
          intents.delete(key);
        }
        return result;
      } finally {
        pending.delete(key);
      }
    },
  };
}

/** The server's own reason when it gave one: the wrapped message repeats the thread id. */
export function describeForkFailure(error: unknown): string {
  if (typeof error === "object" && error !== null && "detail" in error) {
    const { detail } = error as { detail: unknown };
    if (typeof detail === "string" && detail.length > 0) return detail;
  }
  return error instanceof Error ? error.message : "An error occurred.";
}

/** Fork `sourceRef`, then open the destination in the same environment. */
export async function forkThread(
  deps: {
    readonly intents: ReturnType<typeof createThreadForkIntents>;
    readonly send: ForkSend;
    readonly waitForThreadShell: (ref: ScopedThreadRef) => Promise<void>;
    readonly navigate: (ref: ScopedThreadRef) => Promise<void>;
    readonly reportFailure: (title: string, description: string) => void;
  },
  sourceRef: ScopedThreadRef,
): Promise<void> {
  const result = await deps.intents.run(sourceRef, deps.send);
  if (result === null) return;
  if (result._tag === "Failure") {
    if (!isAtomCommandInterrupted(result)) {
      deps.reportFailure(
        "Failed to fork thread",
        describeForkFailure(squashAtomCommandFailure(result)),
      );
    }
    return;
  }
  const destination = scopeThreadRef(sourceRef.environmentId, result.value.destinationThreadId);
  try {
    // Navigating before the shell arrives makes the thread route treat it as missing and bounce.
    await deps.waitForThreadShell(destination);
    await deps.navigate(destination);
  } catch (error) {
    deps.reportFailure("Thread forked, but navigation failed", describeForkFailure(error));
  }
}

/**
 * ProviderService - Service interface for provider sessions, turns, and checkpoints.
 *
 * Acts as the cross-provider facade used by transports (WebSocket/RPC). It
 * resolves provider adapters through `ProviderAdapterRegistry`, routes
 * session-scoped calls via `ProviderSessionDirectory`, and exposes one unified
 * provider event stream to callers.
 *
 * Uses Effect `Context.Service` for dependency injection and returns typed
 * domain errors for validation, session, codex, and checkpoint workflows.
 *
 * @module ProviderService
 */
import type {
  CommandId,
  ProviderInterruptTurnInput,
  ProviderInstanceId,
  ProviderRespondToRequestInput,
  ProviderRespondToUserInputInput,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ProviderStopSessionInput,
  ProviderUploadFeedbackInput,
  ProviderUploadFeedbackResult,
  MessageId,
  ThreadId,
  ProviderTurnStartResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Stream from "effect/Stream";

import type { ProviderServiceError } from "../Errors.ts";
import type { ProviderAdapterCapabilities, ProviderThreadSnapshot } from "./ProviderAdapter.ts";
import type { ProviderInstanceRoutingInfo } from "./ProviderAdapterRegistry.ts";

export interface ProviderForkConversationInput<E = never> {
  readonly sourceThreadId: ThreadId;
  /** Must not have a provider binding yet, unless this same request created it. */
  readonly destinationThreadId: ThreadId;
  /** Identifies the request: repeating it returns the fork it already made. */
  readonly commandId: CommandId;
  /**
   * The caller's record of what it is forking, as JSON. It runs only when this
   * call creates the fork, while no turn can start on the source, and a
   * failure aborts the fork. Its result is stored with the binding and handed
   * back unchanged on every retry, however far the source has moved on.
   */
  readonly capture: Effect.Effect<unknown, E>;
  /** Publishes staged history under destination admission, after native acceptance. */
  readonly publish?: (fork: ProviderForkConversationResult) => Effect.Effect<void, E>;
}

export interface ProviderTransferConversationInput<E = never> {
  readonly threadId: ThreadId;
  readonly toInstanceId: ProviderInstanceId;
  /** A native copy a previous attempt already accepted: bound as is, never copied again. */
  readonly stagedCursor?: unknown;
  /** Runs once the target accepted a new copy, before the binding moves, so a restart can bind it. */
  readonly onForked?: (cursor: unknown) => Effect.Effect<void, E>;
}

export interface ProviderForkConversationResult {
  /** False when a retry found the fork already bound. */
  readonly created: boolean;
  readonly captured: unknown;
}

/**
 * ProviderServiceShape - Service API for provider session and turn orchestration.
 */
export interface ProviderServiceShape {
  /**
   * Start a provider session.
   */
  readonly startSession: (
    threadId: ThreadId,
    input: ProviderSessionStartInput,
  ) => Effect.Effect<ProviderSession, ProviderServiceError>;

  /**
   * Send a provider turn.
   */
  readonly sendTurn: (
    input: ProviderSendTurnInput,
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError>;

  readonly compactThread: (
    threadId: ThreadId,
    modelSelection?: ProviderSendTurnInput["modelSelection"],
    requestId?: MessageId,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Interrupt a running provider turn.
   */
  readonly interruptTurn: (
    input: ProviderInterruptTurnInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Respond to a provider approval request.
   */
  readonly respondToRequest: (
    input: ProviderRespondToRequestInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Respond to a provider structured user-input request.
   */
  readonly respondToUserInput: (
    input: ProviderRespondToUserInputInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Stop a provider session.
   */
  readonly stopSession: (
    input: ProviderStopSessionInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * List active provider sessions.
   *
   * Aggregates runtime session lists from all registered adapters.
   */
  readonly listSessions: () => Effect.Effect<ReadonlyArray<ProviderSession>>;

  /**
   * Read capabilities for the adapter bound to a configured provider instance.
   */
  readonly getCapabilities: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<ProviderAdapterCapabilities, ProviderServiceError>;

  readonly getInstanceInfo: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<ProviderInstanceRoutingInfo, ProviderServiceError>;

  /**
   * Reject unsupported rewind before files change, without resuming the session.
   */
  readonly assertConversationRollbackSupported: (
    threadId: ThreadId,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Roll back provider conversation state by a number of turns.
   */
  readonly rollbackConversation: (input: {
    readonly threadId: ThreadId;
    readonly numTurns: number;
  }) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Fork an idle conversation and durably stage its independent native session.
   * Atomic thread creation publishes the binding. Starts no session and no turn, and never
   * shares the source's native session.
   */
  readonly forkConversation: <E>(
    input: ProviderForkConversationInput<E>,
  ) => Effect.Effect<ProviderForkConversationResult, ProviderServiceError | E>;

  /**
   * Read the provider's own record of a thread, resuming its bound session if
   * none is live. Starts no turn.
   */
  readonly readConversation: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderThreadSnapshot, ProviderServiceError>;

  /**
   * Move an idle thread to another configured account of the same driver by
   * asking the target account's own provider configuration to copy the native
   * history. Fails, leaving the thread where it was, when that account cannot
   * see the conversation. Keeps the ThreadId; starts no turn.
   */
  readonly transferConversation: <E = never>(
    input: ProviderTransferConversationInput<E>,
  ) => Effect.Effect<void, ProviderServiceError | E>;

  /**
   * Upload a thread and return the provider's shareable feedback identifier.
   */
  readonly uploadFeedback: (
    input: ProviderUploadFeedbackInput,
  ) => Effect.Effect<ProviderUploadFeedbackResult, ProviderServiceError>;

  /**
   * Canonical provider runtime event stream.
   *
   * Fan-out is owned by ProviderService (not by a standalone event-bus service).
   */
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}

/**
 * ProviderService - Service tag for provider orchestration.
 */
export class ProviderService extends Context.Service<ProviderService, ProviderServiceShape>()(
  "t3/provider/Services/ProviderService",
) {}

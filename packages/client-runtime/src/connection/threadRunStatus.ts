import type {
  OrchestrationLatestTurn,
  OrchestrationThreadActivity,
  ServerConfig,
  ServerProviderAuthStatus,
} from "@t3tools/contracts";

import type { EnvironmentConnectionPresentation } from "./presentation.ts";

/**
 * Why a thread is not making progress, told from signals the client already
 * holds. Each cause names who must act. Text is fixed: neither connection
 * errors nor recovery payload fields (which can carry URLs or tokens) are
 * ever repeated.
 */

const RECOVERY_ACTIVITY_KIND = "provider.recovery";

/** Every phase ProviderRecoveryReactor emits that still needs the user to know. */
const RECOVERY_PHASES = [
  "quota-wait",
  "retry-wait",
  "switching",
  "switch-failed",
  "resuming",
  "needs-attention",
  "gave-up",
] as const;

export type RecoveryNote = {
  readonly phase: (typeof RECOVERY_PHASES)[number];
  readonly resumeAt: string | null;
  readonly attempt: number | null;
  readonly maxAttempts: number | null;
};

const timeOf = (iso: unknown): number | null => {
  if (typeof iso !== "string") return null;
  const ms = Date.parse(iso);
  return Number.isNaN(ms) ? null : ms;
};

const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isInteger(value) && value >= 1;

/**
 * The newest recovery row decides. "cancelled" and "stopped" end recovery.
 * Later unrelated activity does not end it: only a turn requested after the
 * row (a continuation, or the user's next message) shows the thread moved on.
 * Unreadable dates never clear a note, since hiding a pause costs more than
 * showing a stale one.
 */
export function latestRecoveryNote(
  activities: ReadonlyArray<Pick<OrchestrationThreadActivity, "kind" | "payload" | "createdAt">>,
  latestTurn: Pick<OrchestrationLatestTurn, "requestedAt"> | null,
): RecoveryNote | null {
  const row = activities.findLast((activity) => activity.kind === RECOVERY_ACTIVITY_KIND);
  if (row === undefined) return null;
  const payload = (typeof row.payload === "object" && row.payload !== null ? row.payload : {}) as {
    readonly [key: string]: unknown;
  };
  const phase = RECOVERY_PHASES.find((candidate) => candidate === payload.phase);
  if (phase === undefined) return null;

  const rowAt = timeOf(row.createdAt);
  const turnAt = timeOf(latestTurn?.requestedAt);
  if (rowAt !== null && turnAt !== null && turnAt > rowAt) return null;

  const countsOk =
    isCount(payload.attempt) &&
    isCount(payload.maxAttempts) &&
    payload.attempt <= payload.maxAttempts;
  return {
    phase,
    resumeAt: timeOf(payload.resumeAt) === null ? null : (payload.resumeAt as string),
    attempt: countsOk ? (payload.attempt as number) : null,
    maxAttempts: countsOk ? (payload.maxAttempts as number) : null,
  };
}

/** What the composer banner says while the thread's machine is not connected. */
export function connectionWaitingDescription(
  connection: EnvironmentConnectionPresentation,
): string {
  switch (connection.phase) {
    case "connecting":
    case "reconnecting":
      return "Showing cached thread data. New messages wait for the connection. This is not a provider failure.";
    case "unsupported":
      return "This app cannot talk to that server. Provider and account state are unknown.";
    default:
      return "Showing cached thread data. Provider and account state are unknown until it reconnects.";
  }
}

export type ThreadRunStatusInput = {
  readonly connected: boolean;
  readonly machineLabel: string | null;
  readonly providerLabel: string | null;
  /** Email or plan label from provider auth; null when the provider does not report one. */
  readonly accountLabel: string | null;
  readonly providerAuth: ServerProviderAuthStatus | null;
  /** The provider status banner already tells the user to sign in. */
  readonly providerBannerVisible: boolean;
  readonly pendingApprovalCount: number;
  readonly pendingQuestionCount: number;
  readonly recovery: RecoveryNote | null;
  readonly formatTime: (iso: string) => string;
};

export type ThreadRunStatus = {
  readonly cause: "approval" | "question" | "login" | "recovery";
  readonly variant: "warning" | "info";
  readonly title: string;
  readonly description: string;
};

function recoveryText(note: RecoveryNote, formatTime: (iso: string) => string): ThreadRunStatus {
  const at = note.resumeAt ? formatTime(note.resumeAt) : null;
  switch (note.phase) {
    case "quota-wait":
      return {
        cause: "recovery",
        variant: "info",
        title: "Usage limit reached",
        description: at
          ? `Recovery will resume this turn after ${at}.`
          : "Recovery rechecks quota periodically. Reset time unknown.",
      };
    case "retry-wait":
      return {
        cause: "recovery",
        variant: "info",
        title: "Provider unavailable",
        description: `Recovery retries${at ? ` around ${at}` : " shortly"}${
          note.attempt !== null && note.maxAttempts !== null
            ? ` (attempt ${note.attempt} of ${note.maxAttempts})`
            : ""
        }.`,
      };
    case "switching":
      return {
        cause: "recovery",
        variant: "info",
        title: "Recovery is switching accounts",
        description: "Moving this thread to another enabled account of the same provider.",
      };
    case "switch-failed":
      return {
        cause: "recovery",
        variant: "warning",
        title: "Recovery could not switch accounts",
        description:
          "The thread stays on its current account and recovery keeps waiting. Check the provider in Settings.",
      };
    case "resuming":
      return {
        cause: "recovery",
        variant: "info",
        title: "Recovery is sending a continuation",
        description: "Waiting for the provider to confirm it accepted the continuation.",
      };
    case "needs-attention":
      return {
        cause: "recovery",
        variant: "warning",
        title: "Automatic recovery paused",
        description:
          "Recovery did not repeat the last step because it may already have run. Review the thread, then send a message to continue.",
      };
    case "gave-up":
      return {
        cause: "recovery",
        variant: "warning",
        title: "Automatic recovery gave up",
        description: "It stopped retrying. Send a message to continue.",
      };
  }
}

/**
 * Picks the one cause to show. Only a connected machine reports provider,
 * approval, or recovery state, so a disconnected thread returns null and the
 * connection banner speaks instead. Approval and question stay separate
 * because the user answers them in different panels.
 */
export function resolveThreadRunStatus(input: ThreadRunStatusInput): ThreadRunStatus | null {
  if (!input.connected) return null;
  const context = [input.machineLabel, input.providerLabel, input.accountLabel]
    .filter((part): part is string => part !== null && part.length > 0)
    .join(" · ");
  const withContext = (status: ThreadRunStatus): ThreadRunStatus =>
    context ? { ...status, description: `${context}. ${status.description}` } : status;

  if (input.pendingApprovalCount > 0) {
    return withContext({
      cause: "approval",
      variant: "warning",
      title: "Waiting for your approval",
      description: "The agent is paused on a tool request. Approve or deny it to continue.",
    });
  }
  if (input.pendingQuestionCount > 0) {
    return withContext({
      cause: "question",
      variant: "warning",
      title: "Waiting for your answer",
      description: "The agent asked a question. Answer it to continue.",
    });
  }
  if (input.providerAuth === "unauthenticated" && !input.providerBannerVisible) {
    return withContext({
      cause: "login",
      variant: "warning",
      title: "Provider is signed out",
      description: "Sign in on that machine before sending.",
    });
  }
  return input.recovery ? withContext(recoveryText(input.recovery, input.formatTime)) : null;
}

const OS_LABELS = { darwin: "macOS", linux: "Linux", windows: "Windows" } as const;

/**
 * One line of server identity for a connection row. The server advertises
 * capabilities; that is a claim, not proof the feature works, so the wording
 * says "advertised". Versions are shown, never compared. Without a live
 * connection the config is the last one received, so it is labelled.
 */
export function describeServerDetails(input: {
  readonly connected: boolean;
  /** `providers` is absent for a relay descriptor, which cannot say what drivers support. */
  readonly serverConfig:
    | (Pick<ServerConfig, "environment"> & { readonly providers?: ServerConfig["providers"] })
    | null;
}): string {
  const { connected, serverConfig } = input;
  if (serverConfig === null) return "Server details unavailable until the first connection";
  const { platform, serverVersion, orchestrationProtocolVersion, capabilities } =
    serverConfig.environment;
  const os = platform.os === "unknown" ? null : OS_LABELS[platform.os];
  const arch = platform.arch === "other" ? null : platform.arch;
  const advertised = (on: boolean) => (on ? "advertised" : "Not advertised");
  const parts = [
    [os, arch].filter((part) => part !== null).join(" ") || null,
    `Server ${serverVersion}`,
    orchestrationProtocolVersion === undefined ? null : `Protocol ${orchestrationProtocolVersion}`,
    `Conversation forks: ${
      serverConfig.providers === undefined
        ? "Not reported"
        : advertised(serverConfig.providers.some((p) => p.supportsConversationFork === true))
    }`,
    `Automatic recovery: ${advertised(capabilities.threadRecovery === true)}`,
  ].filter((part): part is string => part !== null && part.length > 0);
  return `${connected ? "" : "Last known: "}${parts.join(" · ")}`;
}

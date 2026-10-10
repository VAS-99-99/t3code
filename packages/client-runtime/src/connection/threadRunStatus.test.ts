import { describe, expect, it } from "vite-plus/test";
import type { ServerConfig } from "@t3tools/contracts";

import {
  connectionWaitingDescription,
  describeServerDetails,
  latestRecoveryNote,
  resolveThreadRunStatus,
  type RecoveryNote,
  type ThreadRunStatusInput,
} from "./threadRunStatus.ts";

const idle: ThreadRunStatusInput = {
  connected: true,
  machineLabel: "Studio",
  providerLabel: "Claude",
  accountLabel: null,
  providerAuth: "authenticated",
  providerBannerVisible: false,
  pendingApprovalCount: 0,
  pendingQuestionCount: 0,
  recovery: null,
  formatTime: (iso) => `at ${iso}`,
};

const note = (phase: RecoveryNote["phase"], rest: Partial<RecoveryNote> = {}): RecoveryNote => ({
  phase,
  resumeAt: null,
  attempt: null,
  maxAttempts: null,
  ...rest,
});

// Rows shaped exactly like ProviderRecoveryReactor.announce writes them.
const T0 = "2026-10-10T10:00:00.000Z";
const T1 = "2026-10-10T10:05:00.000Z";
const T2 = "2026-10-10T10:10:00.000Z";
const recoveryRow = (payload: unknown, createdAt = T1) => ({
  kind: "provider.recovery",
  payload,
  createdAt,
});
const otherRow = (createdAt = T2) => ({ kind: "tool.started", payload: {}, createdAt });
const failedTurn = { requestedAt: T0 };

describe("resolveThreadRunStatus", () => {
  it("shows nothing when nothing is blocking", () => {
    expect(resolveThreadRunStatus(idle)).toBeNull();
  });

  it("stays silent while disconnected so provider state is not guessed", () => {
    expect(
      resolveThreadRunStatus({
        ...idle,
        connected: false,
        pendingApprovalCount: 1,
        providerAuth: "unauthenticated",
      }),
    ).toBeNull();
  });

  it("keeps approval and question as different causes", () => {
    const approval = resolveThreadRunStatus({ ...idle, pendingApprovalCount: 1 });
    const question = resolveThreadRunStatus({ ...idle, pendingQuestionCount: 1 });
    expect(approval?.cause).toBe("approval");
    expect(question?.cause).toBe("question");
    expect(approval?.description).toContain("Approve or deny");
    expect(question?.description).toContain("Answer");
  });

  it("names machine, provider and account only when known", () => {
    const known = resolveThreadRunStatus({
      ...idle,
      accountLabel: "me@example.com",
      pendingQuestionCount: 1,
    });
    expect(known?.description.startsWith("Studio · Claude · me@example.com. ")).toBe(true);
    const unknown = resolveThreadRunStatus({
      ...idle,
      machineLabel: null,
      providerLabel: null,
      pendingQuestionCount: 1,
    });
    expect(unknown?.description.startsWith("The agent asked")).toBe(true);
  });

  it("does not repeat a sign-in prompt the provider banner already shows", () => {
    const signedOut = { ...idle, providerAuth: "unauthenticated" as const };
    expect(resolveThreadRunStatus(signedOut)?.cause).toBe("login");
    expect(resolveThreadRunStatus({ ...signedOut, providerBannerVisible: true })).toBeNull();
  });

  it("treats unknown auth as not a login problem", () => {
    expect(resolveThreadRunStatus({ ...idle, providerAuth: "unknown" })).toBeNull();
  });

  it("describes quota recovery without account switching", () => {
    const status = resolveThreadRunStatus({
      ...idle,
      recovery: note("quota-wait", { resumeAt: "T1" }),
    });
    expect(status?.cause).toBe("recovery");
    expect(status?.description).toContain("after at T1");
    expect(status?.description).not.toMatch(/switch|another account/i);
  });

  it("says reset time is unknown when recovery has none", () => {
    const status = resolveThreadRunStatus({ ...idle, recovery: note("quota-wait") });
    expect(status?.description).toContain("Reset time unknown");
  });

  it("only mentions account switching for the switching phases", () => {
    const switching = resolveThreadRunStatus({ ...idle, recovery: note("switching") });
    expect(switching?.description).toContain("another enabled account");
    const retry = resolveThreadRunStatus({
      ...idle,
      recovery: note("retry-wait", { resumeAt: "T2", attempt: 2, maxAttempts: 5 }),
    });
    expect(retry?.description).toContain("attempt 2 of 5");
  });

  it("puts a pending approval ahead of a recovery note", () => {
    const status = resolveThreadRunStatus({
      ...idle,
      pendingApprovalCount: 1,
      recovery: note("resuming", { attempt: 1, maxAttempts: 3 }),
    });
    expect(status?.cause).toBe("approval");
  });

  it("tells the user to act on needs-attention without implying replay is safe", () => {
    const status = resolveThreadRunStatus({ ...idle, recovery: note("needs-attention") });
    expect(status?.cause).toBe("recovery");
    expect(status?.variant).toBe("warning");
    expect(status?.description).toMatch(/send a message to continue/i);
    expect(status?.description).toMatch(/may already have run/);
    expect(status?.description).not.toMatch(/safe|not replayed|will retry/i);
  });

  it("says recovery gave up and that only the user can continue", () => {
    const status = resolveThreadRunStatus({ ...idle, recovery: note("gave-up") });
    expect(status?.variant).toBe("warning");
    expect(status?.title).toMatch(/gave up/i);
    expect(status?.description).toContain("Send a message to continue");
  });

  it("makes no replay claim while a continuation is unconfirmed", () => {
    const status = resolveThreadRunStatus({ ...idle, recovery: note("resuming", { attempt: 1 }) });
    expect(status?.description).not.toMatch(/replayed|already ran/i);
  });
});

describe("latestRecoveryNote", () => {
  it("reads every wait phase the reactor emits", () => {
    expect(
      latestRecoveryNote(
        [recoveryRow({ phase: "retry-wait", attempt: 1, maxAttempts: 4, resumeAt: T2 })],
        failedTurn,
      ),
    ).toEqual({ phase: "retry-wait", attempt: 1, maxAttempts: 4, resumeAt: T2 });
    for (const payload of [
      { phase: "quota-wait", resumeAt: null, nextCheckAt: T2 },
      { phase: "switching", fromInstanceId: "a", toInstanceId: "b" },
      { phase: "switch-failed", toInstanceId: "b", reason: "https://x.test/?token=secret" },
      { phase: "resuming", attempt: 2, maxAttempts: 4 },
    ]) {
      expect(latestRecoveryNote([recoveryRow(payload)], failedTurn)?.phase).toBe(payload.phase);
    }
  });

  it("surfaces needs-attention and gave-up, which stopRecovery emits", () => {
    expect(latestRecoveryNote([recoveryRow({ phase: "needs-attention" })], failedTurn)?.phase).toBe(
      "needs-attention",
    );
    expect(latestRecoveryNote([recoveryRow({ phase: "gave-up" })], failedTurn)?.phase).toBe(
      "gave-up",
    );
  });

  it("keeps a current wait or needs-attention through unrelated later activity", () => {
    for (const phase of ["quota-wait", "needs-attention", "gave-up"]) {
      expect(latestRecoveryNote([recoveryRow({ phase }), otherRow()], failedTurn)?.phase).toBe(
        phase,
      );
    }
  });

  it("clears on cancelled, stopped, unknown or malformed rows", () => {
    for (const payload of [{ phase: "cancelled" }, { phase: "stopped" }, { phase: "x" }, null]) {
      expect(latestRecoveryNote([recoveryRow(payload)], failedTurn)).toBeNull();
    }
    expect(latestRecoveryNote([], failedTurn)).toBeNull();
  });

  it("uses the newest recovery row, so a cancel after a wait clears it", () => {
    expect(
      latestRecoveryNote(
        [
          recoveryRow({ phase: "quota-wait" }, T1),
          otherRow(),
          recoveryRow({ phase: "cancelled" }, T2),
        ],
        failedTurn,
      ),
    ).toBeNull();
  });

  it("clears once a turn is requested after the recovery row", () => {
    const newer = { requestedAt: T2 };
    for (const phase of ["quota-wait", "resuming", "needs-attention", "gave-up"]) {
      expect(latestRecoveryNote([recoveryRow({ phase })], newer)).toBeNull();
    }
  });

  it("does not clear on a missing turn or an unreadable date", () => {
    expect(latestRecoveryNote([recoveryRow({ phase: "gave-up" })], null)?.phase).toBe("gave-up");
    expect(
      latestRecoveryNote([recoveryRow({ phase: "gave-up" })], { requestedAt: "garbage" })?.phase,
    ).toBe("gave-up");
    expect(
      latestRecoveryNote([recoveryRow({ phase: "gave-up" }, "garbage")], { requestedAt: T2 })
        ?.phase,
    ).toBe("gave-up");
  });

  it("drops invalid dates and malformed numbers from the note", () => {
    expect(
      latestRecoveryNote(
        [recoveryRow({ phase: "retry-wait", resumeAt: "soon", attempt: "2", maxAttempts: 4 })],
        failedTurn,
      ),
    ).toEqual({ phase: "retry-wait", resumeAt: null, attempt: null, maxAttempts: null });
    for (const [attempt, maxAttempts] of [
      [Number.NaN, 4],
      [0, 4],
      [1.5, 4],
      [5, 4],
      [-1, 4],
      [Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY],
    ]) {
      const result = latestRecoveryNote(
        [recoveryRow({ phase: "retry-wait", attempt, maxAttempts })],
        failedTurn,
      );
      expect(result?.attempt).toBeNull();
      expect(result?.maxAttempts).toBeNull();
    }
  });
});

describe("connectionWaitingDescription", () => {
  it("separates reconnecting from provider failure and never echoes the raw error", () => {
    const reconnecting = connectionWaitingDescription({
      phase: "reconnecting",
      error: "token=abc123",
      traceId: null,
    });
    expect(reconnecting).toContain("not a provider failure");
    expect(reconnecting).not.toContain("abc123");
  });

  it("says provider state is unknown when offline or unsupported", () => {
    for (const phase of ["offline", "error", "available", "unsupported"] as const) {
      expect(connectionWaitingDescription({ phase, error: null, traceId: null })).toContain(
        "unknown",
      );
    }
  });
});

const config = (
  capabilities: Record<string, unknown>,
  providers: ReadonlyArray<{ supportsConversationFork?: boolean }> = [],
) =>
  ({
    environment: {
      platform: { os: "darwin", arch: "arm64" },
      serverVersion: "1.2.3",
      orchestrationProtocolVersion: 7,
      capabilities,
    },
    providers,
  }) as unknown as Pick<ServerConfig, "environment" | "providers">;

describe("describeServerDetails", () => {
  it("names the relevant advertised capabilities, not a count", () => {
    const text = describeServerDetails({
      connected: true,
      serverConfig: config({ threadRecovery: true, pullRequests: true }, [
        { supportsConversationFork: true },
      ]),
    });
    expect(text).toContain("macOS arm64");
    expect(text).toContain("Server 1.2.3");
    expect(text).toContain("Conversation forks: advertised");
    expect(text).toContain("Automatic recovery: advertised");
    expect(text).not.toMatch(/\d+ optional/);
    expect(text).not.toMatch(/^Last known/);
  });

  it("says Not advertised when absent or false", () => {
    const text = describeServerDetails({
      connected: true,
      serverConfig: config({ threadRecovery: false }, [{ supportsConversationFork: false }]),
    });
    expect(text).toContain("Conversation forks: Not advertised");
    expect(text).toContain("Automatic recovery: Not advertised");
  });

  it("does not claim fork support from a descriptor that lists no providers", () => {
    const { providers: _omitted, ...descriptorOnly } = config({ threadRecovery: true });
    expect(describeServerDetails({ connected: false, serverConfig: descriptorOnly })).toContain(
      "Conversation forks: Not reported",
    );
  });

  it("labels stale config and unknown config honestly", () => {
    expect(describeServerDetails({ connected: false, serverConfig: config({}) })).toMatch(
      /^Last known: /,
    );
    expect(describeServerDetails({ connected: false, serverConfig: null })).toBe(
      "Server details unavailable until the first connection",
    );
  });
});

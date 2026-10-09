/**
 * Pure decisions for automatic provider recovery: which failures are worth
 * retrying, how long to wait, and which account may take over after a quota
 * window is exhausted. The reactor in orchestration/ owns persistence and
 * side effects; nothing here touches a clock, a provider or the database.
 *
 * @module provider/providerRecoveryPolicy
 */
import type {
  ProviderInstanceId,
  RuntimeErrorClass,
  ServerProviderUsageLimits,
} from "@t3tools/contracts";

export const RECOVERY_MAX_ATTEMPTS = 8;
export const RECOVERY_BASE_DELAY_MS = 5_000;
export const RECOVERY_MAX_DELAY_MS = 5 * 60_000;
/** Longest provider-requested wait honored for a transient failure. */
const MAX_RETRY_AFTER_MS = 60 * 60_000;
/** Quota that was not confirmed this recently is treated as unknown. */
export const QUOTA_FRESH_MS = 10 * 60_000;
/** Poll cadence while an exhausted window reports no reset time. */
export const QUOTA_UNKNOWN_RECHECK_MS = 5 * 60_000;

const TERMINAL_PATTERN =
  /unauthori[sz]ed|authenticat|forbidden|api[ -]?key|sign(?:ed)?[ -]?in|log(?:ged)?[ -]?in|permission|denied|invalid (?:request|input|model)|bad request|too long|context (?:length|window)|usage limit|quota|interrupt|cancel|abort/i;
const TRANSIENT_PATTERN =
  /econn(?:reset|refused|aborted)|etimedout|enotfound|eai_again|epipe|socket hang up|network|fetch failed|timed? ?out|overloaded|temporar(?:y|ily)|unavailable|bad gateway|gateway time|internal server error|\b(?:429|500|502|503|504|529)\b|too many requests|connection (?:reset|closed|lost|error)|stream (?:closed|ended)|disconnect/i;
const RETRY_AFTER_PATTERN =
  /(?:retry[- ]after:?|try again in)\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?|h|hours?)?/i;

export type FailureVerdict =
  | { readonly kind: "transient"; readonly retryAfterMs?: number }
  | { readonly kind: "terminal" };

function retryAfterMs(message: string): number | undefined {
  const match = RETRY_AFTER_PATTERN.exec(message);
  if (!match) return undefined;
  const amount = Number(match[1]);
  const unit = (match[2] ?? "s").toLowerCase();
  const scale =
    unit.startsWith("ms") || unit.startsWith("milli")
      ? 1
      : unit.startsWith("h")
        ? 3_600_000
        : unit.startsWith("m")
          ? 60_000
          : 1_000;
  return Number.isFinite(amount) ? Math.round(amount * scale) : undefined;
}

/**
 * Only failures that look like an outage are retried. Auth, input,
 * permission, cancellation and anything unrecognized stay with the user:
 * repeating them cannot help and may repeat a side effect.
 */
export function classifyFailure(input: {
  readonly message: string;
  readonly errorClass?: RuntimeErrorClass | undefined;
}): FailureVerdict {
  if (input.errorClass === "permission_error" || input.errorClass === "validation_error") {
    return { kind: "terminal" };
  }
  if (TERMINAL_PATTERN.test(input.message)) return { kind: "terminal" };
  if (input.errorClass !== "transport_error" && !TRANSIENT_PATTERN.test(input.message)) {
    return { kind: "terminal" };
  }
  const retryAfter = retryAfterMs(input.message);
  return retryAfter === undefined
    ? { kind: "transient" }
    : { kind: "transient", retryAfterMs: retryAfter };
}

/**
 * A turn the provider refused for an allowance, not for an outage. Codex tags
 * the failure with a typed code; Claude only words it, so its sentence is the
 * fallback. Checked before `classifyFailure`, which keeps these terminal.
 */
export function isQuotaExhaustion(input: {
  readonly message: string;
  readonly code?: string | undefined;
}): boolean {
  return (
    input.code === "usage_limit_exceeded" || /usage limit (?:reached|exceeded)/i.test(input.message)
  );
}

/** Attempt numbers start at 1. A provider's retry-after only ever lengthens the wait. */
export function backoffDelayMs(attempt: number, retryAfter?: number): number {
  const exponent = Math.min(Math.max(attempt - 1, 0), 20);
  const backoff = Math.min(RECOVERY_MAX_DELAY_MS, RECOVERY_BASE_DELAY_MS * 2 ** exponent);
  return retryAfter === undefined
    ? backoff
    : Math.min(MAX_RETRY_AFTER_MS, Math.max(backoff, retryAfter));
}

export interface QuotaAccount {
  readonly instanceId: ProviderInstanceId;
  readonly usageLimits?: ServerProviderUsageLimits | undefined;
}

type Standing =
  | { readonly state: "available"; readonly pressure: number }
  | { readonly state: "exhausted"; readonly eligibleAt: number | null }
  | { readonly state: "unknown" };

/**
 * Fresh, confirmed quota only. A stale snapshot, a failed probe or an account
 * with no windows says nothing about whether a turn would run. An account is
 * eligible again when every exhausted window has reset: the latest reset wins.
 */
function standingOf(account: QuotaAccount, now: number): Standing {
  const limits = account.usageLimits;
  if (!limits || limits.unavailable || limits.windows.length === 0) return { state: "unknown" };
  const checkedAt = Date.parse(limits.checkedAt);
  if (!Number.isFinite(checkedAt) || now - checkedAt > QUOTA_FRESH_MS) return { state: "unknown" };
  const resetAt = (resetsAt: string | undefined) =>
    resetsAt === undefined ? undefined : Date.parse(resetsAt);
  const exhausted = limits.windows.filter((window) => {
    const reset = resetAt(window.resetsAt);
    return window.usedPercent >= 100 && !(reset !== undefined && reset <= now);
  });
  if (exhausted.length === 0) {
    return {
      state: "available",
      pressure: Math.max(...limits.windows.map((window) => window.usedPercent)),
    };
  }
  const resets = exhausted.map((window) => resetAt(window.resetsAt));
  return {
    state: "exhausted",
    eligibleAt: resets.some((reset) => reset === undefined || !Number.isFinite(reset))
      ? null
      : Math.max(...(resets as number[])),
  };
}

export type RecoveryAccountDecision =
  | { readonly action: "resume"; readonly instanceId: ProviderInstanceId }
  | { readonly action: "switch"; readonly instanceId: ProviderInstanceId }
  | {
      readonly action: "wait";
      readonly instanceId: ProviderInstanceId;
      /** Epoch ms the account is eligible again; null when no reset is known. */
      readonly until: number | null;
    };

/**
 * `current` is the account whose turn stopped; `others` are enabled accounts of
 * the same driver that offer the same model. Prefer an account that can run
 * now, otherwise wait for whichever becomes eligible first.
 */
export function chooseRecoveryAccount(input: {
  readonly current: QuotaAccount;
  readonly others: ReadonlyArray<QuotaAccount>;
  readonly now: number;
  /** The adapter just reported a refusal: bars that still read below full do not override it. */
  readonly currentBlocked?: boolean;
}): RecoveryAccountDecision {
  const standings: Array<{ account: QuotaAccount; standing: Standing }> = [
    input.current,
    ...input.others,
  ].map((account) => ({ account, standing: standingOf(account, input.now) }));
  const [currentEntry, ...otherEntries] = standings;
  if (input.currentBlocked && currentEntry!.standing.state !== "exhausted") {
    currentEntry!.standing = { state: "exhausted", eligibleAt: null };
  }
  if (currentEntry!.standing.state === "available") {
    return { action: "resume", instanceId: input.current.instanceId };
  }
  let best: { instanceId: ProviderInstanceId; pressure: number } | undefined;
  for (const { account, standing } of otherEntries) {
    if (
      standing.state === "available" &&
      (best === undefined || standing.pressure < best.pressure)
    ) {
      best = { instanceId: account.instanceId, pressure: standing.pressure };
    }
  }
  if (best) return { action: "switch", instanceId: best.instanceId };
  let earliest: { instanceId: ProviderInstanceId; until: number } | undefined;
  for (const { account, standing } of standings) {
    if (
      standing.state === "exhausted" &&
      standing.eligibleAt !== null &&
      (earliest === undefined || standing.eligibleAt < earliest.until)
    ) {
      earliest = { instanceId: account.instanceId, until: standing.eligibleAt };
    }
  }
  return earliest
    ? { action: "wait", instanceId: earliest.instanceId, until: earliest.until }
    : { action: "wait", instanceId: input.current.instanceId, until: null };
}

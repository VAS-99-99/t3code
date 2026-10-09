import { ProviderInstanceId, type ServerProviderUsageLimits } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  backoffDelayMs,
  chooseRecoveryAccount,
  classifyFailure,
  RECOVERY_MAX_DELAY_MS,
  type QuotaAccount,
} from "./providerRecoveryPolicy.ts";

const NOW = DateTime.makeUnsafe("2026-10-09T10:00:00.000Z").epochMilliseconds;
const iso = (offsetMs: number) => DateTime.formatIso(DateTime.makeUnsafe(NOW + offsetMs));
const HOUR = 3_600_000;

const limits = (
  windows: ReadonlyArray<{ id: string; usedPercent: number; resetsInMs?: number | undefined }>,
  checkedAtOffsetMs = -60_000,
): ServerProviderUsageLimits => ({
  checkedAt: iso(checkedAtOffsetMs),
  windows: windows.map((window) => ({
    id: window.id,
    kind: window.id === "five_hour" ? "session" : "weekly",
    label: window.id,
    usedPercent: window.usedPercent,
    ...(window.resetsInMs === undefined ? {} : { resetsAt: iso(window.resetsInMs) }),
  })),
});

const account = (name: string, usageLimits?: ServerProviderUsageLimits): QuotaAccount => ({
  instanceId: ProviderInstanceId.make(name),
  usageLimits,
});

describe("classifyFailure", () => {
  it("retries network and provider outages", () => {
    for (const message of [
      "read ECONNRESET",
      "fetch failed",
      "API Error: 529 Overloaded",
      "503 Service Unavailable",
      "stream disconnected before completion",
    ]) {
      expect(classifyFailure({ message }).kind, message).toBe("transient");
    }
    expect(classifyFailure({ message: "boom", errorClass: "transport_error" }).kind).toBe(
      "transient",
    );
  });

  it("never retries auth, input, permission, cancellation or unknown failures", () => {
    for (const message of [
      "401 Unauthorized: invalid api key",
      "Please sign in again",
      "Permission denied by user",
      "prompt is too long",
      "Turn was interrupted",
      "something odd happened",
    ]) {
      expect(classifyFailure({ message }).kind, message).toBe("terminal");
    }
    expect(classifyFailure({ message: "timeout", errorClass: "permission_error" }).kind).toBe(
      "terminal",
    );
    expect(classifyFailure({ message: "ECONNRESET", errorClass: "validation_error" }).kind).toBe(
      "terminal",
    );
  });

  it("reads the provider's retry-after", () => {
    expect(classifyFailure({ message: "503 Service Unavailable. Retry-After: 30" })).toEqual({
      kind: "transient",
      retryAfterMs: 30_000,
    });
    expect(classifyFailure({ message: "overloaded, try again in 2 minutes" })).toEqual({
      kind: "transient",
      retryAfterMs: 120_000,
    });
  });
});

describe("backoffDelayMs", () => {
  it("doubles from five seconds and caps", () => {
    expect([1, 2, 3, 4].map((attempt) => backoffDelayMs(attempt))).toEqual([
      5_000, 10_000, 20_000, 40_000,
    ]);
    expect(backoffDelayMs(30)).toBe(RECOVERY_MAX_DELAY_MS);
  });

  it("honors a longer retry-after and never shortens the backoff", () => {
    expect(backoffDelayMs(1, 90_000)).toBe(90_000);
    expect(backoffDelayMs(3, 1_000)).toBe(20_000);
  });
});

describe("chooseRecoveryAccount", () => {
  const exhausted = (name: string, resets: ReadonlyArray<number | undefined>) =>
    account(
      name,
      limits(
        resets.map((resetsInMs, index) => ({
          id: index === 0 ? "five_hour" : "seven_day",
          usedPercent: 100,
          resetsInMs,
        })),
      ),
    );

  it("switches at once to a fresh account with headroom", () => {
    const decision = chooseRecoveryAccount({
      current: exhausted("a", [HOUR]),
      others: [
        exhausted("b", [2 * HOUR]),
        account("c", limits([{ id: "five_hour", usedPercent: 40, resetsInMs: HOUR }])),
      ],
      now: NOW,
    });
    expect(decision).toEqual({ action: "switch", instanceId: "c" });
  });

  it("uses the latest reset among an account's exhausted windows, then the earliest account", () => {
    const decision = chooseRecoveryAccount({
      // a: five_hour back in 1h but the week is gone for 30h -> eligible in 30h
      current: exhausted("a", [HOUR, 30 * HOUR]),
      others: [
        exhausted("b", [2 * HOUR, 10 * HOUR]), // eligible in 10h
        exhausted("c", [5 * HOUR]), // eligible in 5h
      ],
      now: NOW,
    });
    expect(decision).toEqual({ action: "wait", instanceId: "c", until: NOW + 5 * HOUR });
  });

  it("stays on the current account when it recovers first", () => {
    const decision = chooseRecoveryAccount({
      current: exhausted("a", [HOUR]),
      others: [exhausted("b", [3 * HOUR])],
      now: NOW,
    });
    expect(decision).toEqual({ action: "wait", instanceId: "a", until: NOW + HOUR });
  });

  it("ignores stale, unavailable and windowless quota", () => {
    const stale = account("b", limits([{ id: "five_hour", usedPercent: 0 }], -11 * 60_000));
    const probeFailed = account("c", {
      checkedAt: iso(-1000),
      windows: [],
      unavailable: { reason: "probeFailed" },
    });
    const empty = account("d", limits([]));
    const missing = account("e");
    const decision = chooseRecoveryAccount({
      current: exhausted("a", [HOUR]),
      others: [stale, probeFailed, empty, missing],
      now: NOW,
    });
    expect(decision).toEqual({ action: "wait", instanceId: "a", until: NOW + HOUR });
  });

  it("waits without a countdown when no exhausted window reports its reset", () => {
    const decision = chooseRecoveryAccount({
      current: exhausted("a", [undefined]),
      others: [exhausted("b", [HOUR, undefined])],
      now: NOW,
    });
    expect(decision).toEqual({ action: "wait", instanceId: "a", until: null });
  });

  it("treats a window whose reset already passed as available again", () => {
    const decision = chooseRecoveryAccount({
      current: exhausted("a", [HOUR]),
      others: [exhausted("b", [-1000])],
      now: NOW,
    });
    expect(decision).toEqual({ action: "switch", instanceId: "b" });
  });

  it("trusts an explicit refusal over bars that still read below full", () => {
    const decision = chooseRecoveryAccount({
      current: account("a", limits([{ id: "five_hour", usedPercent: 40, resetsInMs: HOUR }])),
      others: [exhausted("b", [2 * HOUR])],
      now: NOW,
      currentBlocked: true,
    });
    expect(decision).toEqual({ action: "wait", instanceId: "b", until: NOW + 2 * HOUR });
    expect(
      chooseRecoveryAccount({
        current: account("a", limits([{ id: "five_hour", usedPercent: 40, resetsInMs: HOUR }])),
        others: [account("c", limits([{ id: "five_hour", usedPercent: 5, resetsInMs: HOUR }]))],
        now: NOW,
        currentBlocked: true,
      }),
    ).toEqual({ action: "switch", instanceId: "c" });
  });

  it("keeps the current account when its own fresh quota is back", () => {
    const decision = chooseRecoveryAccount({
      current: account("a", limits([{ id: "five_hour", usedPercent: 10, resetsInMs: HOUR }])),
      others: [account("b", limits([{ id: "five_hour", usedPercent: 0, resetsInMs: HOUR }]))],
      now: NOW,
    });
    expect(decision).toEqual({ action: "resume", instanceId: "a" });
  });
});

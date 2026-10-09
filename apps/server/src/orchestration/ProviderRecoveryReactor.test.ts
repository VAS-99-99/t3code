import { it } from "@effect/vitest";
import { CommandId, MessageId, ThreadId, TurnId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { expect } from "vite-plus/test";

import { ProviderAdapterRequestError } from "../provider/Errors.ts";
import {
  A,
  B,
  C,
  CLAUDE,
  D,
  dropWorld,
  HOUR,
  iso,
  makeWorld,
  startServer,
  T0,
  type Quota,
  type Server,
  type World,
} from "./providerRecoveryHarness.testUtils.ts";

/** Runs one scenario on a fresh world, always closing the servers and removing the files. */
const scenario = (name: string, body: (server: Server, world: World) => Effect.Effect<void>) =>
  it.effect(name, () =>
    Effect.gen(function* () {
      const world = makeWorld();
      const server = yield* startServer(world);
      yield* body(server, world).pipe(
        Effect.ensuring(server.close.pipe(Effect.andThen(Effect.sync(() => dropWorld(world))))),
      );
    }),
  );

const exhausted = (
  resets: ReadonlyArray<readonly ["five_hour" | "seven_day", number | undefined]>,
): Quota => ({
  read: "fresh",
  windows: resets.map(([id, resetsAt]) => ({ id, usedPercent: 100, resetsAt })),
});
const open = (usedPercent = 10): Quota => ({
  read: "fresh",
  windows: [{ id: "five_hour", usedPercent, resetsAt: T0 + 4 * HOUR }],
});
let stops = 0;
const interrupt = (server: Server, threadId: ThreadId) =>
  server.dispatch({
    type: "thread.turn.interrupt",
    commandId: CommandId.make(`cmd-stop-${threadId}-${++stops}`),
    threadId,
    createdAt: iso(T0),
  });
const sendsOn = (server: Server, id: typeof A) =>
  server.fakes[id]!.sendTurn.mock.calls.map(([turn]) => turn.input ?? "(continuation)");
const phases = (server: Server, id: ThreadId) =>
  server.recoveryRows(id).pipe(Effect.map((rows) => rows.map((row) => row.phase)));
const FAIL_QUOTA = "Claude usage limit reached. Send the message again once the limit resets.";

// --- transient failures ------------------------------------------------------

scenario("a transient outage resumes the same session after the backoff and no earlier", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-transient");
    yield* server.startTurn(id, "original prompt");
    yield* server.failTurn(id, "read ECONNRESET");

    expect(Option.getOrThrow(yield* server.intent(id))).toMatchObject({
      kind: "transient",
      status: "waiting",
      attempt: 1,
      instanceId: A,
    });
    expect(yield* phases(server, id)).toEqual(["retry-wait"]);

    yield* server.advance(4_000);
    expect(sendsOn(server, A)).toEqual(["original prompt"]);

    yield* server.advance(1_000);
    // The original prompt ran once; the continuation does not replay it.
    expect(sendsOn(server, A)).toEqual(["original prompt", "Continue where you left off."]);
    expect(Option.getOrThrow(yield* server.intent(id)).status).toBe("resuming");

    yield* server.completeTurn(id);
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect(yield* phases(server, id)).toEqual(["retry-wait", "resuming"]);
    expect(server.fakes[A]!.startSession).toHaveBeenCalledTimes(1);
  }),
);

scenario("the backoff doubles, honors retry-after and gives up after the cap", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-backoff");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "503 Service Unavailable. Retry-After: 90");
    expect(Option.getOrThrow(yield* server.intent(id)).wakeAt).toBe(iso(T0 + 90_000));
    yield* server.advance(90_000);
    expect(sendsOn(server, A)).toHaveLength(2);

    // Each further failure waits longer, capped at five minutes.
    const waits: number[] = [];
    for (let attempt = 2; attempt <= 8; attempt++) {
      const before = T0 + 90_000 + waits.reduce((sum, wait) => sum + wait, 0);
      yield* server.failTurn(id, "read ECONNRESET");
      const intent = Option.getOrThrow(yield* server.intent(id));
      expect(intent.attempt).toBe(attempt);
      const wait = Math.min(300_000, 5_000 * 2 ** (attempt - 1));
      expect(Date.parse(intent.wakeAt!)).toBeGreaterThanOrEqual(before);
      waits.push(wait);
      yield* server.advance(wait);
    }
    expect(sendsOn(server, A)).toHaveLength(9);
    yield* server.failTurn(id, "read ECONNRESET");
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect((yield* phases(server, id)).at(-1)).toBe("gave-up");
    yield* server.advance(24 * HOUR);
    expect(sendsOn(server, A)).toHaveLength(9);
  }),
);

scenario("auth, permission, input and unknown failures are never retried", (server) =>
  Effect.gen(function* () {
    for (const [index, message] of [
      "401 Unauthorized: invalid api key",
      "Permission denied by user",
      "prompt is too long",
      "something odd happened",
    ].entries()) {
      const id = yield* server.createThread(`t-terminal-${index}`);
      yield* server.startTurn(id, "work");
      yield* server.failTurn(id, message, "provider_error");
      expect(Option.isNone(yield* server.intent(id)), message).toBe(true);
    }
    yield* server.advance(24 * HOUR);
    expect(sendsOn(server, A)).toHaveLength(4);
  }),
);

scenario("a turn that is running again by itself is never given a second turn", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-active");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "read ECONNRESET");
    expect(Option.isSome(yield* server.intent(id))).toBe(true);

    // The provider's own session picks the work back up before the timer fires.
    yield* server.startTurn(id, "provider-side retry");
    yield* server.advance(5_000);
    expect(sendsOn(server, A)).toEqual(["work", "provider-side retry"]);
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
  }),
);

// --- quota exhaustion --------------------------------------------------------

scenario("a window that parks the turn inside the provider is left alone", (server, world) =>
  Effect.gen(function* () {
    world.quota.set(A, exhausted([["five_hour", T0 + 2 * HOUR]]));
    world.quota.set(B, exhausted([["five_hour", T0 + 3 * HOUR]]));
    world.quota.set(C, exhausted([["five_hour", T0 + 4 * HOUR]]));
    const id = yield* server.createThread("t-parked");
    yield* server.startTurn(id, "work");

    yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + 2 * HOUR });
    // The parked window re-reports itself as its wait shrinks: one look, not a probe each time.
    const probesAfterFirst = server.world.probes.length;
    for (let repeat = 0; repeat < 3; repeat++) {
      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + 2 * HOUR });
    }
    expect(server.world.probes).toHaveLength(probesAfterFirst);
    // The SDK retries internally and keeps the turn open.
    server.fakes[A]!.emit({
      type: "session.state.changed",
      threadId: id,
      payload: { state: "running", reason: "api_retry:2/10" },
    });
    yield* server.advance(3 * HOUR);

    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect(sendsOn(server, A)).toEqual(["work"]);
    expect(server.fakes[B]!.forkThread).not.toHaveBeenCalled();
    expect(yield* server.recoveryRows(id)).toEqual([]);

    // The window reopens and the SDK finishes the very same turn.
    yield* server.completeTurn(id);
    expect(sendsOn(server, A)).toEqual(["work"]);
  }),
);

scenario(
  "one exhausted account hands the conversation to an account with headroom",
  (server, world) =>
    Effect.gen(function* () {
      world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
      world.quota.set(B, open(20));
      const id = yield* server.createThread("t-switch");
      yield* server.startTurn(id, "original prompt");
      const sourceCursor = Option.getOrThrow(yield* server.binding(id)).resumeCursor;

      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
      yield* server.advance(0);

      const moved = Option.getOrThrow(yield* server.binding(id));
      expect(moved.providerInstanceId).toBe(B);
      // B copied the history through its own configuration; A's session is closed untouched.
      expect(server.fakes[B]!.forkThread.mock.calls[0]![0]).toMatchObject({
        sourceThreadId: id,
        destinationThreadId: id,
        sourceResumeCursor: sourceCursor,
      });
      expect(server.fakes[A]!.forkThread).not.toHaveBeenCalled();
      expect(server.fakes[A]!.sessions.has(id)).toBe(false);
      expect((yield* server.shell(id)).pipe(Option.map((shell) => shell.instanceId))).toEqual(
        Option.some(B),
      );
      // The new account resumes the copied conversation and continues; nothing is replayed.
      expect(server.fakes[B]!.startSession.mock.calls.at(-1)![0].resumeCursor).toEqual(
        moved.resumeCursor,
      );
      expect(sendsOn(server, A)).toEqual(["original prompt"]);
      expect(sendsOn(server, B)).toEqual(["Continue where you left off."]);
      expect(yield* phases(server, id)).toEqual(["switching"]);

      yield* server.completeTurn(id);
      expect(Option.isNone(yield* server.intent(id))).toBe(true);
    }),
);

scenario(
  "when every account is out, the wait ends at the earliest reset and rechecks quota",
  (server, world) =>
    Effect.gen(function* () {
      world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
      world.quota.set(B, exhausted([["five_hour", T0 + 2 * HOUR]]));
      world.quota.set(C, exhausted([["five_hour", T0 + 3 * HOUR]]));
      const id = yield* server.createThread("t-all-out");
      yield* server.startTurn(id, "work");
      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
      yield* server.failTurn(id, FAIL_QUOTA, "provider_error");

      const intent = Option.getOrThrow(yield* server.intent(id));
      expect(intent).toMatchObject({ kind: "quota", targetInstanceId: null });
      expect(intent.wakeAt).toBe(iso(T0 + HOUR));
      expect((yield* server.recoveryRows(id))[0]!.summary).toBe(
        "Usage limit reached. Resuming when it resets in 1h.",
      );

      // The provider extends the wait before the wake: the wake rechecks, it does not trust the old reset.
      world.quota.set(A, exhausted([["five_hour", T0 + 2 * HOUR]]));
      yield* server.advance(HOUR);
      expect(sendsOn(server, A)).toEqual(["work"]);
      expect(Option.getOrThrow(yield* server.intent(id)).wakeAt).toBe(iso(T0 + 2 * HOUR));

      yield* server.advance(HOUR);
      expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
      expect(server.fakes[B]!.forkThread).not.toHaveBeenCalled();
    }),
);

scenario(
  "an account is eligible at the latest of its exhausted windows, the pool at the earliest",
  (server, world) =>
    Effect.gen(function* () {
      // A: the five-hour window reopens in 1h but the weekly one only in 30h.
      world.quota.set(
        A,
        exhausted([
          ["five_hour", T0 + HOUR],
          ["seven_day", T0 + 30 * HOUR],
        ]),
      );
      world.quota.set(B, exhausted([["five_hour", T0 + 5 * HOUR]]));
      world.quota.set(
        C,
        exhausted([
          ["five_hour", T0 + 9 * HOUR],
          ["seven_day", T0 + 12 * HOUR],
        ]),
      );
      const id = yield* server.createThread("t-windows");
      yield* server.startTurn(id, "work");
      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
      yield* server.failTurn(id, FAIL_QUOTA, "provider_error");

      const intent = Option.getOrThrow(yield* server.intent(id));
      expect(intent.targetInstanceId).toBe(B);
      expect(intent.wakeAt).toBe(iso(T0 + 5 * HOUR));

      // Nothing happens when A's five-hour window alone reopens.
      yield* server.advance(HOUR + 1);
      expect(server.fakes[B]!.forkThread).not.toHaveBeenCalled();
      expect(sendsOn(server, A)).toEqual(["work"]);

      yield* server.advance(4 * HOUR);
      expect(Option.getOrThrow(yield* server.binding(id)).providerInstanceId).toBe(B);
      expect(sendsOn(server, B)).toEqual(["Continue where you left off."]);
    }),
);

scenario("stale, failed and missing quota never count as headroom", (server, world) =>
  Effect.gen(function* () {
    world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
    world.quota.set(B, { read: "stale", windows: [{ id: "five_hour", usedPercent: 0 }] });
    world.quota.set(C, { read: "failed", windows: [] });
    const id = yield* server.createThread("t-stale");
    yield* server.startTurn(id, "work");
    yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
    yield* server.advance(0);
    expect(server.fakes[B]!.forkThread).not.toHaveBeenCalled();
    expect(server.fakes[C]!.forkThread).not.toHaveBeenCalled();
    expect(Option.isNone(yield* server.intent(id))).toBe(true); // still parked in the SDK

    yield* server.failTurn(id, FAIL_QUOTA, "provider_error");
    expect(Option.getOrThrow(yield* server.intent(id)).targetInstanceId).toBeNull();
    // Every sibling was probed before it was judged.
    expect(new Set(server.world.probes)).toEqual(new Set([A, B, C, D].filter((x) => x !== D)));
    yield* server.advance(HOUR);
    expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
    expect(Option.getOrThrow(yield* server.binding(id)).providerInstanceId).toBe(A);
  }),
);

scenario(
  "an exhausted window with no reset time waits visibly and rechecks every five minutes",
  (server, world) =>
    Effect.gen(function* () {
      world.quota.set(A, exhausted([["five_hour", undefined]]));
      world.quota.set(B, exhausted([["five_hour", undefined]]));
      const id = yield* server.createThread("t-no-reset");
      yield* server.startTurn(id, "work");
      yield* server.rejectWindow(id, { id: "five_hour" });
      yield* server.failTurn(id, FAIL_QUOTA, "provider_error");

      expect(Option.getOrThrow(yield* server.intent(id)).wakeAt).toBeNull();
      expect((yield* server.recoveryRows(id))[0]!.summary).toContain("did not say when it resets");

      yield* server.advance(5 * 60_000);
      yield* server.advance(5 * 60_000);
      expect(sendsOn(server, A)).toEqual(["work"]);
      // A recheck that changes nothing stays quiet.
      expect(yield* phases(server, id)).toEqual(["quota-wait"]);

      world.quota.set(A, open(5));
      yield* server.advance(5 * 60_000);
      expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
    }),
);

scenario(
  "only enabled accounts of the same driver offering the same model are considered",
  (server, world) =>
    Effect.gen(function* () {
      world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
      world.quota.set(B, open(0));
      world.enabled.set(B, false);
      world.quota.set(C, open(0));
      world.models.set(C, ["some-other-model"]);
      world.quota.set(D, open(0)); // a Codex account, different driver
      const id = yield* server.createThread("t-incompatible");
      yield* server.startTurn(id, "work");
      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
      yield* server.failTurn(id, FAIL_QUOTA, "provider_error");

      for (const other of [B, C, D]) expect(server.fakes[other]!.forkThread).not.toHaveBeenCalled();
      expect(Option.getOrThrow(yield* server.intent(id))).toMatchObject({
        instanceId: A,
        targetInstanceId: null,
      });
      yield* server.advance(HOUR);
      expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
      expect(CLAUDE).toBeDefined();
    }),
);

scenario("a configuration signed in as the same account is not another account", (server, world) =>
  Effect.gen(function* () {
    world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
    world.quota.set(B, open(0));
    world.emails.set(A, "person@example.com");
    world.emails.set(B, "person@example.com");
    world.quota.set(C, open(0));
    world.emails.set(C, "someone-else@example.com");
    const id = yield* server.createThread("t-same-account");
    yield* server.startTurn(id, "work");
    yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
    yield* server.advance(0);

    expect(server.fakes[B]!.forkThread).not.toHaveBeenCalled();
    expect(server.fakes[C]!.forkThread).toHaveBeenCalledTimes(1);
    expect(Option.getOrThrow(yield* server.binding(id)).providerInstanceId).toBe(C);
  }),
);

scenario(
  "a target that cannot read the conversation leaves it on its account and waits",
  (server, world) =>
    Effect.gen(function* () {
      world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
      world.quota.set(B, open(0));
      server.fakes[B]!.forkThread.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: CLAUDE,
            method: "thread/fork",
            detail: "Session was not found in this account's configuration.",
          }),
        ),
      );
      const id = yield* server.createThread("t-transfer-fails");
      yield* server.startTurn(id, "work");
      yield* server.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
      yield* server.advance(0);

      expect(Option.getOrThrow(yield* server.binding(id)).providerInstanceId).toBe(A);
      expect((yield* server.shell(id)).pipe(Option.map((shell) => shell.instanceId))).toEqual(
        Option.some(A),
      );
      expect(server.fakes[B]!.sendTurn).not.toHaveBeenCalled();
      expect(yield* phases(server, id)).toEqual(["switching", "switch-failed", "quota-wait"]);
      const waiting = Option.getOrThrow(yield* server.intent(id));
      expect(waiting.excluded).toEqual([B]);
      expect(waiting.wakeAt).toBe(iso(T0 + HOUR));

      yield* server.advance(HOUR);
      expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
      expect(server.fakes[B]!.forkThread).toHaveBeenCalledTimes(1);
    }),
);

// --- cancellation ------------------------------------------------------------

scenario("Stop before the wake cancels the intent and nothing runs late", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-stop");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "read ECONNRESET");
    yield* server.advance(4_999);

    yield* interrupt(server, id);
    yield* server.settle;
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect((yield* phases(server, id)).at(-1)).toBe("cancelled");

    yield* server.advance(1);
    yield* server.advance(24 * HOUR);
    expect(sendsOn(server, A)).toEqual(["work"]);
  }),
);

scenario("Stop during a resumed turn interrupts it and clears the intent", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-stop-resumed");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "read ECONNRESET");
    yield* server.advance(5_000);
    expect(Option.getOrThrow(yield* server.intent(id)).status).toBe("resuming");

    yield* interrupt(server, id);
    yield* server.settle;
    expect(server.fakes[A]!.interruptTurn.mock.calls.map(([threadId]) => threadId)).toEqual([id]);
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    // The interrupted turn's own ending does not schedule anything.
    yield* server.advance(24 * HOUR);
    expect(sendsOn(server, A)).toHaveLength(2);
  }),
);

scenario("Stop while a continuation is starting prevents a late failure from rearming", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-stop-starting");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "read ECONNRESET");
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    server.fakes[A]!.sendTurn.mockImplementationOnce(() =>
      Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
        return yield* Effect.fail(
          new ProviderAdapterRequestError({
            provider: CLAUDE,
            method: "sendTurn",
            detail: "read ECONNRESET",
          }),
        );
      }),
    );
    yield* server.advance(5_000);
    yield* Deferred.await(entered);
    yield* interrupt(server, id);
    yield* server.settle;
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    yield* Deferred.succeed(release, undefined);
    yield* server.settle;
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    yield* server.advance(24 * HOUR);
    expect(sendsOn(server, A)).toHaveLength(2);
  }),
);

scenario("a continuation accepted without a reply is parked instead of repeated", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-accepted-no-reply");
    yield* server.startTurn(id, "work");
    yield* server.failTurn(id, "read ECONNRESET");
    let accepted = 0;
    server.fakes[A]!.sendTurn.mockImplementation(() =>
      Effect.gen(function* () {
        accepted++;
        return yield* Effect.fail(
          new ProviderAdapterRequestError({
            provider: CLAUDE,
            method: "sendTurn",
            detail: "read ECONNRESET",
          }),
        );
      }),
    );
    yield* server.advance(5_000);
    yield* server.advance(10_000);
    expect(accepted).toBe(1);
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect((yield* server.recoveryRows(id)).at(-1)!.phase).toBe("needs-attention");
  }),
);

scenario("a new message, archiving, or turning the flag off also cancels", (server) =>
  Effect.gen(function* () {
    const flagged = yield* server.createThread("t-flag");
    const archived = yield* server.createThread("t-archive");
    const messaged = yield* server.createThread("t-message");
    for (const id of [flagged, archived, messaged]) {
      yield* server.startTurn(id, "work");
      yield* server.failTurn(id, "read ECONNRESET");
      expect(Option.isSome(yield* server.intent(id))).toBe(true);
    }
    yield* server.dispatch({
      type: "thread.recovery.set",
      commandId: CommandId.make("cmd-flag-off"),
      threadId: flagged,
      enabled: false,
    });
    yield* server.dispatch({
      type: "thread.archive",
      commandId: CommandId.make("cmd-archive"),
      threadId: archived,
    });
    yield* server.dispatch({
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-message"),
      threadId: messaged,
      message: {
        messageId: MessageId.make("m-new"),
        role: "user",
        text: "actually, do this instead",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: iso(T0),
    });
    yield* server.settle;
    for (const id of [flagged, archived, messaged]) {
      expect(Option.isNone(yield* server.intent(id)), id).toBe(true);
    }
    yield* server.advance(HOUR);
    expect(sendsOn(server, A)).toHaveLength(3);
  }),
);

scenario(
  "flag off, archive, delete or a new message while a continuation starts also fence a late failure",
  (server) =>
    Effect.gen(function* () {
      const cancellers = {
        flagOff: (threadId: ThreadId) =>
          server.dispatch({
            type: "thread.recovery.set",
            commandId: CommandId.make("cmd-late-flag"),
            threadId,
            enabled: false,
          }),
        archive: (threadId: ThreadId) =>
          server.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("cmd-late-archive"),
            threadId,
          }),
        delete: (threadId: ThreadId) =>
          server.dispatch({
            type: "thread.delete",
            commandId: CommandId.make("cmd-late-delete"),
            threadId,
          }),
        newMessage: (threadId: ThreadId) =>
          server.dispatch({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-late-message"),
            threadId,
            message: {
              messageId: MessageId.make("m-late"),
              role: "user",
              text: "do this instead",
              attachments: [],
            },
            runtimeMode: "full-access",
            interactionMode: "default",
            createdAt: iso(T0),
          }),
      };
      for (const [name, cancelWith] of Object.entries(cancellers)) {
        const id = yield* server.createThread(`t-late-${name}`);
        yield* server.startTurn(id, "work");
        yield* server.failTurn(id, "read ECONNRESET");
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        server.fakes[A]!.sendTurn.mockImplementationOnce(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return yield* Effect.fail(
              new ProviderAdapterRequestError({
                provider: CLAUDE,
                method: "sendTurn",
                detail: "read ECONNRESET",
              }),
            );
          }),
        );
        yield* server.advance(5_000);
        yield* Deferred.await(entered);
        yield* cancelWith(id);
        yield* server.settle;
        yield* Deferred.succeed(release, undefined);
        yield* server.settle;
        expect(Option.isNone(yield* server.intent(id)), name).toBe(true);
      }
    }),
);

// --- restart -----------------------------------------------------------------

it.effect("a restart while waiting resumes once, even with the old process still alive", () =>
  Effect.gen(function* () {
    const world = makeWorld();
    const first = yield* startServer(world);
    const id = yield* first.createThread("t-restart");
    yield* first.startTurn(id, "work");
    yield* first.failTurn(id, "read ECONNRESET");
    const persisted = Option.getOrThrow(yield* first.intent(id));

    // The new process starts while the old one has not exited yet and holds the same intent.
    const second = yield* startServer(world, { at: T0 + 2_000 });
    expect(Option.getOrThrow(yield* second.intent(id)).token).toBe(persisted.token);

    // Both timers fire. The old process is slow to read the provider's history, so
    // both wakes have seen the intent waiting before either one claims it.
    const gate = yield* Deferred.make<void>();
    first.fakes[A]!.readThread.mockImplementationOnce((threadId) =>
      Deferred.await(gate).pipe(Effect.as({ threadId, turns: [] })),
    );
    yield* second.advance(3_000);
    expect(sendsOn(second, A)).toEqual(["Continue where you left off."]);
    yield* Deferred.succeed(gate, undefined);
    yield* first.settle;

    const continuations = [first, second].flatMap((server) =>
      sendsOn(server, A).filter((input) => input === "Continue where you left off."),
    );
    expect(continuations).toHaveLength(1);
    expect((yield* phases(second, id)).filter((phase) => phase === "resuming")).toHaveLength(1);

    yield* first.close;
    yield* second.close;
    dropWorld(world);
  }),
);

it.effect("a restart in the middle of a resumed turn never repeats the continuation", () =>
  Effect.gen(function* () {
    const world = makeWorld();
    const first = yield* startServer(world);
    const id = yield* first.createThread("t-restart-resuming");
    yield* first.startTurn(id, "work");
    yield* first.failTurn(id, "read ECONNRESET");
    yield* first.advance(5_000);
    expect(Option.getOrThrow(yield* first.intent(id)).status).toBe("resuming");
    yield* first.close;

    const second = yield* startServer(world, { at: T0 + 6_000 });
    yield* second.settle;
    expect(Option.isNone(yield* second.intent(id))).toBe(true);
    expect((yield* phases(second, id)).at(-1)).toBe("needs-attention");
    yield* second.advance(24 * HOUR);
    expect(sendsOn(second, A)).toEqual([]);
    yield* second.close;
    dropWorld(world);
  }),
);

it.effect("a crash after the target accepted a native copy is surfaced, not retried blind", () =>
  Effect.gen(function* () {
    const world = makeWorld();
    world.quota.set(A, exhausted([["five_hour", T0 + HOUR]]));
    world.quota.set(B, open(0));
    const first = yield* startServer(world);
    const id = yield* first.createThread("t-crash-transfer");
    yield* first.startTurn(id, "work");
    // The source cannot be closed, as if the process died right after B accepted the copy.
    first.fakes[A]!.stopSession.mockImplementation(() => Effect.never);
    yield* first.rejectWindow(id, { id: "five_hour", resetsAt: T0 + HOUR });
    yield* first.advance(0);
    const staged = Option.getOrThrow(yield* first.intent(id));
    expect(staged.status).toBe("resuming");
    expect(staged.stagedCursor).not.toBeNull();
    expect(first.fakes[B]!.forkThread).toHaveBeenCalledTimes(1);
    yield* first.close;

    const second = yield* startServer(world, { at: T0 + 10_000 });
    yield* second.settle;
    expect(Option.isNone(yield* second.intent(id))).toBe(true);
    expect((yield* phases(second, id)).at(-1)).toBe("needs-attention");
    yield* second.advance(24 * HOUR);
    expect(sendsOn(second, A)).toEqual([]);
    expect(sendsOn(second, B)).toEqual([]);
    expect(Option.getOrThrow(yield* second.binding(id)).providerInstanceId).toBe(A);
    yield* second.close;
    dropWorld(world);
  }),
);

// --- reconciliation ----------------------------------------------------------

scenario("a tool call that started but never reported back is not repeated", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-tool");
    yield* server.startTurn(id, "work");
    server.fakes[A]!.emit({
      type: "item.started",
      threadId: id,
      turnId: TurnId.make("claude-a-turn-1"),
      itemId: "tool-1",
      payload: { itemType: "command_execution", title: "Run the migration" },
    });
    yield* server.settle;
    yield* server.failTurn(id, "read ECONNRESET");

    yield* server.advance(5_000);
    expect(sendsOn(server, A)).toEqual(["work"]);
    expect(Option.isNone(yield* server.intent(id))).toBe(true);
    expect(yield* server.recoveryRows(id)).toEqual([
      expect.objectContaining({ phase: "retry-wait" }),
      expect.objectContaining({
        phase: "needs-attention",
        summary: expect.stringContaining("Repeating it could duplicate an action that already ran"),
      }),
    ]);
  }),
);

scenario("a finished tool call does not block the continuation", (server) =>
  Effect.gen(function* () {
    const id = yield* server.createThread("t-tool-done");
    yield* server.startTurn(id, "work");
    for (const type of ["item.started", "item.completed"]) {
      server.fakes[A]!.emit({
        type,
        threadId: id,
        turnId: TurnId.make("claude-a-turn-1"),
        itemId: "tool-1",
        payload: { itemType: "command_execution", title: "Run the migration", status: "completed" },
      });
    }
    yield* server.settle;
    yield* server.failTurn(id, "read ECONNRESET");
    yield* server.advance(5_000);
    expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
  }),
);

scenario(
  "a provider history that ends inside an item, or cannot be read, stops the retry",
  (server) =>
    Effect.gen(function* () {
      const inFlight = yield* server.createThread("t-native-open");
      const unreadable = yield* server.createThread("t-native-unreadable");
      for (const id of [inFlight, unreadable]) {
        yield* server.startTurn(id, "work");
        yield* server.failTurn(id, "read ECONNRESET");
      }
      server.fakes[A]!.readThread.mockImplementation((threadId) =>
        threadId === inFlight
          ? Effect.succeed({
              threadId,
              turns: [
                {
                  id: TurnId.make("t1"),
                  items: [{ type: "commandExecution", status: "inProgress" }],
                },
              ],
            })
          : (Effect.fail(
              new ProviderAdapterRequestError({
                provider: CLAUDE,
                method: "thread/read",
                detail: "history unavailable",
              }),
            ) as never),
      );
      yield* server.advance(5_000);
      expect(sendsOn(server, A)).toHaveLength(2); // one prompt per thread, no continuation
      expect((yield* server.recoveryRows(inFlight)).at(-1)!.phase).toBe("needs-attention");
      expect((yield* server.recoveryRows(unreadable)).at(-1)!.summary).toContain(
        "history could not be read",
      );
    }),
);

// --- who is recovered --------------------------------------------------------

scenario("threads that never opted in are never touched, and the opt-in is explicit", (server) =>
  Effect.gen(function* () {
    const historical = yield* server.createThread("t-historical", { automaticRecovery: false });
    yield* server.startTurn(historical, "work");
    yield* server.failTurn(historical, "read ECONNRESET");
    yield* server.advance(24 * HOUR);
    expect(Option.isNone(yield* server.intent(historical))).toBe(true);
    expect(yield* server.recoveryRows(historical)).toEqual([]);
    expect(sendsOn(server, A)).toEqual(["work"]);
    expect(
      (yield* server.shell(historical)).pipe(
        Option.flatMap((s) => Option.fromNullishOr(s.automaticRecovery)),
      ),
    ).toEqual(Option.none());

    // One explicit action on one thread opts it in.
    yield* server.dispatch({
      type: "thread.recovery.set",
      commandId: CommandId.make("cmd-opt-in"),
      threadId: historical,
      enabled: true,
    });
    yield* server.settle;
    yield* server.startTurn(historical, "more work");
    yield* server.failTurn(historical, "read ECONNRESET");
    expect(Option.isSome(yield* server.intent(historical))).toBe(true);
  }),
);

scenario(
  "a thread created without any recovery field, as an older mobile app does, recovers",
  (server) =>
    Effect.gen(function* () {
      const id = yield* server.createThread("t-old-mobile");
      expect(
        (yield* server.shell(id)).pipe(Option.map((shell) => shell.automaticRecovery)),
      ).toEqual(Option.some(true));
      yield* server.startTurn(id, "work");
      yield* server.failTurn(id, "fetch failed");
      yield* server.advance(5_000);
      expect(sendsOn(server, A)).toEqual(["work", "Continue where you left off."]);
    }),
);

void B;
void C;
void D;

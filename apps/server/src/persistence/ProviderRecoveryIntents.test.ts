import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { SqlitePersistenceMemory } from "./Layers/Sqlite.ts";
import * as Intents from "./ProviderRecoveryIntents.ts";

const threadId = ThreadId.make("thread-1");
const intent = (token: string): Intents.ProviderRecoveryIntent => ({
  threadId,
  token,
  kind: "quota",
  status: "waiting",
  attempt: 2,
  wakeAt: "2026-10-09T12:00:00.000Z",
  turnId: "turn-1",
  instanceId: ProviderInstanceId.make("claude-a"),
  sessionIdentity: { resume: "session-1" },
  targetInstanceId: null,
  stagedCursor: null,
  excluded: [ProviderInstanceId.make("claude-b")],
  reason: "5-hour window exhausted",
  createdAt: "2026-10-09T10:00:00.000Z",
  updatedAt: "2026-10-09T10:00:00.000Z",
});

const layer = Intents.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory));

it.layer(layer)("ProviderRecoveryIntents", (it) => {
  it.effect("round-trips every field", () =>
    Effect.gen(function* () {
      const store = yield* Intents.ProviderRecoveryIntents;
      yield* store.put(intent("token-1"));
      const loaded = yield* store.get(threadId);
      assert.deepStrictEqual(Option.getOrNull(loaded), intent("token-1"));
      assert.strictEqual((yield* store.list()).length, 1);
      yield* store.remove(threadId);
    }),
  );

  it.effect("only the current token can claim, and only once", () =>
    Effect.gen(function* () {
      const store = yield* Intents.ProviderRecoveryIntents;
      yield* store.put(intent("token-1"));
      yield* store.put(intent("token-2"));
      assert.isFalse(yield* store.claim(threadId, "token-1"));
      assert.isTrue(yield* store.claim(threadId, "token-2"));
      assert.isFalse(yield* store.claim(threadId, "token-2"));
      const loaded = Option.getOrThrow(yield* store.get(threadId));
      assert.strictEqual(loaded.status, "resuming");
      yield* store.remove(threadId);
    }),
  );

  it.effect("cancellation removes the intent and stale tokens cannot remove a newer arming", () =>
    Effect.gen(function* () {
      const store = yield* Intents.ProviderRecoveryIntents;
      yield* store.put(intent("token-1"));
      assert.isFalse(yield* store.remove(threadId, "stale"));
      assert.isTrue(Option.isSome(yield* store.get(threadId)));
      assert.isTrue(yield* store.remove(threadId));
      assert.isTrue(Option.isNone(yield* store.get(threadId)));
      assert.isFalse(yield* store.claim(threadId, "token-1"));
    }),
  );

  it.effect("staging a native copy survives for the same arming only", () =>
    Effect.gen(function* () {
      const store = yield* Intents.ProviderRecoveryIntents;
      yield* store.put(intent("token-1"));
      assert.isFalse(yield* store.stage(threadId, "stale", { resume: "copy" }));
      assert.isTrue(yield* store.stage(threadId, "token-1", { resume: "copy" }));
      const loaded = Option.getOrThrow(yield* store.get(threadId));
      assert.deepStrictEqual(loaded.stagedCursor, { resume: "copy" });
      yield* store.remove(threadId);
    }),
  );
});

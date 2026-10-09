import { IsoDateTime, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "./Errors.ts";

/**
 * One pending automatic recovery per thread. The row is the whole intent:
 * deleting it is cancellation, so a timer that fires late finds nothing to do.
 * `token` fences every re-arm; only the fiber holding the current token may claim it.
 */
export const ProviderRecoveryIntent = Schema.Struct({
  threadId: ThreadId,
  token: Schema.String,
  kind: Schema.Literals(["transient", "quota"]),
  /** `resuming` is set by the claim just before a turn is sent. */
  status: Schema.Literals(["waiting", "resuming"]),
  attempt: Schema.Number,
  /** Null while an exhausted quota window reports no reset time. */
  wakeAt: Schema.NullOr(IsoDateTime),
  turnId: Schema.NullOr(Schema.String),
  /** The account the turn was running on when it stopped. */
  instanceId: ProviderInstanceId,
  /** Resume cursor at failure; a different cursor later means the conversation moved on. */
  sessionIdentity: Schema.fromJsonString(Schema.Unknown),
  /** Account chosen to take over, while a transfer is in flight. */
  targetInstanceId: Schema.NullOr(ProviderInstanceId),
  /** Native copy accepted for the target but not yet bound to the thread. */
  stagedCursor: Schema.NullOr(Schema.fromJsonString(Schema.Unknown)),
  /** Accounts whose transfer already failed for this intent. */
  excluded: Schema.fromJsonString(Schema.Array(ProviderInstanceId)),
  reason: Schema.String,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type ProviderRecoveryIntent = typeof ProviderRecoveryIntent.Type;

const COLUMNS = `thread_id AS "threadId", token, kind, status, attempt, wake_at AS "wakeAt",
  turn_id AS "turnId", instance_id AS "instanceId", session_identity AS "sessionIdentity",
  target_instance_id AS "targetInstanceId", staged_cursor_json AS "stagedCursor", excluded_json AS excluded,
  reason, created_at AS "createdAt", updated_at AS "updatedAt"`;

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const select = (where: string) => (threadId?: string) =>
    sql.unsafe(
      `SELECT ${COLUMNS} FROM provider_recovery_intents ${where} ORDER BY created_at`,
      threadId === undefined ? [] : [threadId],
    );
  const getRow = SqlSchema.findOneOption({
    Request: Schema.Struct({ threadId: ThreadId }),
    Result: ProviderRecoveryIntent,
    execute: ({ threadId }) => select("WHERE thread_id = ?")(threadId),
  });
  const listRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProviderRecoveryIntent,
    execute: () => select("")(),
  });
  const putRow = SqlSchema.void({
    Request: ProviderRecoveryIntent,
    execute: (row) => sql`
      INSERT INTO provider_recovery_intents
        (thread_id, token, kind, status, attempt, wake_at, turn_id, instance_id, session_identity,
          target_instance_id, staged_cursor_json, excluded_json, reason, created_at, updated_at)
      VALUES (${row.threadId}, ${row.token}, ${row.kind}, ${row.status}, ${row.attempt}, ${row.wakeAt},
        ${row.turnId}, ${row.instanceId}, ${row.sessionIdentity}, ${row.targetInstanceId},
        ${row.stagedCursor}, ${row.excluded}, ${row.reason}, ${row.createdAt}, ${row.updatedAt})
      ON CONFLICT (thread_id) DO UPDATE SET
        token = excluded.token, kind = excluded.kind, status = excluded.status,
        attempt = excluded.attempt, wake_at = excluded.wake_at, turn_id = excluded.turn_id,
        instance_id = excluded.instance_id, session_identity = excluded.session_identity,
        target_instance_id = excluded.target_instance_id, staged_cursor_json = excluded.staged_cursor_json,
        excluded_json = excluded.excluded_json, reason = excluded.reason, updated_at = excluded.updated_at
    `,
  });
  const fail = (operation: string) => toPersistenceSqlError(`ProviderRecoveryIntents.${operation}`);

  return {
    get: (threadId: ThreadId) => getRow({ threadId }).pipe(Effect.mapError(fail("get"))),
    list: () => listRows(undefined).pipe(Effect.mapError(fail("list"))),
    put: (intent: ProviderRecoveryIntent) => putRow(intent).pipe(Effect.mapError(fail("put"))),
    /** Compare-and-set `waiting` to `resuming`: false when the intent was cancelled or re-armed. */
    claim: (threadId: ThreadId, token: string) =>
      sql<{ readonly threadId: string }>`
        UPDATE provider_recovery_intents SET status = 'resuming'
        WHERE thread_id = ${threadId} AND token = ${token} AND status = 'waiting'
        RETURNING thread_id AS "threadId"
      `.pipe(
        Effect.map((rows) => rows.length > 0),
        Effect.mapError(fail("claim")),
      ),
    /** Records an accepted native copy so a restart binds it instead of copying again. */
    stage: (threadId: ThreadId, token: string, cursor: unknown) =>
      sql<{ readonly threadId: string }>`
        UPDATE provider_recovery_intents SET staged_cursor_json = ${JSON.stringify(cursor)}
        WHERE thread_id = ${threadId} AND token = ${token}
        RETURNING thread_id AS "threadId"
      `.pipe(
        Effect.map((rows) => rows.length > 0),
        Effect.mapError(fail("stage")),
      ),
    /** Deletes the intent. With a token, only that arming; false when already gone. */
    remove: (threadId: ThreadId, token?: string) =>
      sql<{ readonly threadId: string }>`
        DELETE FROM provider_recovery_intents
        WHERE thread_id = ${threadId} AND (${token ?? null} IS NULL OR token = ${token ?? null})
        RETURNING thread_id AS "threadId"
      `.pipe(
        Effect.map((rows) => rows.length > 0),
        Effect.mapError(fail("remove")),
      ),
  };
});

export type ProviderRecoveryIntentStore = Effect.Success<typeof make>;

export class ProviderRecoveryIntents extends Context.Service<
  ProviderRecoveryIntents,
  ProviderRecoveryIntentStore
>()("t3/persistence/ProviderRecoveryIntents") {}

export const layer = Layer.effect(ProviderRecoveryIntents, make);

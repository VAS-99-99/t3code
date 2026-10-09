import {
  CommandId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "./Errors.ts";

const ForkBinding = Schema.Struct({
  provider: ProviderDriverKind,
  providerInstanceId: ProviderInstanceId,
  runtimeMode: RuntimeMode,
  resumeCursor: Schema.Unknown,
  runtimePayload: Schema.Unknown,
});
const ForkRequest = Schema.Struct({
  destinationThreadId: ThreadId,
  sourceThreadId: ThreadId,
  commandId: CommandId,
  binding: ForkBinding,
  captured: Schema.Unknown,
});
const ForkRow = Schema.Struct({
  ...ForkRequest.fields,
  binding: Schema.fromJsonString(ForkBinding),
  captured: Schema.fromJsonString(Schema.Unknown),
  publishedSequence: Schema.NullOr(Schema.Number),
  creationCommandId: Schema.NullOr(Schema.String),
});

/** Uses the caller's SQLite transaction when publishing. No native calls run here. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const getRow = SqlSchema.findOneOption({
    Request: Schema.Struct({ threadId: ThreadId }),
    Result: ForkRow,
    execute: ({ threadId }) => sql`
      SELECT destination_thread_id AS "destinationThreadId", source_thread_id AS "sourceThreadId",
        command_id AS "commandId", binding_json AS binding, captured_json AS captured,
        published_sequence AS "publishedSequence",
        (SELECT command_id FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${threadId} AND event_type = 'thread.created'
          ORDER BY sequence DESC LIMIT 1) AS "creationCommandId"
      FROM provider_conversation_forks WHERE destination_thread_id = ${threadId}
    `,
  });
  const stageRow = SqlSchema.void({
    Request: ForkRequest.mapFields((fields) => ({
      ...fields,
      binding: Schema.fromJsonString(ForkBinding),
      captured: Schema.fromJsonString(Schema.Unknown),
    })),
    execute: (fork) => sql`
      INSERT INTO provider_conversation_forks
        (destination_thread_id, source_thread_id, command_id, binding_json, captured_json)
      VALUES (${fork.destinationThreadId}, ${fork.sourceThreadId}, ${fork.commandId}, ${fork.binding}, ${fork.captured})
      ON CONFLICT (destination_thread_id) DO NOTHING
    `,
  });
  const get = (threadId: ThreadId) =>
    getRow({ threadId }).pipe(
      Effect.mapError(toPersistenceSqlError("ProviderConversationForks.get")),
    );
  const stage = (fork: typeof ForkRequest.Type) =>
    stageRow(fork).pipe(Effect.mapError(toPersistenceSqlError("ProviderConversationForks.stage")));
  const publish = (threadId: ThreadId, sequence: number, createdAt: string) =>
    sql`
      INSERT INTO provider_session_runtime
        (thread_id, provider_name, provider_instance_id, adapter_key, runtime_mode, status,
          last_seen_at, resume_cursor_json, runtime_payload_json)
      SELECT destination_thread_id, json_extract(binding_json, '$.provider'),
        json_extract(binding_json, '$.providerInstanceId'), json_extract(binding_json, '$.provider'),
        json_extract(binding_json, '$.runtimeMode'), 'stopped', ${createdAt},
        binding_json -> '$.resumeCursor', binding_json -> '$.runtimePayload'
      FROM provider_conversation_forks WHERE destination_thread_id = ${threadId} AND published_sequence IS NULL
      ON CONFLICT (thread_id) DO NOTHING
      RETURNING thread_id
    `.pipe(
      Effect.flatMap((rows) =>
        rows.length === 0
          ? Effect.succeed(false)
          : sql`
        UPDATE provider_conversation_forks SET published_sequence = ${sequence}
        WHERE destination_thread_id = ${threadId}
      `.pipe(Effect.as(true)),
      ),
      Effect.mapError(toPersistenceSqlError("ProviderConversationForks.publish")),
    );
  return { get, stage, publish };
});

export type ProviderConversationForkStore = Effect.Success<typeof make>;

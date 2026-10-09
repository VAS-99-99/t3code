import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Accepted native copies are recovery records, never routable sessions.
  yield* sql`
    CREATE TABLE provider_conversation_forks (
      destination_thread_id TEXT PRIMARY KEY,
      source_thread_id TEXT NOT NULL,
      command_id TEXT NOT NULL,
      binding_json TEXT NOT NULL,
      captured_json TEXT NOT NULL,
      published_sequence INTEGER
    )
  `;
});

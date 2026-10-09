import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;
  // Existing rows get 0: only threads created after this migration opt in.
  if (!columns.some((column) => column.name === "automatic_recovery")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN automatic_recovery INTEGER NOT NULL DEFAULT 0
    `;
  }
  yield* sql`
    CREATE TABLE IF NOT EXISTS provider_recovery_intents (
      thread_id TEXT PRIMARY KEY,
      token TEXT NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL,
      attempt INTEGER NOT NULL,
      wake_at TEXT,
      turn_id TEXT,
      instance_id TEXT NOT NULL,
      session_identity TEXT NOT NULL,
      target_instance_id TEXT,
      staged_cursor_json TEXT,
      excluded_json TEXT NOT NULL,
      reason TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
});

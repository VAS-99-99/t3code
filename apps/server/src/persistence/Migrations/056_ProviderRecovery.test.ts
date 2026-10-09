import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("056_ProviderRecovery", (it) => {
  it.effect("leaves every existing thread out of automatic recovery", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 55 });
      const now = "2026-01-01T00:00:00.000Z";
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          created_at, updated_at
        ) VALUES (
          'historical', 'project-1', 'Copied history',
          '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', ${now}, ${now}
        )
      `;
      yield* runMigrations({ toMigrationInclusive: 56 });
      const migrated = yield* sql<{ readonly automaticRecovery: number }>`
        SELECT automatic_recovery AS "automaticRecovery" FROM projection_threads
        WHERE thread_id = 'historical'
      `;
      assert.deepEqual(migrated, [{ automaticRecovery: 0 }]);
      const intents = yield* sql`SELECT thread_id FROM provider_recovery_intents`;
      assert.strictEqual(intents.length, 0);
    }),
  );
});

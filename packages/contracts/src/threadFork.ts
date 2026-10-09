import * as Schema from "effect/Schema";
import { CommandId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * Fork a thread into a new, independent thread in the same project.
 *
 * Clients allocate `destinationThreadId` and `commandId`; repeating a request
 * with the same pair returns the same fork instead of creating another one.
 */
export const ThreadForkInput = Schema.Struct({
  sourceThreadId: ThreadId,
  destinationThreadId: ThreadId,
  commandId: CommandId,
  /** Defaults to the source title. */
  title: Schema.optional(TrimmedNonEmptyString),
});
export type ThreadForkInput = typeof ThreadForkInput.Type;

export const ThreadForkResult = Schema.Struct({ destinationThreadId: ThreadId });
export type ThreadForkResult = typeof ThreadForkResult.Type;

export const ThreadForkFailureReason = Schema.Literals([
  "source-not-found",
  /** The source is running, waiting on an approval or input, or its last turn did not complete. */
  "source-not-forkable",
  /** No completed turn exists yet, so there is no native session to fork. */
  "empty-history",
  "unsupported-provider",
  /** The destination id is already used by something that is not this fork. */
  "destination-conflict",
  "provider-failed",
  "history-failed",
]);
export type ThreadForkFailureReason = typeof ThreadForkFailureReason.Type;

export class ThreadForkError extends Schema.TaggedError<ThreadForkError>()("ThreadForkError", {
  reason: ThreadForkFailureReason,
  sourceThreadId: ThreadId,
  destinationThreadId: ThreadId,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Cannot fork thread '${this.sourceThreadId}': ${this.detail}`;
  }
}

import type { MessageId, TurnId } from "@t3tools/contracts";
import { deriveTaskResults, type TaskResult } from "@t3tools/client-runtime/task-results";

import type { TimelineEntry } from "../../session-logic";
import type { TurnDiffSummary } from "../../types";

export {
  taskResultsEqual,
  type RecordedCommand,
  type RecordedCommandStatus,
  type TaskResult,
} from "@t3tools/client-runtime/task-results";

/** Feeds the shared derivation from the web timeline; the rules live in client-runtime. */
export function deriveTaskResultsByAssistantMessageId(input: {
  timelineEntries: ReadonlyArray<TimelineEntry>;
  terminalAssistantMessageIds: ReadonlySet<string>;
  activeTurnIds: ReadonlySet<TurnId>;
  latestTurn: { turnId: TurnId; state: string } | null;
  turnDiffSummaryByAssistantMessageId: ReadonlyMap<MessageId, TurnDiffSummary>;
}): Map<string, TaskResult> {
  const workEntries = [];
  const messages = [];
  for (const timelineEntry of input.timelineEntries) {
    if (timelineEntry.kind === "work") workEntries.push(timelineEntry.entry);
    else if (timelineEntry.kind === "message") messages.push(timelineEntry.message);
  }
  const changedFileCountByMessageId = new Map<string, number>();
  for (const [messageId, summary] of input.turnDiffSummaryByAssistantMessageId) {
    changedFileCountByMessageId.set(messageId, summary.files.length);
  }
  return deriveTaskResults({
    workEntries,
    messages,
    terminalAssistantMessageIds: input.terminalAssistantMessageIds,
    activeTurnIds: input.activeTurnIds,
    latestTurn: input.latestTurn,
    changedFileCountByMessageId,
  });
}

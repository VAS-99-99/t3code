import { EventId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { describeRecordedCommand } from "@t3tools/client-runtime/task-results";

import { deriveWorkLogEntries } from "./session-logic";
import { deriveTaskResultsByAssistantMessageId } from "./components/chat/TaskResult.logic";
import type { TimelineEntry } from "./session-logic";

const TURN = TurnId.make("turn-1");
let seq = 0;

function toolActivity(
  kind: "tool.started" | "tool.updated" | "tool.completed",
  payload: Record<string, unknown>,
): OrchestrationThreadActivity {
  seq += 1;
  return {
    id: EventId.make(`activity-${seq}`),
    createdAt: new Date(Date.UTC(2026, 9, 10, 0, 0, seq)).toISOString(),
    kind,
    summary: "Ran command",
    tone: "tool",
    turnId: TURN,
    sequence: seq,
    payload: { itemType: "command_execution", title: "Ran command", ...payload },
  } as OrchestrationThreadActivity;
}

/** Real ingestion first, then the shared result derivation the timeline uses. */
function commandResults(activities: OrchestrationThreadActivity[]) {
  const timelineEntries: TimelineEntry[] = deriveWorkLogEntries(activities).map((entry) => ({
    id: entry.id,
    kind: "work",
    createdAt: entry.createdAt,
    entry,
  }));
  timelineEntries.push({
    id: "final",
    kind: "message",
    createdAt: new Date(Date.UTC(2026, 9, 10, 1)).toISOString(),
    message: {
      id: "final",
      role: "assistant",
      text: "All tests passed, verified.",
      turnId: TURN,
      createdAt: new Date(Date.UTC(2026, 9, 10, 1)).toISOString(),
      streaming: false,
    },
  } as TimelineEntry);
  return deriveTaskResultsByAssistantMessageId({
    timelineEntries,
    terminalAssistantMessageIds: new Set(["final"]),
    activeTurnIds: new Set(),
    latestTurn: null,
    turnDiffSummaryByAssistantMessageId: new Map(),
  }).get("final")?.commands;
}

describe("command exit code ingestion", () => {
  it("shows a completed tool with a nonzero structured exit as failed", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { command: ["vp", "test", "run"], exitCode: 2 } },
      }),
    ]);
    expect(commands).toMatchObject([{ command: "vp test run", status: "failed", exitCode: 2 }]);
  });

  it("reads the ACP rawOutput and exit_code spellings", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { command: "make", rawOutput: { exit_code: 7, stdout: "x" } },
      }),
    ]);
    expect(commands).toMatchObject([{ status: "failed", exitCode: 7 }]);
  });

  it("keeps the exit from a trailing marker and strips it from the output", () => {
    const [entry] = deriveWorkLogEntries([
      toolActivity("tool.completed", {
        status: "completed",
        detail: "boom\n<exited with exit code 1>",
        data: { item: { command: ["node", "x.js"] } },
      }),
    ]);
    expect(entry?.exitCode).toBe(1);
    expect(entry?.detail).toBe("boom");
  });

  it("says Exit 0 for a clean exit, never that a test passed", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { command: ["vp", "test", "run"], exitCode: 0 } },
      }),
    ])!;
    expect(commands[0]).toMatchObject({ status: "completed", exitCode: 0 });
    expect(describeRecordedCommand(commands[0]!)).toBe("Exit 0");
  });

  it("leaves a completed call without an exit unverified", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { input: { command: "vp test run" }, result: { content: "ok" } } },
      }),
    ])!;
    expect(commands[0]?.status).toBe("completed");
    expect(commands[0]?.exitCode).toBeUndefined();
    expect(describeRecordedCommand(commands[0]!)).toBe("Finished, exit not recorded");
  });

  it("does not read token counts or other numbers as an exit", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: {
          item: { command: ["ls"], result: { content: "a", tokenCount: 0, status: 0 } },
          usage: { outputTokens: 0, exitCode: "0" },
        },
      }),
    ])!;
    expect(commands[0]?.exitCode).toBeUndefined();
  });

  it("keeps the exit across lifecycle updates that omit it", () => {
    const commands = commandResults([
      toolActivity("tool.started", {
        toolCallId: "call-1",
        status: "inProgress",
        data: { toolCallId: "call-1", item: { command: ["make"] } },
      }),
      toolActivity("tool.completed", {
        toolCallId: "call-1",
        status: "completed",
        data: { toolCallId: "call-1", item: { command: ["make"], exitCode: 3 } },
      }),
      toolActivity("tool.updated", {
        toolCallId: "call-1",
        data: { toolCallId: "call-1", item: { command: ["make"] } },
      }),
    ])!;
    expect(commands).toHaveLength(1);
    expect(commands[0]).toMatchObject({ status: "failed", exitCode: 3 });
  });

  it("keeps declined and stopped calls as they were reported", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        toolCallId: "d",
        status: "declined",
        data: { toolCallId: "d", item: { command: ["rm", "x"], exitCode: 1 } },
      }),
      toolActivity("tool.completed", {
        toolCallId: "s",
        status: "stopped",
        data: { toolCallId: "s", item: { command: ["sleep", "9"] } },
      }),
    ])!;
    expect(commands.map((c) => c.status)).toEqual(["declined", "stopped"]);
  });

  it("records one shell exit for a compound command", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { command: "vp lint && vp test run", exitCode: 0 } },
      }),
    ])!;
    expect(commands).toHaveLength(1);
    expect(describeRecordedCommand(commands[0]!)).toBe("Exit 0");
  });
});

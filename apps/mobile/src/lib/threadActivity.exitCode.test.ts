import { EventId, TurnId, type OrchestrationThreadActivity } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { deriveTaskResults, type TaskResultWorkEntry } from "@t3tools/client-runtime/task-results";

import { buildThreadFeed } from "./threadActivity";

const TURN = TurnId.make("turn-1");
let seq = 0;

function toolActivity(
  kind: "tool.started" | "tool.completed",
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

/** Real mobile ingestion, then the same shared derivation the feed uses. */
function commandResults(activities: OrchestrationThreadActivity[]) {
  const feed = buildThreadFeed({ messages: [], activities });
  const workEntries: TaskResultWorkEntry[] = feed.flatMap((entry) =>
    entry.type === "activity-group" ? entry.activities.map((a) => a.workEntry) : [],
  );
  return deriveTaskResults({
    workEntries,
    messages: [{ id: "final", role: "assistant", turnId: TURN }],
    terminalAssistantMessageIds: new Set(["final"]),
    activeTurnIds: new Set(),
    latestTurn: null,
  }).get("final")?.commands;
}

describe("mobile command exit code ingestion", () => {
  it("shows a completed tool with a nonzero structured exit as failed", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { command: ["vp", "test", "run"], exitCode: 2 } },
      }),
    ]);
    expect(commands).toMatchObject([{ status: "failed", exitCode: 2 }]);
  });

  it("keeps the exit from a trailing marker", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        detail: "boom\n<exited with exit code 1>",
        data: { item: { command: ["node", "x.js"] } },
      }),
    ]);
    expect(commands).toMatchObject([{ status: "failed", exitCode: 1 }]);
  });

  it("leaves a completed call without an exit unverified", () => {
    const commands = commandResults([
      toolActivity("tool.completed", {
        status: "completed",
        data: { item: { input: { command: "vp test run" }, result: { content: "ok" } } },
      }),
    ]);
    expect(commands?.[0]?.status).toBe("completed");
    expect(commands?.[0]?.exitCode).toBeUndefined();
  });

  it("keeps the exit across a lifecycle merge", () => {
    const commands = commandResults([
      toolActivity("tool.started", {
        toolCallId: "call-1",
        status: "inProgress",
        data: { toolCallId: "call-1", item: { command: ["make"] } },
      }),
      toolActivity("tool.completed", {
        toolCallId: "call-1",
        status: "completed",
        data: { toolCallId: "call-1", item: { command: ["make"], exitCode: 0 } },
      }),
    ]);
    expect(commands).toMatchObject([{ status: "completed", exitCode: 0 }]);
  });
});

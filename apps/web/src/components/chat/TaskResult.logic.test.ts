import { MessageId, TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { TimelineEntry, WorkLogEntry } from "../../session-logic";
import type { ChatMessage, TurnDiffSummary } from "../../types";
import { deriveMessagesTimelineRows } from "./MessagesTimeline.logic";
import { deriveTaskResultsByAssistantMessageId } from "./TaskResult.logic";

const T1 = TurnId.make("turn-1");
const T2 = TurnId.make("turn-2");
const time = (second: number) => new Date(Date.UTC(2026, 8, 4, 0, 0, second)).toISOString();

function assistant(id: string, turnId: TurnId, second: number, streaming = false): TimelineEntry {
  const message: ChatMessage = {
    id: MessageId.make(id),
    role: "assistant",
    text: "All tests passed, verified.",
    turnId,
    createdAt: time(second),
    updatedAt: time(second),
    streaming,
  };
  return { id: id, kind: "message", createdAt: message.createdAt, message };
}

function command(
  id: string,
  turnId: TurnId,
  second: number,
  overrides: { [K in keyof WorkLogEntry]?: WorkLogEntry[K] | undefined } = {},
): TimelineEntry {
  return {
    id,
    kind: "work",
    createdAt: time(second),
    entry: {
      id,
      createdAt: time(second),
      turnId,
      label: "Ran command",
      tone: "tool",
      itemType: "command_execution",
      command: "vp test run",
      sourceActivityKind: "tool.completed",
      toolLifecycleStatus: "completed",
      ...overrides,
    } as WorkLogEntry,
  };
}

function derive(
  timelineEntries: TimelineEntry[],
  extra: Partial<Parameters<typeof deriveTaskResultsByAssistantMessageId>[0]> = {},
) {
  const terminal = new Set<string>();
  const lastByTurn = new Map<string, string>();
  for (const entry of timelineEntries) {
    if (entry.kind === "message" && entry.message.turnId) {
      lastByTurn.set(entry.message.turnId, entry.message.id);
    }
  }
  for (const id of lastByTurn.values()) terminal.add(id);
  return deriveTaskResultsByAssistantMessageId({
    timelineEntries,
    terminalAssistantMessageIds: terminal,
    activeTurnIds: new Set(),
    latestTurn: null,
    turnDiffSummaryByAssistantMessageId: new Map(),
    ...extra,
  });
}

describe("deriveTaskResultsByAssistantMessageId", () => {
  it("reports provider status per command and keeps failed, unknown and running visible", () => {
    const results = derive([
      command("ok", T1, 1),
      command("bad", T1, 2, { toolLifecycleStatus: "failed", command: "vp run build" }),
      command("unk", T1, 3, { toolLifecycleStatus: undefined, command: "node x.js" }),
      command("run", T1, 4, { toolLifecycleStatus: "inProgress", command: "sleep 9" }),
      assistant("a1", T1, 5),
    ]);
    expect(results.get("a1")?.commands.map((c) => c.status)).toEqual([
      "completed",
      "failed",
      "unknown",
      "running",
    ]);
  });

  it("ignores assistant prose that claims success", () => {
    const results = derive([
      command("bad", T1, 1, { toolLifecycleStatus: "failed" }),
      assistant("a1", T1, 2),
    ]);
    expect(results.get("a1")?.commands).toEqual([
      { id: "bad", command: "vp test run", status: "failed" },
    ]);
  });

  it("flags a completed call whose output reports a failure", () => {
    const results = derive([
      command("c", T1, 1, { detail: "bash: nope: command not found" }),
      assistant("a1", T1, 2),
    ]);
    expect(results.get("a1")?.commands[0]?.status).toBe("failed");
  });

  it("never lends one turn's commands to another turn", () => {
    const results = derive([
      command("old", T1, 1, { toolLifecycleStatus: "failed" }),
      assistant("a1", T1, 2),
      command("new", T2, 3, { command: "vp lint" }),
      assistant("a2", T2, 4),
    ]);
    expect(results.get("a1")?.commands.map((c) => c.id)).toEqual(["old"]);
    expect(results.get("a2")?.commands.map((c) => c.id)).toEqual(["new"]);
  });

  it("collapses lifecycle updates by toolCallId to the latest status", () => {
    const results = derive([
      command("u1", T1, 1, { toolCallId: "call-1", toolLifecycleStatus: "inProgress" }),
      command("u2", T1, 2, { toolCallId: "call-1", toolLifecycleStatus: "failed" }),
      command("u3", T1, 3, {
        toolCallId: "call-1",
        toolLifecycleStatus: undefined,
        command: undefined,
      }),
      assistant("a1", T1, 4),
    ]);
    expect(results.get("a1")?.commands).toMatchObject([
      { status: "failed", command: "vp test run" },
    ]);
  });

  it("attaches only to the terminal message, not earlier commentary", () => {
    const results = derive([
      assistant("commentary", T1, 1),
      command("c", T1, 2),
      assistant("final", T1, 3),
    ]);
    expect([...results.keys()]).toEqual(["final"]);
  });

  it("emits nothing while the turn is active or the message streams", () => {
    const entries = [command("c", T1, 1), assistant("a1", T1, 2)];
    expect(derive(entries, { activeTurnIds: new Set([T1]) }).size).toBe(0);
    expect(derive([command("c", T1, 1), assistant("a1", T1, 2, true)]).size).toBe(0);
  });

  it("marks interrupted and errored latest turns instead of implying success", () => {
    const entries = [command("c", T1, 1), assistant("a1", T1, 2)];
    expect(
      derive(entries, { latestTurn: { turnId: T1, state: "interrupted" } }).get("a1")?.outcome,
    ).toBe("interrupted");
    expect(derive(entries, { latestTurn: { turnId: T1, state: "error" } }).get("a1")?.outcome).toBe(
      "error",
    );
  });

  it("emits nothing without commands, PR links or changed files", () => {
    expect(derive([assistant("a1", T1, 1)]).size).toBe(0);
    expect(
      derive([command("read", T1, 1, { command: undefined }), assistant("a1", T1, 2)]).size,
    ).toBe(0);
  });

  it("keeps a diff-only turn as 'no recorded commands'", () => {
    const summary = {
      turnId: T1,
      files: [{ path: "a.ts", additions: 1, deletions: 0 }],
    } as unknown as TurnDiffSummary;
    const results = derive([assistant("a1", T1, 1)], {
      turnDiffSummaryByAssistantMessageId: new Map([[MessageId.make("a1"), summary]]),
    });
    expect(results.get("a1")).toMatchObject({ commands: [], changedFileCount: 1 });
  });

  it("links a PR only from a completed link call with a recognised URL", () => {
    const link = (id: string, url: string, status: WorkLogEntry["toolLifecycleStatus"]) =>
      command(id, T1, 1, {
        itemType: "mcp_tool_call",
        command: undefined,
        toolTitle: "t3-code.link_pull_request",
        toolLifecycleStatus: status,
        toolData: { server: "t3-code", tool: "link_pull_request", arguments: { url } },
      });
    const results = derive([
      link("ok", "https://github.com/acme/app/pull/12", "completed"),
      link("failed", "https://github.com/acme/app/pull/13", "failed"),
      link("junk", "javascript:alert(1)", "completed"),
      link("issue", "https://github.com/acme/app/issues/14", "completed"),
      assistant("a1", T1, 2),
    ]);
    expect(results.get("a1")?.pullRequests).toEqual([
      { url: "https://github.com/acme/app/pull/12", number: 12 },
    ]);
  });
});

describe("deriveMessagesTimelineRows task result", () => {
  it.each(["interrupted", "error"] as const)(
    "puts a %s result on its user row before assistant text",
    (state) => {
      const user = assistant("user", T1, 1);
      if (user.kind === "message") user.message = { ...user.message, role: "user" };
      const rows = deriveMessagesTimelineRows({
        timelineEntries: [user, command("c", T1, 2)],
        latestTurn: { turnId: T1, state, startedAt: time(1), completedAt: time(3) },
        isWorking: false,
        activeTurnStartedAt: null,
        turnDiffSummaries: [],
        supportsConversationRollback: false,
      });
      expect(
        rows
          .filter((row) => row.kind === "message" && row.assistantTaskResult)
          .map((row) => row.id),
      ).toEqual(["user"]);
    },
  );
  it("puts the result on the terminal assistant row only", () => {
    const rows = deriveMessagesTimelineRows({
      timelineEntries: [
        assistant("commentary", T1, 1),
        command("c", T1, 2),
        assistant("final", T1, 3),
      ],
      latestTurn: { turnId: T1, state: "completed", startedAt: time(0), completedAt: time(4) },
      isWorking: false,
      activeTurnStartedAt: null,
      turnDiffSummaries: [],
      supportsConversationRollback: false,
    });
    const withResult = rows.flatMap((row) =>
      row.kind === "message" && row.assistantTaskResult ? [row.id] : [],
    );
    expect(withResult).toEqual(["final"]);
  });
});

import { describe, expect, it } from "vite-plus/test";
import {
  EventId,
  TurnId,
  type OrchestrationThreadActivity,
  type OrchestrationThreadDetailSnapshot,
} from "@t3tools/contracts";
import {
  deriveTaskResults,
  type TaskResultWorkEntry,
} from "../../../../packages/client-runtime/src/work-log/taskResults.ts";
import { deriveWorkLogEntries } from "../../../web/src/session-logic.ts";
import { buildThreadFeed } from "../../../mobile/src/lib/threadActivity.ts";
import {
  projectActivityPayload,
  projectThreadDetailSnapshot,
} from "./ActivityPayloadProjection.ts";

const turnId = TurnId.make("t1");
function activity(data: object, extra: object = {}): OrchestrationThreadActivity {
  return {
    id: EventId.make("a1"),
    kind: "tool.completed",
    tone: "tool",
    summary: "Ran command",
    turnId,
    createdAt: "2026-10-10T10:00:00.000Z",
    payload: { itemType: "command_execution", status: "completed", data, ...extra },
  };
}
const clients = {
  web: deriveWorkLogEntries,
  mobile: (activities: ReadonlyArray<OrchestrationThreadActivity>) =>
    buildThreadFeed({ messages: [], activities }).flatMap((e) =>
      e.type === "activity-group" ? e.activities.map((a) => a.workEntry) : [],
    ),
};
function result(entries: TaskResultWorkEntry[]) {
  return deriveTaskResults({
    workEntries: entries,
    messages: [{ id: "final", role: "assistant", turnId }],
    terminalAssistantMessageIds: new Set(["final"]),
    activeTurnIds: new Set(),
    latestTurn: null,
  }).get("final");
}

for (const [client, ingest] of Object.entries(clients))
  describe(`projection to ${client} results`, () => {
    it.each([
      [
        {
          item: {
            command: "vp test run",
            exitCode: 2,
            aggregatedOutput: "FAIL regression\n" + "x".repeat(5000),
          },
        },
        2,
      ],
      [{ item: { command: "make", result: { exit_code: 3, content: "output" } } }, 3],
      [{ command: "make", rawOutput: { exit_code: -1, stdout: "output" } }, -1],
      [
        {
          toolName: "Bash",
          input: { command: "make" },
          result: { content: "output\n<exited with exit code 7>" },
        },
        7,
      ],
      [{ item: { command: "make", exitCode: 0 } }, 0],
      [{ item: { command: "make", exitCode: 1.5, result: { tokens: 2, status: 2 } } }, undefined],
      [{ item: { command: "make", exit_code: Number.MAX_SAFE_INTEGER + 1 } }, undefined],
      [
        {
          command: "make",
          rawOutput: { content: "output\n<exited with exit code 9007199254740992>" },
        },
        undefined,
      ],
      [{ item: { command: "make" }, exitCode: 2, status: 2, tokens: 2 }, undefined],
    ])("retains only subprocess exits for %j", (data, exitCode) => {
      const projected = projectActivityPayload(activity(data));
      const command = result(ingest([projected]))?.commands[0];
      expect(command?.exitCode).toBe(exitCode);
      expect(command?.status).toBe(
        exitCode !== undefined && exitCode !== 0 ? "failed" : "completed",
      );
      expect(JSON.stringify(projected).length).toBeLessThan(800);
    });

    it.each(["codex", "claude"])(
      "keeps safe screenshot and recording metadata from %s",
      (provider) => {
        const preview = (tool: string, output: object) =>
          activity(
            provider === "codex"
              ? {
                  item: {
                    server: "t3-code",
                    tool,
                    result: {
                      structuredContent: output,
                      content: [{ type: "image", data: "SECRET_BYTES" }],
                    },
                  },
                }
              : {
                  toolName: `mcp__t3-code__${tool}`,
                  result: {
                    content: JSON.stringify({
                      content: [{ type: "text", text: JSON.stringify(output) }],
                    }),
                  },
                },
            { itemType: "mcp_tool_call" },
          );
        const screenshot = projectActivityPayload(
          preview("preview_snapshot", {
            screenshotPath: "/tmp/shot.png",
            url: "https://user:secret@example.com",
            prose: "SECRET_PROSE",
          }),
        );
        const recording = {
          ...projectActivityPayload(
            preview("preview_recording_stop", { path: "/tmp/run.webm", mimeType: "video/webm" }),
          ),
          id: EventId.make("a2"),
        };
        expect(result(ingest([screenshot, recording]))?.visuals).toEqual([
          { kind: "image", path: "/tmp/shot.png" },
          { kind: "video", path: "/tmp/run.webm" },
        ]);
        expect(JSON.stringify(screenshot)).not.toMatch(/SECRET|secret|user:/);
      },
    );

    it.each([
      { isError: true, structuredContent: { screenshotPath: "/tmp/shot.png" } },
      { is_error: true, content: JSON.stringify({ screenshotPath: "/tmp/shot.png" }) },
      { structuredContent: { screenshotPath: "//user:secret@host/shot.png" } },
      { structuredContent: { screenshotPath: "/tmp/shot.png?token=secret" } },
      { structuredContent: { screenshotPath: "https://user:secret@host/shot.png" } },
      { structuredContent: { screenshotPath: "/tmp/shot.png\nprose" } },
      { structuredContent: { screenshotPath: "/tmp/" + "x".repeat(1024) + ".png" } },
    ])("rejects unsuccessful or unsafe preview metadata %j", (output) => {
      const preview = projectActivityPayload(
        activity(
          { item: { server: "t3-code", tool: "preview_snapshot", result: output } },
          { itemType: "mcp_tool_call" },
        ),
      );
      expect(result(ingest([preview]))?.visuals ?? []).toEqual([]);
    });

    it.each(["failed", "declined", "stopped"])(
      "preserves %s through live merge and snapshot dedupe",
      (status) => {
        const first = {
          ...activity({ item: { command: "make" } }, { toolCallId: "call", status }),
          kind: "tool.updated",
        };
        const completed = {
          ...activity({ item: { command: "make" } }, { toolCallId: "call" }),
          id: EventId.make("a2"),
        };
        const activities = [first, completed];
        const snapshot = projectThreadDetailSnapshot({
          thread: { messages: [], activities },
        } as unknown as OrchestrationThreadDetailSnapshot);
        expect(result(ingest(activities.map(projectActivityPayload)))?.commands[0]?.status).toBe(
          status,
        );
        expect(result(ingest(snapshot.thread.activities))?.commands[0]?.status).toBe(status);
      },
    );

    it("keeps a nonzero exit from an update when a wrapper completes without it", () => {
      const first = {
        ...activity({ item: { command: "make", exitCode: 4 } }, { toolCallId: "call" }),
        kind: "tool.updated",
      };
      const last = {
        ...activity({ item: { command: "make" } }, { toolCallId: "call" }),
        id: EventId.make("a2"),
      };
      const snapshot = projectThreadDetailSnapshot({
        thread: { messages: [], activities: [first, last] },
      } as unknown as OrchestrationThreadDetailSnapshot);
      expect(result(ingest(snapshot.thread.activities))?.commands[0]).toMatchObject({
        status: "failed",
        exitCode: 4,
      });
    });
    it("cannot replace a failed subprocess exit with a later completed wrapper's Exit 0", () => {
      const first = {
        ...activity({ item: { command: "make", exitCode: 4 } }, { toolCallId: "call" }),
        kind: "tool.updated",
      };
      const last = {
        ...activity({ item: { command: "make", exitCode: 0 } }, { toolCallId: "call" }),
        id: EventId.make("a2"),
      };
      expect(result(ingest([first, last].map(projectActivityPayload)))?.commands[0]).toMatchObject({
        status: "failed",
        exitCode: 4,
      });
    });
    it("keeps a trailing exit marker on an update's detail through snapshot deduplication", () => {
      const first = {
        ...activity(
          { item: { command: "make" } },
          { toolCallId: "call", detail: "output\n<exited with exit code 9>" },
        ),
        kind: "tool.updated",
      };
      const last = {
        ...activity({ item: { command: "make" } }, { toolCallId: "call" }),
        id: EventId.make("a2"),
      };
      const snapshot = projectThreadDetailSnapshot({
        thread: { messages: [], activities: [first, last] },
      } as unknown as OrchestrationThreadDetailSnapshot);
      expect(result(ingest(snapshot.thread.activities))?.commands[0]).toMatchObject({
        status: "failed",
        exitCode: 9,
      });
    });
  });

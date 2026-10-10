import { describe, expect, it } from "vite-plus/test";

import {
  deriveTaskResults,
  describeRecordedCommand,
  structuredCommandExitCode,
  type TaskResultWorkEntry,
} from "./taskResults.ts";

type Overrides = { [K in keyof TaskResultWorkEntry]?: TaskResultWorkEntry[K] | undefined };

function entry(id: string, overrides: Overrides = {}): TaskResultWorkEntry {
  return {
    id,
    label: "Ran command",
    tone: "tool",
    turnId: "t1",
    itemType: "command_execution",
    command: "vp test run",
    toolLifecycleStatus: "completed",
    ...overrides,
  } as TaskResultWorkEntry;
}

function derive(workEntries: TaskResultWorkEntry[]) {
  return deriveTaskResults({
    workEntries,
    messages: [
      { id: "final", role: "assistant", turnId: "t1" },
      { id: "other", role: "assistant", turnId: "t2" },
    ],
    terminalAssistantMessageIds: new Set(["final", "other"]),
    activeTurnIds: new Set(),
    latestTurn: null,
  });
}

const preview = (tool: string, result: unknown, id = tool): TaskResultWorkEntry =>
  entry(id, {
    itemType: "mcp_tool_call",
    command: undefined,
    toolData: { item: { server: "t3-code", tool, result } },
  });

describe("structuredCommandExitCode", () => {
  it("reads only exit fields on item, item.result and rawOutput", () => {
    expect(structuredCommandExitCode({ item: { exitCode: 4 } })).toBe(4);
    expect(structuredCommandExitCode({ item: { result: { exit_code: 0 } } })).toBe(0);
    expect(structuredCommandExitCode({ rawOutput: { exitCode: -1 } })).toBe(-1);
    expect(structuredCommandExitCode({ item: { exitCode: "1" }, usage: { exitCode: 3 } })).toBe(
      undefined,
    );
    expect(structuredCommandExitCode({ item: { exitCode: 1.5 } })).toBe(undefined);
  });
});

describe("deriveTaskResults evidence", () => {
  it("does not count preview artifacts as code edits", () => {
    const result = derive([
      entry("preview", {
        command: undefined,
        itemType: "mcp_tool_call",
        changedFiles: ["/tmp/run.webm"],
        toolData: {
          server: "t3-code",
          tool: "preview_recording_stop",
          result: { path: "/tmp/run.webm", mimeType: "video/webm" },
        },
      }),
    ]).get("final");
    expect(result?.visuals).toHaveLength(1);
    expect(result?.changedFileCount).toBe(0);
  });
  it("uses the owning turn's checkpoint files for an edit-only result", () => {
    const result = deriveTaskResults({
      workEntries: [],
      messages: [{ id: "final", role: "assistant", turnId: "t1" }],
      terminalAssistantMessageIds: new Set(["final"]),
      activeTurnIds: new Set(),
      latestTurn: null,
      changedFileCountByTurnId: new Map([
        ["t1", 2],
        ["other-turn", 9],
      ]),
    }).get("final");
    expect(result).toMatchObject({ turnId: "t1", commands: [], changedFileCount: 2 });
  });
  it("anchors a producer-shaped user without turnId by the stopped turn's time boundary", () => {
    const input = {
      workEntries: [entry("a")],
      messages: [
        { id: "previous", role: "user", turnId: null, createdAt: "2026-10-10T09:00:00Z" },
        { id: "user", role: "user", turnId: null, createdAt: "2026-10-10T10:00:00Z" },
        { id: "next", role: "user", turnId: null, createdAt: "2026-10-10T11:00:00Z" },
      ],
      terminalAssistantMessageIds: new Set<string>(),
      activeTurnIds: new Set<string>(),
      latestTurn: { turnId: "t1", state: "interrupted", requestedAt: "2026-10-10T10:00:01Z" },
    };
    expect([...deriveTaskResults(input).keys()]).toEqual(["user"]);
    expect(
      deriveTaskResults({ ...input, latestTurn: { ...input.latestTurn, state: "running" } }).size,
    ).toBe(0);
  });
  it("does not hide a reported failure behind Exit 0", () => {
    expect(
      describeRecordedCommand({ id: "a", command: "make", status: "failed", exitCode: 0 }),
    ).toBe("Failed, Exit 0");
  });

  it("does not borrow an earlier user's row when the stopped turn's user is unloaded", () => {
    const results = deriveTaskResults({
      workEntries: [entry("a")],
      messages: [
        { id: "old-user", role: "user", turnId: null, createdAt: "2026-10-10T09:00:00Z" },
        {
          id: "old-assistant",
          role: "assistant",
          turnId: "old-turn",
          createdAt: "2026-10-10T09:01:00Z",
        },
      ],
      terminalAssistantMessageIds: new Set<string>(),
      activeTurnIds: new Set<string>(),
      latestTurn: { turnId: "t1", state: "interrupted", requestedAt: "2026-10-10T10:00:00Z" },
    });
    expect(results.size).toBe(0);
  });

  it.each(["interrupted", "error"])(
    "anchors a %s turn without assistant text to its user",
    (state) => {
      const input = {
        workEntries: [entry("a")],
        messages: [{ id: "user", role: "user", turnId: "t1" }],
        terminalAssistantMessageIds: new Set<string>(),
        activeTurnIds: new Set<string>(),
        latestTurn: { turnId: "t1", state },
      };
      expect(deriveTaskResults(input).get("user")).toMatchObject({
        outcome: state,
        commands: [{ command: "vp test run" }],
      });
      expect(deriveTaskResults({ ...input, workEntries: [] }).get("user")?.outcome).toBe(state);
      expect(deriveTaskResults({ ...input, activeTurnIds: new Set(["t1"]) }).size).toBe(0);
      expect(
        deriveTaskResults({ ...input, latestTurn: { turnId: "t2", state: "running" } }).size,
      ).toBe(0);
    },
  );

  it("does not attach to a user when a terminal assistant exists", () => {
    const results = deriveTaskResults({
      workEntries: [entry("a")],
      messages: [
        { id: "user", role: "user", turnId: "t1" },
        { id: "final", role: "assistant", turnId: "t1" },
      ],
      terminalAssistantMessageIds: new Set(["final"]),
      activeTurnIds: new Set(),
      latestTurn: { turnId: "t1", state: "error" },
    });
    expect([...results.keys()]).toEqual(["final"]);
  });

  it("keeps an edit-only completed turn", () => {
    expect(
      derive([
        entry("edit", { command: undefined, itemType: "file_change", changedFiles: ["a.ts"] }),
      ]).get("final"),
    ).toMatchObject({ commands: [], changedFileCount: 1 });
  });

  it("keeps a recorded failure when a later update reports completed", () => {
    const results = derive([
      entry("a", { toolCallId: "c", toolLifecycleStatus: "failed" }),
      entry("b", { toolCallId: "c", toolLifecycleStatus: "completed" }),
    ]);
    expect(results.get("final")?.commands[0]?.status).toBe("failed");
  });

  it("describes each status without claiming a pass", () => {
    const labels = (overrides: Overrides) => {
      const command = derive([entry("a", overrides)]).get("final")!.commands[0]!;
      return describeRecordedCommand(command);
    };
    expect(labels({ exitCode: 0 })).toBe("Exit 0");
    expect(labels({})).toBe("Finished, exit not recorded");
    expect(labels({ exitCode: 2 })).toBe("Exit 2");
    expect(labels({ toolLifecycleStatus: undefined })).toBe("Not checked");
    expect(labels({ toolLifecycleStatus: "declined", exitCode: 1 })).toBe("Declined");
  });

  it("links a preview screenshot only from the structured result of a completed call", () => {
    const results = derive([
      preview("preview_snapshot", { structuredContent: { screenshotPath: "/tmp/shot.png" } }),
      preview("preview_recording_stop", { path: "/tmp/run.webm", mimeType: "video/webm" }),
      entry("viewed", { viewedImagePath: "/tmp/viewed.png", command: undefined }),
    ]);
    expect(results.get("final")?.visuals).toEqual([
      { kind: "image", path: "/tmp/shot.png" },
      { kind: "video", path: "/tmp/run.webm" },
    ]);
  });

  it("records no visual when the result lacks a valid path or the call failed", () => {
    const results = derive([
      preview("preview_snapshot", { content: "took a screenshot" }, "text"),
      preview("preview_snapshot", { screenshotPath: "shot.png" }, "relative"),
      preview("preview_snapshot", { screenshotPath: "/tmp/a.png\n/etc/x" }, "newline"),
      preview(
        "preview_snapshot",
        { isError: true, structuredContent: { screenshotPath: "/tmp/failed.png" } },
        "result-error",
      ),
      entry("item-error", {
        command: undefined,
        itemType: "mcp_tool_call",
        toolData: {
          server: "t3-code",
          tool: "preview_snapshot",
          status: "failed",
          result: { screenshotPath: "/tmp/failed.png" },
        },
      }),
      {
        ...preview("preview_snapshot", { screenshotPath: "/tmp/ok.png" }, "failed"),
        toolLifecycleStatus: "failed",
      },
      entry("cmd"),
    ]);
    expect(results.get("final")?.visuals).toEqual([]);
  });

  it("never attaches another turn's evidence", () => {
    const results = derive([entry("a", { turnId: "t2", exitCode: 1 }), entry("b")]);
    expect(results.get("final")?.commands.map((c) => c.id)).toEqual(["b"]);
    expect(results.get("other")?.commands.map((c) => c.id)).toEqual(["a"]);
  });

  it("drops credential-bearing PR links and strips query and fragment", () => {
    const link = (id: string, url: string) =>
      entry(id, {
        itemType: "mcp_tool_call",
        command: undefined,
        toolTitle: "t3-code.link_pull_request",
        toolData: { server: "t3-code", tool: "link_pull_request", arguments: { url } },
      });
    const results = derive([
      link("a", "https://user:secret@github.com/acme/app/pull/1"),
      link("b", "https://github.com/acme/app/pull/2?token=abc#frag"),
    ]);
    expect(results.get("final")?.pullRequests).toEqual([
      { url: "https://github.com/acme/app/pull/2", number: 2 },
    ]);
  });
});

import { parseChangeRequestUrl } from "@t3tools/shared/changeRequestUrl";
import { isWorkspaceImagePreviewPath } from "@t3tools/shared/filePreview";
import { isWindowsAbsolutePath } from "@t3tools/shared/path";
import { videoMimeType } from "@t3tools/shared/video";

import {
  resolveWorkEntryToolPresentation,
  toolGroupAction,
  workEntryDisplayIndicatesToolFailure,
  type WorkLogPresentationEntry,
} from "./presentation.ts";

/**
 * What the provider reported for one command, never inferred from prose.
 * "completed" means the tool call closed; only `exitCode` says how the
 * subprocess ended, and the provider often does not report it.
 */
export type RecordedCommandStatus =
  | "completed"
  | "failed"
  | "declined"
  | "running"
  | "stopped"
  | "unknown";

export interface RecordedCommand {
  id: string;
  command: string;
  status: RecordedCommandStatus;
  /** Present only when the provider reported it as a structured field or exit marker. */
  exitCode?: number;
}

export interface RecordedPullRequest {
  url: string;
  number: number;
}

/** A screenshot or recording a preview tool reported producing in this turn. */
export interface RecordedVisual {
  kind: "image" | "video";
  path: string;
}

export interface TaskResult {
  turnId: string;
  /** Only the latest turn exposes its end state; earlier settled turns read "settled". */
  outcome: "settled" | "interrupted" | "error";
  commands: ReadonlyArray<RecordedCommand>;
  pullRequests: ReadonlyArray<RecordedPullRequest>;
  visuals: ReadonlyArray<RecordedVisual>;
  changedFileCount: number;
}

/** The smallest work-entry shape both clients already produce. */
export interface TaskResultWorkEntry extends WorkLogPresentationEntry {
  readonly id: string;
  readonly exitCode?: number | undefined;
}

export interface TaskResultMessage {
  readonly id: string;
  readonly role: string;
  readonly streaming?: boolean | undefined;
  readonly turnId?: string | null | undefined;
  readonly createdAt?: string | undefined;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/**
 * Reads only fields named `exitCode`/`exit_code`. Wrapper status, token counts,
 * and any other number never count. Codex puts it on the item, ACP providers
 * on `rawOutput`, older MCP-style results on `item.result`.
 */
export function structuredCommandExitCode(dataValue: unknown): number | undefined {
  const data = asRecord(dataValue);
  const item = asRecord(data?.item);
  for (const holder of [
    item,
    asRecord(item?.result),
    asRecord(data?.rawOutput),
    asRecord(data?.result),
  ]) {
    for (const value of [holder?.exitCode, holder?.exit_code]) {
      if (typeof value === "number" && Number.isSafeInteger(value)) return value;
    }
  }
  return undefined;
}

export function recordedCommandStatus(entry: TaskResultWorkEntry): RecordedCommandStatus {
  switch (entry.toolLifecycleStatus) {
    case "inProgress":
      return "running";
    case "stopped":
      return "stopped";
    case "declined":
      return "declined";
    case "failed":
      return "failed";
    default:
      break;
  }
  if (workEntryDisplayIndicatesToolFailure(entry)) return "failed";
  if (entry.exitCode !== undefined && entry.exitCode !== 0) return "failed";
  return entry.toolLifecycleStatus === "completed" ? "completed" : "unknown";
}

/** Short, exact wording for one command. It never says a test passed. */
export function describeRecordedCommand(command: RecordedCommand): string {
  const exit = command.exitCode === undefined ? null : `Exit ${command.exitCode}`;
  switch (command.status) {
    case "completed":
      return exit ?? "Finished, exit not recorded";
    case "failed":
      return command.exitCode === 0 ? "Failed, Exit 0" : (exit ?? "Failed");
    case "declined":
      return "Declined";
    case "running":
      return "Still running";
    case "stopped":
      return exit ? `Stopped, ${exit}` : "Stopped";
    case "unknown":
      return "Not checked";
  }
}

/** Shown with every card: a recorded exit is one command, not the whole task. */
export const TASK_RESULT_CAVEAT =
  "Recorded exits cover single commands only. They do not show the whole task was checked or that it looks right.";

export function taskResultSummary(result: TaskResult): string {
  const { commands } = result;
  if (commands.length === 0) return "No commands recorded";
  const attention = commands.filter((command) => command.status !== "completed").length;
  const total = `${commands.length} command${commands.length === 1 ? "" : "s"} recorded`;
  return attention === 0
    ? total
    : `${total}, ${attention} need${attention === 1 ? "s" : ""} attention`;
}

export function taskResultNeedsAttention(result: TaskResult): boolean {
  return (
    result.outcome !== "settled" ||
    result.commands.some((command) => command.status !== "completed")
  );
}

const PR_LINK_FIELDS = ["arguments", "input", "rawInput"] as const;

/** A PR counts only when a completed link call carried a URL the shared parser accepts. */
function recordedPullRequest(entry: TaskResultWorkEntry): RecordedPullRequest | null {
  if (entry.toolLifecycleStatus !== "completed") return null;
  if (resolveWorkEntryToolPresentation(entry)?.action !== "link-pr") return null;
  const data = asRecord(entry.toolData);
  const input = PR_LINK_FIELDS.map((key) => asRecord(data?.[key])).find(Boolean);
  if (typeof input?.url !== "string") return null;
  const target = parseChangeRequestUrl(input.url);
  if (!target) return null;
  const url = new URL(input.url);
  // Drop credentials, query and fragment: the card shows a link, not whatever was pasted.
  return url.username || url.password
    ? null
    : { url: `${url.origin}${url.pathname}`, number: target.number };
}

const PREVIEW_VISUAL_TOOL =
  /^(?:mcp__)?(?:t3-code|t3_code|t3code)_{1,2}preview_(snapshot|recording_stop)$/;

function isAbsoluteLocalPath(path: string): boolean {
  return (
    path.length <= 1024 &&
    // oxlint-disable-next-line no-control-regex -- Provider paths must reject control bytes.
    !/[\x00-\x1f?#]/.test(path) &&
    !path.startsWith("//") &&
    !path.startsWith("\\\\") &&
    (path.startsWith("/") || isWindowsAbsolutePath(path))
  );
}

/**
 * Only the structured result of a completed T3 preview tool counts: the
 * screenshot path or the stopped recording's artifact path. `viewedImagePath`
 * is a file the agent looked at, never proof that this turn produced it. The
 * server keeps only bounded path metadata from these successful tools.
 */
function recordedVisual(entry: TaskResultWorkEntry): RecordedVisual | null {
  if (entry.toolLifecycleStatus !== "completed") return null;
  const data = asRecord(entry.toolData);
  const item = asRecord(data?.item) ?? data;
  const name =
    item && typeof item.server === "string" && typeof item.tool === "string"
      ? `mcp__${item.server}__${item.tool}`
      : (data?.toolName ?? data?.tool);
  const match = typeof name === "string" ? PREVIEW_VISUAL_TOOL.exec(name) : null;
  if (!match) return null;
  const result = asRecord(item?.result ?? data?.result);
  const structured = asRecord(result?.structuredContent) ?? result;
  if (
    item?.error != null ||
    item?.status === "failed" ||
    item?.status === "declined" ||
    item?.status === "stopped" ||
    asRecord(data?.state)?.status === "error" ||
    result?.isError === true ||
    result?.is_error === true ||
    structured?.isError === true ||
    structured?.is_error === true
  )
    return null;
  if (match[1] === "snapshot") {
    const path = structured?.screenshotPath;
    return typeof path === "string" &&
      isAbsoluteLocalPath(path) &&
      isWorkspaceImagePreviewPath(path)
      ? { kind: "image", path }
      : null;
  }
  const path = structured?.path;
  const mimeType = structured?.mimeType;
  return typeof path === "string" &&
    isAbsoluteLocalPath(path) &&
    typeof mimeType === "string" &&
    videoMimeType({ name: path, mimeType: "" }) === mimeType
    ? { kind: "video", path }
    : null;
}

/** Results are rebuilt on every derivation; the stable-rows cache compares by value. */
export function taskResultsEqual(a: TaskResult | undefined, b: TaskResult | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.turnId === b.turnId &&
    a.outcome === b.outcome &&
    a.changedFileCount === b.changedFileCount &&
    a.commands.length === b.commands.length &&
    a.commands.every((command, index) => {
      const other = b.commands[index]!;
      return (
        command.id === other.id &&
        command.command === other.command &&
        command.status === other.status &&
        command.exitCode === other.exitCode
      );
    }) &&
    a.pullRequests.length === b.pullRequests.length &&
    a.pullRequests.every((pr, index) => pr.url === b.pullRequests[index]!.url) &&
    a.visuals.length === b.visuals.length &&
    a.visuals.every((visual, index) => visual.path === b.visuals[index]!.path)
  );
}

const SETTLED_LIFECYCLE = new Set(["failed", "declined", "stopped"]);

/**
 * Latest lifecycle update wins, but a status-less update never erases a known
 * status or exit code, and a recorded failure, decline or stop is never
 * overwritten by a later "completed" for the same call.
 */
function mergeLifecycle(
  previous: TaskResultWorkEntry | undefined,
  next: TaskResultWorkEntry,
): TaskResultWorkEntry {
  if (!previous) return next;
  const previousStatus = recordedCommandStatus(previous);
  const keepPreviousStatus =
    previous.toolLifecycleStatus !== undefined &&
    (next.toolLifecycleStatus === undefined ||
      (SETTLED_LIFECYCLE.has(previousStatus) && !SETTLED_LIFECYCLE.has(next.toolLifecycleStatus)));
  const command = next.command ?? previous.command;
  const exitCode =
    previous.exitCode !== undefined && previous.exitCode !== 0
      ? previous.exitCode
      : (next.exitCode ?? previous.exitCode);
  return {
    ...next,
    ...(command !== undefined ? { command } : {}),
    ...(exitCode !== undefined ? { exitCode } : {}),
    ...(previousStatus === "failed"
      ? { toolLifecycleStatus: "failed" as const }
      : keepPreviousStatus
        ? { toolLifecycleStatus: previous.toolLifecycleStatus }
        : {}),
  };
}

/**
 * Attach evidence to its settled turn's terminal assistant, or to the owning
 * user message when the latest turn stops/errors before any assistant text.
 * A user without a turn id needs the turn's recorded time boundary.
 */
export function deriveTaskResults(input: {
  workEntries: Iterable<TaskResultWorkEntry>;
  messages: Iterable<TaskResultMessage>;
  terminalAssistantMessageIds: ReadonlySet<string>;
  activeTurnIds: ReadonlySet<string>;
  latestTurn: {
    turnId: string;
    state: string;
    requestedAt?: string;
    startedAt?: string | null;
  } | null;
  changedFileCountByMessageId?: ReadonlyMap<string, number>;
  changedFileCountByTurnId?: ReadonlyMap<string, number>;
}): Map<string, TaskResult> {
  const entriesByTurn = new Map<string, Map<string, TaskResultWorkEntry>>();
  for (const entry of input.workEntries) {
    if (!entry.turnId) continue;
    let turnEntries = entriesByTurn.get(entry.turnId);
    if (!turnEntries) {
      turnEntries = new Map();
      entriesByTurn.set(entry.turnId, turnEntries);
    }
    const key = entry.toolCallId ? `call:${entry.toolCallId}` : `entry:${entry.id}`;
    turnEntries.set(key, mergeLifecycle(turnEntries.get(key), entry));
  }

  const messages = [...input.messages];
  const assistantTurnIds = new Set(
    messages
      .filter(
        (message) =>
          message.role === "assistant" && input.terminalAssistantMessageIds.has(message.id),
      )
      .map((message) => message.turnId),
  );
  const boundary = input.latestTurn?.requestedAt ?? input.latestTurn?.startedAt;
  const boundaryMessage = boundary
    ? messages.findLast(
        (message) => !message.createdAt || Date.parse(message.createdAt) <= Date.parse(boundary),
      )
    : undefined;
  const userAnchorMessage =
    input.latestTurn &&
    (input.latestTurn.state === "interrupted" || input.latestTurn.state === "error") &&
    !assistantTurnIds.has(input.latestTurn.turnId)
      ? (messages.findLast(
          (message) => message.role === "user" && message.turnId === input.latestTurn!.turnId,
        ) ??
        (boundaryMessage?.role === "user" && !boundaryMessage.turnId && boundaryMessage.createdAt
          ? boundaryMessage
          : undefined))
      : undefined;
  const results = new Map<string, TaskResult>();
  for (const message of messages) {
    const userAnchor = message === userAnchorMessage;
    const turnId = userAnchor ? input.latestTurn!.turnId : message.turnId;
    const latest =
      input.latestTurn && input.latestTurn.turnId === turnId ? input.latestTurn.state : null;
    if (
      !(
        userAnchor ||
        (message.role === "assistant" && input.terminalAssistantMessageIds.has(message.id))
      ) ||
      message.streaming ||
      !turnId ||
      input.activeTurnIds.has(turnId)
    ) {
      continue;
    }
    const commands: RecordedCommand[] = [];
    const pullRequests = new Map<string, RecordedPullRequest>();
    const visuals = new Map<string, RecordedVisual>();
    const changedFiles = new Set<string>();
    for (const entry of entriesByTurn.get(turnId)?.values() ?? []) {
      if (entry.toolLifecycleStatus === "completed" && toolGroupAction(entry) === "edit") {
        for (const path of entry.changedFiles ?? []) changedFiles.add(path);
      }
      const pullRequest = recordedPullRequest(entry);
      const visual = pullRequest ? null : recordedVisual(entry);
      if (pullRequest) {
        pullRequests.set(pullRequest.url, pullRequest);
      } else if (visual) {
        visuals.set(visual.path, visual);
      } else if (entry.command?.trim() && toolGroupAction(entry) === "command") {
        commands.push({
          id: entry.id,
          command: entry.command.trim(),
          status: recordedCommandStatus(entry),
          ...(entry.exitCode !== undefined ? { exitCode: entry.exitCode } : {}),
        });
      }
    }
    const changedFileCount =
      input.changedFileCountByMessageId?.get(message.id) ??
      input.changedFileCountByTurnId?.get(turnId) ??
      changedFiles.size;
    if (
      commands.length === 0 &&
      pullRequests.size === 0 &&
      visuals.size === 0 &&
      changedFileCount === 0 &&
      latest !== "interrupted" &&
      latest !== "error"
    ) {
      continue;
    }
    results.set(message.id, {
      turnId,
      outcome: latest === "interrupted" || latest === "error" ? latest : "settled",
      commands,
      pullRequests: [...pullRequests.values()],
      visuals: [...visuals.values()],
      changedFileCount,
    });
  }
  return results;
}

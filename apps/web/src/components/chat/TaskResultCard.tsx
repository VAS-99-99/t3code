import { memo, type ComponentProps } from "react";
import {
  TASK_RESULT_CAVEAT,
  describeRecordedCommand,
  taskResultNeedsAttention,
  taskResultSummary,
} from "@t3tools/client-runtime/task-results";

import { cn } from "~/lib/utils";
import type { TaskResult } from "./TaskResult.logic";
import ChatMarkdown from "../ChatMarkdown";

const OUTCOME_LABEL: Record<TaskResult["outcome"], string | null> = {
  settled: null,
  interrupted: "Stopped before finishing",
  error: "Ended with an error",
};

function fileName(path: string): string {
  return path.split(/[\\/]/).at(-1) || path;
}

/**
 * Quiet record of what the provider reported for a finished turn. It never
 * says the task was verified: a command exit is not a passing test or a visual
 * check. Open by default only when something needs attention.
 */
export const TaskResultCard = memo(function TaskResultCard({
  result,
  cwd,
  threadRef,
  onImageExpand,
}: Pick<ComponentProps<typeof ChatMarkdown>, "cwd" | "threadRef" | "onImageExpand"> & {
  result: TaskResult;
}) {
  const outcomeLabel = OUTCOME_LABEL[result.outcome];

  return (
    <details
      data-task-result=""
      open={taskResultNeedsAttention(result)}
      className="mt-2 rounded-lg bg-secondary/60 px-3 py-1.5 text-xs text-muted-foreground dark:bg-input/10"
    >
      <summary
        data-scroll-anchor-ignore
        className="cursor-pointer select-none font-medium text-foreground/80"
      >
        Recorded result
        <span className="ms-2 font-normal text-muted-foreground">
          {outcomeLabel ? `${outcomeLabel} · ` : ""}
          {taskResultSummary(result)}
        </span>
      </summary>
      <ul className="mt-1.5 space-y-1">
        {result.commands.map((command) => (
          <li key={command.id} className="flex min-w-0 items-baseline gap-2">
            <span
              className={cn(
                "w-28 shrink-0",
                command.status === "failed" && "text-destructive",
                command.status === "completed" && "text-foreground/70",
              )}
            >
              {describeRecordedCommand(command)}
            </span>
            <code className="min-w-0 whitespace-pre-wrap break-words font-mono">
              {command.command}
            </code>
          </li>
        ))}
        {result.pullRequests.map((pullRequest) => (
          <li key={pullRequest.url}>
            <a
              href={pullRequest.url}
              target="_blank"
              rel="noopener noreferrer"
              className="underline underline-offset-2 hover:text-foreground"
            >
              Linked PR #{pullRequest.number}
            </a>
          </li>
        ))}
        <li>
          {result.visuals.length === 0 ? (
            "Visual check: Not recorded"
          ) : (
            <>
              Visual evidence:{" "}
              {result.visuals.map((visual) => (
                <ChatMarkdown
                  key={visual.path}
                  text={`[${fileName(visual.path).replace(/[[\]]/g, "")}](<${encodeURI(visual.path)}>)`}
                  cwd={cwd}
                  threadRef={threadRef}
                  onImageExpand={onImageExpand}
                />
              ))}
            </>
          )}
        </li>
      </ul>
      <p className="mt-1.5">{TASK_RESULT_CAVEAT}</p>
    </details>
  );
});

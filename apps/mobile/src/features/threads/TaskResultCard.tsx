import {
  TASK_RESULT_CAVEAT,
  describeRecordedCommand,
  taskResultNeedsAttention,
  taskResultSummary,
  type TaskResult,
  type RecordedVisual,
} from "@t3tools/client-runtime/task-results";
import { memo, useState } from "react";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { tryOpenExternalUrl } from "../../lib/openExternalUrl";

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
export const TaskResultCard = memo(function TaskResultCard(props: {
  readonly result: TaskResult;
  readonly onOpenVisual: (visual: RecordedVisual) => void;
  readonly onOpenDiff: (turnId: string) => void;
}) {
  const { result } = props;
  const [expandedOverride, setExpandedOverride] = useState<boolean | null>(null);
  const expanded = expandedOverride ?? taskResultNeedsAttention(result);
  const outcomeLabel = OUTCOME_LABEL[result.outcome];

  return (
    <View className="mt-2 gap-1.5 rounded-[14px] bg-subtle px-3 py-2">
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`Recorded result. ${taskResultSummary(result)}`}
        onPress={() => setExpandedOverride(!expanded)}
      >
        <Text className="font-t3-medium text-xs text-foreground-secondary">
          Recorded result · {outcomeLabel ? `${outcomeLabel} · ` : ""}
          {taskResultSummary(result)}
        </Text>
      </Pressable>
      {expanded ? (
        <View className="gap-1.5">
          {result.commands.map((command) => (
            <View key={command.id} className="gap-0.5">
              <Text
                className={
                  command.status === "failed"
                    ? "font-t3-medium text-xs text-danger-foreground"
                    : "font-t3-medium text-xs text-foreground-secondary"
                }
              >
                {describeRecordedCommand(command)}
              </Text>
              <Text selectable className="font-mono text-xs text-foreground-secondary">
                {command.command}
              </Text>
            </View>
          ))}
          {result.pullRequests.map((pullRequest) => (
            <Pressable
              key={pullRequest.url}
              accessibilityRole="link"
              onPress={() => void tryOpenExternalUrl(pullRequest.url, "pull-request")}
            >
              <Text className="font-sans text-xs text-foreground-secondary">
                Linked PR #{pullRequest.number}
              </Text>
            </Pressable>
          ))}
          <Text className="font-sans text-xs text-foreground-secondary">
            {result.visuals.length === 0 ? "Visual check: Not recorded" : "Visual evidence:"}
          </Text>
          {result.visuals.map((visual) => (
            <Pressable
              key={visual.path}
              accessibilityRole="button"
              accessibilityLabel={`Open ${visual.kind} ${fileName(visual.path)}`}
              onPress={() => props.onOpenVisual(visual)}
            >
              <Text className="font-sans text-xs text-foreground-secondary">
                {fileName(visual.path)}
              </Text>
            </Pressable>
          ))}
          {result.changedFileCount > 0 ? (
            <Pressable accessibilityRole="button" onPress={() => props.onOpenDiff(result.turnId)}>
              <Text className="font-sans text-xs text-foreground-secondary">
                View turn diff · {result.changedFileCount} files
              </Text>
            </Pressable>
          ) : null}
          <Text className="font-sans text-xs text-foreground-secondary">{TASK_RESULT_CAVEAT}</Text>
        </View>
      ) : null}
    </View>
  );
});

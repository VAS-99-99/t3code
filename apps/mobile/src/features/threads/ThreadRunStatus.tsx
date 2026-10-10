import {
  latestRecoveryNote,
  resolveThreadRunStatus,
  type EnvironmentConnectionPhase,
} from "@t3tools/client-runtime/connection";
import type { OrchestrationThreadShell, ServerConfig as T3ServerConfig } from "@t3tools/contracts";
import { useMemo } from "react";
import { View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { useSelectedThreadDetail } from "../../state/use-thread-detail";

const formatTime = (iso: string) =>
  new Date(iso).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

/**
 * One text strip saying why the thread is not moving and who must act. The
 * approval and question answers stay on their existing cards; this only names
 * the cause with the machine, provider and account it applies to.
 */
export function ThreadRunStatus(props: {
  readonly thread: OrchestrationThreadShell;
  readonly serverConfig: T3ServerConfig | null;
  readonly connectionState: EnvironmentConnectionPhase;
  readonly environmentLabel: string | null;
  readonly hasPendingApproval: boolean;
  readonly hasPendingUserInput: boolean;
}) {
  const detail = useSelectedThreadDetail();
  const activities = detail?.activities;
  const provider = props.serverConfig?.providers.find(
    (candidate) =>
      candidate.instanceId ===
      (props.thread.session?.providerInstanceId ?? props.thread.modelSelection.instanceId),
  );
  const status = useMemo(
    () =>
      resolveThreadRunStatus({
        connected: props.connectionState === "connected",
        machineLabel: props.environmentLabel,
        providerLabel: provider?.displayName ?? null,
        accountLabel: provider?.auth.email ?? null,
        providerAuth: provider?.auth.status ?? null,
        providerBannerVisible: false,
        pendingApprovalCount: props.hasPendingApproval ? 1 : 0,
        pendingQuestionCount: props.hasPendingUserInput ? 1 : 0,
        recovery: latestRecoveryNote(activities ?? [], props.thread.latestTurn),
        formatTime,
      }),
    [
      activities,
      props.connectionState,
      props.environmentLabel,
      props.hasPendingApproval,
      props.hasPendingUserInput,
      props.thread.latestTurn,
      provider,
    ],
  );
  if (status === null) return null;
  return (
    <View className="shrink-0 gap-0.5 px-4 pb-3" accessibilityRole="alert">
      <Text
        className={
          status.variant === "warning"
            ? "text-sm font-t3-bold text-foreground"
            : "text-sm font-t3-bold text-foreground-muted"
        }
      >
        {status.title}
      </Text>
      <Text className="text-xs text-foreground-muted">{status.description}</Text>
    </View>
  );
}

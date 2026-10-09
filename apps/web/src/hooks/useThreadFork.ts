import type { ScopedThreadRef } from "@t3tools/contracts";
import { CommandId } from "@t3tools/contracts";
import { useRouter } from "@tanstack/react-router";
import { useCallback } from "react";

import { createThreadForkIntents, forkThread } from "../components/threadFork.logic";
import { stackedThreadToast, toastManager } from "../components/ui/toast";
import { newThreadId, randomUUID } from "../lib/utils";
import { waitForThreadShell } from "../state/entities";
import { threadFork } from "../state/threadFork";
import { useAtomCommand } from "../state/use-atom-command";
import { buildThreadRouteParams } from "../threadRoutes";

// Module scope: the sidebar row and the chat header menu fork the same source
// thread, and a retry from either must reuse the same ids.
const forkIntents = createThreadForkIntents(() => ({
  commandId: CommandId.make(randomUUID()),
  destinationThreadId: newThreadId(),
}));

/**
 * Forks a thread on its own environment and opens the copy. `navigate`
 * defaults to a plain route change; the sidebar passes its own so the mobile
 * drawer closes.
 */
export function useThreadFork() {
  const router = useRouter();
  const send = useAtomCommand(threadFork, { reportFailure: false });
  return useCallback(
    (sourceRef: ScopedThreadRef, navigate?: (ref: ScopedThreadRef) => Promise<void>) =>
      forkThread(
        {
          intents: forkIntents,
          send,
          waitForThreadShell,
          navigate:
            navigate ??
            ((ref) =>
              router.navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(ref),
              })),
          reportFailure: (title, description) =>
            toastManager.add(stackedThreadToast({ type: "error", title, description })),
        },
        sourceRef,
      ),
    [router, send],
  );
}

import { WS_METHODS } from "@t3tools/contracts";
import { createEnvironmentRpcCommand } from "@t3tools/client-runtime/state/runtime";

import { connectionAtomRuntime } from "../connection/runtime";

/** Forks a thread on the environment that owns it. */
export const threadFork = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "environment-data:threads:fork",
  tag: WS_METHODS.threadsFork,
});

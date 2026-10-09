import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-01-01T00:00:00.000Z";
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };

function makeReadModel(input: {
  readonly automaticRecovery?: boolean;
  readonly withThread?: boolean;
}): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [
      {
        id: ProjectId.make("project-1"),
        title: "Project",
        workspaceRoot: "/tmp/project",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
    ],
    threads:
      input.withThread === false
        ? []
        : [
            {
              id: ThreadId.make("thread-1"),
              projectId: ProjectId.make("project-1"),
              title: "Thread",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
              pullRequests: [],
              latestTurn: null,
              createdAt: NOW,
              updatedAt: NOW,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              ...(input.automaticRecovery === undefined
                ? {}
                : { automaticRecovery: input.automaticRecovery }),
              deletedAt: null,
              messages: [],
              proposedPlans: [],
              activities: [],
              checkpoints: [],
              session: null,
            },
          ],
    updatedAt: NOW,
  };
}

const events = (event: Effect.Success<ReturnType<typeof decideOrchestrationCommand>>) =>
  Array.isArray(event) ? event : [event];

const createFields = {
  threadId: ThreadId.make("thread-new"),
  projectId: ProjectId.make("project-1"),
  title: "New",
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  createdAt: NOW,
} as const;

const createdPayload = (decided: ReadonlyArray<{ type: string; payload: unknown }>) => {
  const created = decided.find((event) => event.type === "thread.created");
  return created?.payload as { automaticRecovery?: boolean } | undefined;
};

it.layer(NodeServices.layer)("automatic recovery decider", (it) => {
  it.effect("new threads recover automatically by default", () =>
    Effect.gen(function* () {
      const decided = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.create",
            commandId: CommandId.make("cmd-create"),
            ...createFields,
          },
          readModel: makeReadModel({ withThread: false }),
        }),
      );
      expect(createdPayload(decided)?.automaticRecovery).toBe(true);
    }),
  );

  it.effect("imported history never opts a thread in", () =>
    Effect.gen(function* () {
      const decided = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.create-with-history",
            commandId: CommandId.make("cmd-import"),
            ...createFields,
            messages: [
              {
                messageId: MessageId.make("m1"),
                role: "user",
                text: "old",
                createdAt: NOW,
              },
            ],
          },
          readModel: makeReadModel({ withThread: false }),
        }),
      );
      expect(createdPayload(decided)?.automaticRecovery).toBeFalsy();
    }),
  );

  it.effect("a fork is a new live thread and opts in", () =>
    Effect.gen(function* () {
      const decided = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.create-with-history",
            commandId: CommandId.make("cmd-fork"),
            ...createFields,
            messages: [
              {
                messageId: MessageId.make("m1"),
                role: "user",
                text: "copied",
                createdAt: NOW,
              },
            ],
            fork: {
              sourceThreadId: ThreadId.make("thread-1"),
              commandId: CommandId.make("cmd-fork"),
            },
          },
          readModel: makeReadModel({ withThread: false }),
        }),
      );
      expect(createdPayload(decided)?.automaticRecovery).toBe(true);
    }),
  );

  it.effect("enabling an existing thread is explicit and stamps updatedAt", () =>
    Effect.gen(function* () {
      const [event] = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.recovery.set",
            commandId: CommandId.make("cmd-on"),
            threadId: ThreadId.make("thread-1"),
            enabled: true,
          },
          readModel: makeReadModel({}),
        }),
      );
      expect(event?.type).toBe("thread.recovery-set");
      if (event?.type === "thread.recovery-set") {
        expect(event.payload.enabled).toBe(true);
        expect(event.payload.updatedAt).not.toBe(NOW);
      }
    }),
  );

  it.effect("repeating the current state keeps updatedAt", () =>
    Effect.gen(function* () {
      const [event] = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.recovery.set",
            commandId: CommandId.make("cmd-on-again"),
            threadId: ThreadId.make("thread-1"),
            enabled: true,
          },
          readModel: makeReadModel({ automaticRecovery: true }),
        }),
      );
      if (event?.type === "thread.recovery-set") {
        expect(event.payload.updatedAt).toBe(NOW);
      } else {
        expect.unreachable("expected thread.recovery-set");
      }
    }),
  );

  it.effect("disabling clears the flag", () =>
    Effect.gen(function* () {
      const [event] = events(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.recovery.set",
            commandId: CommandId.make("cmd-off"),
            threadId: ThreadId.make("thread-1"),
            enabled: false,
          },
          readModel: makeReadModel({ automaticRecovery: true }),
        }),
      );
      if (event?.type === "thread.recovery-set") {
        expect(event.payload.enabled).toBe(false);
      } else {
        expect.unreachable("expected thread.recovery-set");
      }
    }),
  );
});

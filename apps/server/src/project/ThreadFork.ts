// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

import {
  CommandId,
  isImportedAgentSessionMessageId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadForkError,
  TrimmedNonEmptyString,
  type ChatAttachment,
  type OrchestrationMessage,
  type OrchestrationMessageContext,
  type OrchestrationThread,
  type ThreadForkFailureReason,
  type ThreadForkInput,
  type ThreadForkResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import {
  parseAttachmentFileExtension,
  resolveAttachmentPath,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";
import { ServerConfig } from "../config.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderService from "../provider/Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";

/**
 * Forks a thread into a new thread in the same project. The conversation up to
 * the source's last completed turn is copied, and the destination gets its own
 * native provider session forked from the source's. No turn runs, and no files
 * or Git branches are copied: both threads keep working in the same workspace.
 */
export class ThreadFork extends Context.Service<
  ThreadFork,
  {
    readonly fork: (input: ThreadForkInput) => Effect.Effect<ThreadForkResult, ThreadForkError>;
  }
>()("t3/project/ThreadFork") {}

/**
 * What the request forked, stored with the staged native copy when
 * the native session is forked. A retry finishes from this record instead of
 * the source's current state: the source may be running, changed or gone by then.
 * Messages are named, not copied; they are read back from the source.
 */
const ForkCapture = Schema.Struct({
  projectId: ProjectId,
  title: TrimmedNonEmptyString,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  messageIds: Schema.Array(MessageId),
});
const encodeForkCapture = Schema.encodeSync(ForkCapture);
const decodeForkCapture = Schema.decodeUnknownEffect(ForkCapture);

// Namespaced like imported history so reverting the fork keeps the copied messages.
const forkedMessageId = (destinationThreadId: string, index: number) =>
  MessageId.make(`import:fork:${destinationThreadId}:${String(index).padStart(6, "0")}`);

/** Stable per request, so a retry overwrites its own copy instead of leaking another. */
function forkedAttachmentId(
  destinationSegment: string,
  commandId: string,
  sourceAttachmentId: string,
): string {
  const hex = NodeCrypto.createHash("sha256")
    .update(`${commandId}\0${sourceAttachmentId}`)
    .digest("hex");
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  const extension = parseAttachmentFileExtension(sourceAttachmentId);
  return `${destinationSegment}-${uuid}${extension ? `-${extension}` : ""}`;
}

/**
 * Messages the native session has finished with: everything created by the
 * last completed turn's end. Without any turn, only imported or forked history
 * qualifies: the provider session it came from already holds all of it.
 * Otherwise a reason says why the thread cannot be forked.
 */
function completedHistory(
  thread: OrchestrationThread,
): { readonly messages: ReadonlyArray<OrchestrationMessage> } | { readonly reason: string } {
  const { session, latestTurn } = thread;
  if (session?.status === "starting" || session?.status === "running" || session?.activeTurnId) {
    return { reason: "the thread is running. Fork it after the turn completes." };
  }
  const messages = thread.messages.filter((message) => message.role !== "system");
  if (messages.some((message) => message.streaming)) {
    return { reason: "a reply is still streaming." };
  }
  if (latestTurn === null) {
    return { messages: messages.filter((message) => isImportedAgentSessionMessageId(message.id)) };
  }
  if (latestTurn.state !== "completed" || latestTurn.completedAt === null) {
    return { reason: "its last turn did not complete, so the native history boundary is unknown." };
  }
  const completedAt = Date.parse(latestTurn.completedAt);
  return { messages: messages.filter((message) => Date.parse(message.createdAt) <= completedAt) };
}

const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const provider = yield* ProviderService.ProviderService;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const fileSystem = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig;
  // ponytail: one fork at a time server-wide. Per-destination locks if that ever queues.
  const lock = yield* Semaphore.make(1);

  /** Copy the files first, so no message ever points at an attachment that is not there. */
  const copyAttachments = Effect.fn("ThreadFork.copyAttachments")(function* (
    input: ThreadForkInput,
    messages: ReadonlyArray<OrchestrationMessage>,
  ) {
    const destinationSegment = toSafeThreadAttachmentSegment(input.destinationThreadId);
    const renamed = new Map<string, string>();
    if (destinationSegment === null) return renamed;
    for (const message of messages) {
      for (const attachment of message.attachments ?? []) {
        const from = resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment });
        if (from === null || renamed.has(attachment.id)) continue;
        const id = forkedAttachmentId(destinationSegment, input.commandId, attachment.id);
        const to = resolveAttachmentPath({
          attachmentsDir: config.attachmentsDir,
          attachment: { ...attachment, id },
        });
        if (to === null) continue;
        yield* fileSystem.copyFile(from, to);
        renamed.set(attachment.id, id);
      }
    }
    return renamed;
  });

  const withForkedAttachments = (
    message: OrchestrationMessage,
    renamed: ReadonlyMap<string, string>,
  ) => {
    const attachments = message.attachments?.map((attachment): ChatAttachment => ({
      ...attachment,
      id: renamed.get(attachment.id) ?? attachment.id,
    }));
    // Context records bind to attachments by id.
    const context = message.context && {
      ...message.context,
      records: message.context.records.map((record) =>
        "attachmentId" in record && typeof record.attachmentId === "string"
          ? { ...record, attachmentId: renamed.get(record.attachmentId) ?? record.attachmentId }
          : record,
      ),
    };
    return {
      ...(attachments ? { attachments } : {}),
      ...(context ? { context: context as OrchestrationMessageContext } : {}),
    };
  };

  const fork = Effect.fn("ThreadFork.fork")(function* (input: ThreadForkInput) {
    const fail = (reason: ThreadForkFailureReason, detail: string, cause?: unknown) =>
      new ThreadForkError({
        reason,
        sourceThreadId: input.sourceThreadId,
        destinationThreadId: input.destinationThreadId,
        detail,
        ...(cause !== undefined ? { cause } : {}),
      });
    const readThread = (threadId: typeof input.sourceThreadId) =>
      snapshots.getThreadDetailById(threadId, { activityKinds: [] }).pipe(
        Effect.map(Option.getOrUndefined),
        Effect.mapError((cause) => fail("history-failed", "Could not read the thread.", cause)),
      );

    // The destination may only be a thread this request already made.
    const destination = yield* readThread(input.destinationThreadId);
    const store = directory.forks;
    if (store === undefined) {
      return yield* fail("provider-failed", "Native fork persistence is unavailable.");
    }
    const staged = yield* store
      .get(input.destinationThreadId)
      .pipe(
        Effect.mapError((cause) =>
          fail("provider-failed", "Could not read the provider binding.", cause),
        ),
      );
    if (destination !== undefined) {
      const owned =
        Option.isSome(staged) &&
        staged.value.sourceThreadId === input.sourceThreadId &&
        staged.value.commandId === input.commandId &&
        staged.value.publishedSequence !== null &&
        staged.value.creationCommandId === `${input.commandId}:create`;
      return owned && destination.deletedAt === null
        ? { destinationThreadId: input.destinationThreadId }
        : yield* fail(
            "destination-conflict",
            "The destination thread already exists or was deleted.",
          );
    }
    if (
      Option.isNone(staged) &&
      Option.isSome(
        yield* directory
          .getBinding(input.destinationThreadId)
          .pipe(
            Effect.mapError((cause) =>
              fail("provider-failed", "Could not read the provider binding.", cause),
            ),
          ),
      )
    ) {
      return yield* fail("destination-conflict", "The destination already has a provider session.");
    }

    // Runs inside the provider's source exclusion, on the first attempt only.
    const capture = Effect.gen(function* () {
      const source = yield* readThread(input.sourceThreadId);
      if (source === undefined || source.deletedAt !== null) {
        return yield* fail("source-not-found", "The source thread does not exist.");
      }
      const history = completedHistory(source);
      if (!("messages" in history)) {
        return yield* fail("source-not-forkable", history.reason);
      }
      if (history.messages.length === 0) {
        return yield* fail("empty-history", "The thread has no completed turn to fork yet.");
      }
      const instanceId = source.modelSelection.instanceId;
      const capabilities = yield* provider
        .getCapabilities(instanceId)
        .pipe(
          Effect.mapError((cause) =>
            fail("unsupported-provider", `Provider '${instanceId}' is not available.`, cause),
          ),
        );
      if (capabilities.supportsConversationFork !== true) {
        return yield* fail(
          "unsupported-provider",
          `Provider '${instanceId}' cannot fork conversations.`,
        );
      }
      return encodeForkCapture({
        projectId: source.projectId,
        title: input.title ?? source.title,
        modelSelection: source.modelSelection,
        runtimeMode: source.runtimeMode,
        interactionMode: source.interactionMode,
        branch: source.branch,
        worktreePath: source.worktreePath,
        messageIds: history.messages.map((message) => message.id),
      });
    });

    // A request that cannot work is turned away before the provider is touched.
    // The capture runs again inside the source's exclusion, and that one counts.
    if (Option.isNone(staged)) yield* capture;

    const publish = Effect.fn("ThreadFork.publish")(function* (
      forked: ProviderService.ProviderForkConversationResult,
    ) {
      // Another same-request caller may have published while this one waited
      // for destination admission. Its completed result no longer needs the source.
      const current = yield* readThread(input.destinationThreadId);
      const previous = yield* store
        .get(input.destinationThreadId)
        .pipe(
          Effect.mapError((cause) =>
            fail("history-failed", "Could not verify fork publication.", cause),
          ),
        );
      if (
        current !== undefined &&
        Option.isSome(previous) &&
        previous.value.publishedSequence !== null &&
        previous.value.creationCommandId === `${input.commandId}:create`
      ) {
        return current.deletedAt === null
          ? undefined
          : yield* fail("destination-conflict", "The destination thread was deleted.");
      }
      const captured = yield* decodeForkCapture(forked.captured).pipe(
        Effect.mapError((cause) =>
          fail("history-failed", "The recorded fork request is unreadable.", cause),
        ),
      );
      // The history is what the fork captured, whatever the source did since.
      const source = yield* readThread(input.sourceThreadId);
      const sourceMessages = new Map(source?.messages.map((message) => [message.id, message]));
      const messages = captured.messageIds.flatMap((id) => sourceMessages.get(id) ?? []);
      if (
        source === undefined ||
        source.deletedAt !== null ||
        messages.length < captured.messageIds.length
      ) {
        return yield* fail(
          "history-failed",
          "The source conversation this fork was taken from is no longer available.",
        );
      }

      const renamed = yield* copyAttachments(input, messages).pipe(
        Effect.mapError((cause) => fail("history-failed", "Could not copy an attachment.", cause)),
      );
      const createdAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
      // One command, so the destination appears with all of its history or not at all.
      yield* engine
        .dispatch({
          type: "thread.create-with-history",
          commandId: CommandId.make(`${input.commandId}:create`),
          threadId: input.destinationThreadId,
          projectId: captured.projectId,
          title: captured.title,
          modelSelection: captured.modelSelection,
          runtimeMode: captured.runtimeMode,
          interactionMode: captured.interactionMode,
          branch: captured.branch,
          worktreePath: captured.worktreePath,
          createdAt,
          fork: { sourceThreadId: input.sourceThreadId, commandId: input.commandId },
          messages: messages.map((message, index) => ({
            messageId: forkedMessageId(input.destinationThreadId, index),
            role: message.role === "assistant" ? "assistant" : "user",
            text: message.text,
            ...withForkedAttachments(message, renamed),
            createdAt: message.createdAt,
          })),
        })
        .pipe(Effect.mapError((cause) => fail("history-failed", cause.message, cause)));
      // A reused command id can acknowledge an earlier, different command. Only
      // publication inside the create transaction proves this thread is ours.
      const published = yield* store
        .get(input.destinationThreadId)
        .pipe(
          Effect.mapError((cause) =>
            fail("history-failed", "Could not verify fork publication.", cause),
          ),
        );
      if (
        Option.isNone(published) ||
        published.value.publishedSequence === null ||
        published.value.creationCommandId !== `${input.commandId}:create`
      ) {
        return yield* fail(
          "destination-conflict",
          "The destination was not created by this fork request.",
        );
      }
    });

    yield* provider
      .forkConversation({
        sourceThreadId: input.sourceThreadId,
        destinationThreadId: input.destinationThreadId,
        commandId: input.commandId,
        capture,
        publish,
      })
      .pipe(
        Effect.mapError((cause) =>
          cause._tag === "ThreadForkError"
            ? cause
            : cause._tag === "ProviderValidationError" && Option.isSome(staged)
              ? fail("destination-conflict", "The destination belongs to another request.", cause)
              : fail("provider-failed", cause.message, cause),
        ),
      );
    return { destinationThreadId: input.destinationThreadId };
  });

  return ThreadFork.of({
    fork: (input) => lock.withPermit(fork(input)),
  });
});

export const layer = Layer.effect(ThreadFork, make);

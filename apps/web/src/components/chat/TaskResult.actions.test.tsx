import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { beforeAll, describe, expect, it, vi } from "vite-plus/test";
import type { TaskResult } from "@t3tools/client-runtime/task-results";

const state = vi.hoisted(() => ({ createAssetUrl: vi.fn() }));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => null }));
vi.mock("../../assets/assetUrls", () => ({
  useAssetUrlRefresh: () => vi.fn(),
  useAssetUrlState: () => ({ _tag: "Loading" }),
}));
vi.mock("../../hooks/useTheme", () => ({ useTheme: () => ({ resolvedTheme: "dark" }) }));
vi.mock("../../state/use-atom-query-runner", () => ({
  useAtomQueryRunner: () => state.createAssetUrl,
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("../../state/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../state/session")>()),
  usePreparedConnection: () => ({
    _tag: "Some",
    value: { httpBaseUrl: "https://environment.test" },
  }),
}));
vi.mock("../../state/entities", () => ({
  readThreadShell: () => null,
  useProjects: () => [],
  useServerConfigs: () => new Map(),
}));
vi.mock("../../remoteOpen", () => ({
  useRemoteOpenResolution: () => ({ state: { mode: "local-exec" }, isResolved: true }),
}));
vi.mock("../../editorPreferences", () => ({
  useOpenInPreferredEditor: () => vi.fn(),
  usePreferredEditor: () => [null, vi.fn()],
}));
vi.mock("~/lib/openPullRequestLink", () => ({
  findProjectOnChangeRequestHost: () => undefined,
  parseChangeRequestUrl: () => null,
  resolvePullRequestPreviewTarget: () => null,
  useOpenChangeRequestLink: () => vi.fn(),
}));
import { TaskResultCard } from "./TaskResultCard";

beforeAll(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal(
    "Element",
    class {
      matches() {
        return false;
      }
    },
  );
  vi.stubGlobal("localStorage", { getItem: () => null, setItem() {}, removeItem() {} });
  vi.stubGlobal("window", {
    Element,
    localStorage,
    addEventListener() {},
    removeEventListener() {},
    matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  });
  vi.stubGlobal("document", {
    getElementById: () => ({}),
    addEventListener() {},
    removeEventListener() {},
    documentElement: { classList: { add() {}, remove() {}, toggle() {} } },
  });
});

describe("mounted result artifact actions", () => {
  it.each([
    { kind: "image" as const, path: "/tmp/shot.png" },
    { kind: "video" as const, path: "/tmp/run.webm" },
  ])("opens $kind through its owning environment asset action", async (visual) => {
    state.createAssetUrl
      .mockReset()
      .mockResolvedValue({ _tag: "Success", value: { relativeUrl: "/api/assets/signed" } });
    const onImageExpand = vi.fn();
    const threadRef = {
      environmentId: EnvironmentId.make("env"),
      threadId: ThreadId.make("thread"),
    };
    const result: TaskResult = {
      turnId: "t1",
      outcome: "error",
      commands: [],
      pullRequests: [],
      visuals: [visual],
      changedFileCount: 0,
    };
    let renderer: ReactTestRenderer | undefined;
    try {
      await act(() => {
        renderer = create(
          <TaskResultCard
            result={result}
            cwd="/project"
            threadRef={threadRef}
            onImageExpand={onImageExpand}
          />,
        );
      });
      const target = renderer!.root
        .findAll((element) => typeof element.type === "string")
        .find(
          (element) =>
            typeof element.props.onClick === "function" &&
            element.findAll((node) =>
              node.children.some(
                (child) =>
                  typeof child === "string" && child.includes(visual.path.split("/").at(-1)!),
              ),
            ).length > 0,
        );
      expect(target, JSON.stringify(renderer!.toJSON())).toBeDefined();
      await act(async () =>
        target!.props.onClick({ preventDefault() {}, stopPropagation() {}, button: 0 }),
      );
      expect(state.createAssetUrl).toHaveBeenCalledWith({
        environmentId: threadRef.environmentId,
        input: {
          resource: { _tag: "media-file", threadId: threadRef.threadId, path: visual.path },
        },
      });
      expect(onImageExpand.mock.calls[0]?.[0]).toMatchObject({
        images: [
          {
            src: "https://environment.test/api/assets/signed",
            ...(visual.kind === "video" ? { type: "video" } : {}),
          },
        ],
      });
    } finally {
      await act(() => renderer?.unmount());
    }
  });
});

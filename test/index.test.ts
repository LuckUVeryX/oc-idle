import { expect, it } from "@effect/vitest";
import type { Plugin } from "@opencode/plugin/effect";
import { Session } from "@opencode/schema/session";
import type { SessionInbox } from "@opencode/schema/session-inbox";
import { Clock, Effect, Logger, Stream } from "effect";
import { TestClock } from "effect/testing";
import { mockDeep } from "vitest-mock-extended";
import plugin from "../src/index.ts";

type Event = Stream.Success<ReturnType<Plugin.Context["event"]["subscribe"]>>;

const finished = (name: string, input: number): Event[] => {
  const sessionID = Session.ID.make(`ses_${name}`);
  return [
    mockDeep<Extract<Event, { type: "session.step.ended" }>>({
      type: "session.step.ended",
      data: { sessionID, tokens: { input, output: 0, cache: { read: 0, write: 0 } } },
    }),
    mockDeep<Extract<Event, { type: "session.execution.succeeded" }>>({
      type: "session.execution.succeeded", data: { sessionID },
    }),
  ];
};

const run = Effect.fnUntraced(function* (events: ReadonlyArray<Event>, options: Plugin.Context["options"] = {}) {
  const requests: Array<{ sessionID: Session.ID; at: number }> = [];
  const logs: unknown[] = [];
  const ctx = mockDeep<Plugin.Context>({ options });
  ctx.event.subscribe.mockReturnValue(Stream.fromIterable(events));
  ctx.session.get.mockReturnValue(Effect.succeed(mockDeep<Session.Info>({ parentID: undefined })));
  ctx.session.compact.mockImplementation(({ sessionID }) => sessionID === "ses_broken"
    ? Effect.fail("SDK failure")
    : Clock.currentTimeMillis.pipe(Effect.map((at) => {
      requests.push({ sessionID, at });
      return mockDeep<SessionInbox.Compaction>();
    })));

  yield* plugin.effect(ctx).pipe(
    Effect.provide(Logger.layer([Logger.make(({ message }) => logs.push(message))])),
  );
  yield* TestClock.adjust("54 minutes");
  return { requests, logs };
});

it.effect("uses the default options", () => Effect.gen(function* () {
  const { requests } = yield* run(finished("large", 200_001));
  expect(requests).toEqual([{ sessionID: "ses_large", at: 3_240_000 }]);
}));

it.effect("new execution cancels only that session's timer", () => Effect.gen(function* () {
  const { requests } = yield* run([
    ...finished("cancelled", 200_001),
    mockDeep<Extract<Event, { type: "session.execution.started" }>>({
      type: "session.execution.started", data: { sessionID: Session.ID.make("ses_cancelled") },
    }),
    ...finished("idle", 200_001),
  ], { idleMinutes: 1 });
  expect(requests).toEqual([{ sessionID: "ses_idle", at: 60_000 }]);
}));

it.effect("skips usage equal to the token limit", () => Effect.gen(function* () {
  const { requests } = yield* run([
    ...finished("equal", 200_000),
    ...finished("above", 200_001),
  ]);
  expect(requests).toEqual([{ sessionID: "ses_above", at: 3_240_000 }]);
}));

it.effect("logs compaction failures without affecting other sessions", () => Effect.gen(function* () {
  const { requests, logs } = yield* run([
    ...finished("broken", 2),
    ...finished("healthy", 2),
  ], { minTokens: 1 });
  expect(logs).toEqual([
    ["oc-idle compaction failed", { sessionID: "ses_broken", e: "SDK failure" }],
  ]);
  expect(requests).toEqual([{ sessionID: "ses_healthy", at: 3_240_000 }]);
}));

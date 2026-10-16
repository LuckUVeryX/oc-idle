import { Plugin } from "@opencode/plugin/effect";
import type { SessionID } from "@opencode/schema/session-id";
import { Duration, Effect, FiberMap, HashMap, HashSet, Option, Ref, Schema, Stream } from "effect";

const Options = Schema.Struct({
  idleMinutes: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(54)),
  ),
  minTokens: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(200_000)),
  ),
});

interface Options extends Schema.Schema.Type<typeof Options> {}

export default Plugin.define({
  id: "oc-idle",
  effect: (ctx) =>
    Effect.gen(function* () {
      const rawOptions = ctx.options;
      const options: Options = yield* Schema.decodeEffect(Options)(rawOptions).pipe(Effect.orDie);

      const sessionTokens = yield* Ref.make(HashMap.empty<SessionID, number>());
      const timers = yield* FiberMap.make();
      const compactingSessions = yield* Ref.make(HashSet.empty<SessionID>());

      const schedule = Effect.fn("oc-idle.schedule")(function* (sessionID: SessionID) {
        yield* FiberMap.remove(timers, sessionID);
        yield* FiberMap.run(
          timers,
          sessionID,
          Effect.gen(function* () {
            const tokens = HashMap.get(yield* Ref.get(sessionTokens), sessionID);
            if (Option.isNone(tokens) || tokens.value <= options.minTokens) return;
            if (HashSet.has(yield* Ref.get(compactingSessions), sessionID)) return;

            const session = yield* ctx.session.get({ sessionID });
            if (session.parentID) return;

            yield* Ref.update(sessionTokens, HashMap.remove(sessionID));
            yield* ctx.session.compact({ sessionID });
          }).pipe(
            Effect.delay(Duration.minutes(options.idleMinutes)),
            Effect.catch((e) => Effect.logError("oc-idle compaction failed", { sessionID, e })),
          ),
        );
      });

      yield* ctx.event.subscribe().pipe(
        Stream.filter(
          (event) =>
            event.type === "session.step.ended" ||
            event.type === "session.execution.succeeded" ||
            event.type === "session.execution.failed" ||
            event.type === "session.execution.interrupted" ||
            event.type === "session.inbox.enqueued" ||
            event.type === "session.execution.started" ||
            event.type === "session.compaction.started" ||
            event.type === "session.compaction.ended" ||
            event.type === "session.compaction.failed" ||
            event.type === "session.deleted",
        ),
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            const sessionID = event.data.sessionID;

            switch (event.type) {
              case "session.step.ended":
                const sessions = yield* Ref.get(compactingSessions);
                if (HashSet.has(sessions, sessionID)) {
                  break;
                }
                const tokens = event.data.tokens;

                yield* Ref.update(
                  sessionTokens,
                  HashMap.set(
                    sessionID,
                    tokens.input + tokens.output + tokens.cache.read + tokens.cache.write,
                  ),
                );
                break;

              case "session.execution.succeeded":
              case "session.execution.failed":
              case "session.execution.interrupted":
                yield* schedule(sessionID);
                break;

              case "session.inbox.enqueued":
              case "session.execution.started":
                yield* FiberMap.remove(timers, sessionID);
                break;

              case "session.compaction.started":
                yield* FiberMap.remove(timers, sessionID);
                yield* Ref.update(compactingSessions, HashSet.add(sessionID));
                break;

              case "session.compaction.ended":
              case "session.compaction.failed":
              case "session.deleted":
                yield* Ref.update(sessionTokens, HashMap.remove(sessionID));
                yield* Ref.update(compactingSessions, HashSet.remove(sessionID));
                yield* FiberMap.remove(timers, sessionID);
                break;
            }

            yield* Effect.void;
          }),
        ),
        Effect.catch((e) => Effect.logError("oc-idle event listener stopped", { e })),
        Effect.forkScoped(),
      );
    }),
});

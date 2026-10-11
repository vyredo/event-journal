// Versioning one event while the rest stay put: a retired version keeps its
// frozen schema and an upcast, the current version names it in `previous`, and
// the ladder registers itself. A throwaway `test.ladder.ended` goes v1 -> v2 ->
// v3, so no real schema changes to prove it.
import { describe, expect, it } from "vitest";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { z } from "zod";

import * as Codec from "../codec";
import { EventCatalogue } from "./catalogue";
import * as Envelope from "./envelope";
import type { JournalRow } from "./envelope";
import * as EventConfig from "./event_config";

const table = sqliteTable("test_ladder", { id: text("id").primaryKey() });
type LadderModule = EventConfig.ModuleTypes<null, typeof table>;
const STREAM = "test.ladder";

// v1: the shape first written. Retired twice over.
const EndedV1 = EventConfig.retiredEvent({
  type: "test.ladder.ended",
  v: 1,
  schema: z.object({ reason: z.enum(["stop", "watchdog"]) }),
  upcast: ({ reason }) => ({ reason, source: reason === "watchdog" ? "app" : "game" }),
});

// v2: added `source`. Retired by v3.
const EndedV2 = EventConfig.retiredEvent({
  type: "test.ladder.ended",
  v: 2,
  schema: z.object({ reason: z.enum(["stop", "watchdog"]), source: z.string() }),
  previous: EndedV1,
  upcast: ({ reason, source }) => ({ reason, source, note: null }),
});

// v3: current.
const Ended = EventConfig.defineEvent<LadderModule>()({
  type: "test.ladder.ended",
  v: 3,
  streamType: STREAM,
  schema: z.object({ reason: z.enum(["stop", "watchdog"]), source: z.string(), note: z.string().nullable() }),
  previous: EndedV2,
});

// Another event of the same module, still at v1: nothing about it changes.
const Started = EventConfig.defineEvent<LadderModule>()({
  type: "test.ladder.started",
  v: 1,
  streamType: STREAM,
  schema: z.object({}),
});

const catalogue = new EventCatalogue();
catalogue.register([Ended.registration, Started.registration]);

/** A row as an older build stored it. */
function stored(type: string, v: number, payload: Record<string, unknown>): JournalRow {
  return {
    seq: 1,
    eventId: `evt-${type}-${v}`,
    streamType: STREAM,
    streamId: "s-1",
    type,
    v,
    ev: Envelope.ENVELOPE_VERSION,
    at: "2026-10-08T10:00:00.000Z",
    recordedAt: "2026-10-08T10:00:00.000Z",
    origin: "app",
    payload,
  };
}

describe("a retired event version", () => {
  it("registers its ladder and its frozen schema through the current version's `previous`", () => {
    expect(Object.keys(Ended.registration.upcasters ?? {}).sort()).toEqual(["1", "2"]);
    expect(Ended.registration.retired?.map((r) => r.v)).toEqual([2, 1]);
    expect(Started.registration.upcasters).toBeUndefined();
  });

  it("reads a stored v1 as the current v3, every step applied", () => {
    const decoded = Codec.decodeRow(stored("test.ladder.ended", 1, { reason: "watchdog" }), catalogue);
    expect(decoded.isOk && decoded.value).toMatchObject({
      v: 3,
      payload: { reason: "watchdog", source: "app", note: null },
    });
  });

  it("reads a stored v2 from where it is", () => {
    const decoded = Codec.decodeRow(stored("test.ladder.ended", 2, { reason: "stop", source: "operator" }), catalogue);
    expect(decoded.isOk && decoded.value.payload).toEqual({ reason: "stop", source: "operator", note: null });
  });

  it("refuses, loudly, a stored v1 that never fit v1", () => {
    const decoded = Codec.decodeRow(stored("test.ladder.ended", 1, { reason: "exploded" }), catalogue);
    expect(decoded.isOk).toBe(false);
  });

  it("refuses an append of a retired version: writers build the current one", () => {
    const appended = Codec.encodeEvent(
      { streamType: STREAM, streamId: "s-1", type: "test.ladder.ended", v: 1, payload: { reason: "stop" }, origin: "app" },
      { now: "2026-10-08T10:00:00.000Z", eventId: "evt-new" }, catalogue
    );
    expect(appended.isOk).toBe(false);
  });

  it("leaves the module's other events at their own version", () => {
    const decoded = Codec.decodeRow(stored("test.ladder.started", 1, {}), catalogue);
    expect(decoded.isOk && decoded.value.v).toBe(1);
  });

  it("makes a wrong upcast a compile error where the next version names it", () => {
    const Wrong = EventConfig.retiredEvent({
      type: "test.ladder.ended",
      v: 1,
      schema: z.object({ reason: z.string() }),
      // Produces no `source`, which v2 requires.
      upcast: ({ reason }) => ({ reason }),
    });
    EventConfig.retiredEvent({
      type: "test.ladder.ended",
      v: 2,
      schema: z.object({ reason: z.string(), source: z.string() }),
      // @ts-expect-error the v1 upcast does not produce a v2 payload
      previous: Wrong,
      upcast: (p) => p,
    });
    expect(Wrong.v).toBe(1);
  });
});

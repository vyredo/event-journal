// Renaming an event type: a stored event is immutable, type string included, so
// the log keeps the old name and the codec reads it as the new one. A throwaway
// `test.part.opened` (formerly `test.chunk.opened`) proves it, with no real
// event renamed to do so.
import { describe, expect, it } from "vitest";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { z } from "zod";

import * as Codec from "../codec";
import * as Redact from "../redact";
import { EventCatalogue } from "./catalogue";
import * as Envelope from "./envelope";
import type { JournalRow } from "./envelope";
import * as EventConfig from "./event_config";

const table = sqliteTable("test_parts", { id: text("id").primaryKey() });
type PartModule = EventConfig.ModuleTypes<{ opened: number }, typeof table>;
const STREAM = "test.part";

const Opened = EventConfig.defineEvent<PartModule>()({
  type: "test.part.opened",
  formerType: "test.chunk.opened",
  v: 1,
  streamType: STREAM,
  schema: z.object({ name: z.string() }),
  personal: ["name"],
  evolve: (state) => ({ opened: state.opened + 1 }),
});

const parts = EventConfig.defineEventModule({
  name: "test_parts",
  version: 1,
  table,
  projection: { shape: "row", key: "id" },
  initialState: () => ({ opened: 0 }),
  events: [Opened],
});

const catalogue = new EventCatalogue();
catalogue.register(parts.registration);

/** A row as the build before the rename stored it. */
function stored(type: string, seq: number): JournalRow {
  return {
    seq,
    eventId: `evt-${seq}`,
    streamType: STREAM,
    streamId: "s-1",
    type,
    v: 1,
    ev: Envelope.ENVELOPE_VERSION,
    at: "2026-10-08T10:00:00.000Z",
    recordedAt: "2026-10-08T10:00:00.000Z",
    origin: "app",
    payload: { name: "Ada" },
  };
}

describe("a renamed event type", () => {
  it("reads a row stored under the former name as the current type", () => {
    const decoded = Codec.decodeRow(stored("test.chunk.opened", 1), catalogue);
    expect(decoded.isOk && decoded.value).toMatchObject({ type: "test.part.opened", v: 1, payload: { name: "Ada" } });
  });

  it("folds old and new rows as one type", () => {
    const { events } = Codec.decodeRows([stored("test.chunk.opened", 1), stored("test.part.opened", 2)], catalogue);
    const state = events.reduce((s, e) => parts.evolve(s, e), parts.initialState());
    expect(state.opened).toBe(2);
  });

  it("selects the former name too, so the store finds rows written before the rename", () => {
    expect(parts.eventTypes).toEqual(["test.part.opened", "test.chunk.opened"]);
    expect(parts.liveTypes).toEqual(["test.part.opened"]);
  });

  it("refuses an append under the former name: only the current one is written", () => {
    const appended = Codec.encodeEvent(
      { streamType: STREAM, streamId: "s-1", type: "test.chunk.opened", v: 1, payload: { name: "Ada" }, origin: "app" },
      { now: "2026-10-08T10:00:00.000Z", eventId: "evt-new" },
      catalogue,
    );
    expect(appended.isOk).toBe(false);
  });

  it("redacts an old row by the current type's personal fields", () => {
    const [redacted] = Redact.redactRows([stored("test.chunk.opened", 1)], catalogue);
    expect(redacted?.payload.name).not.toBe("Ada");
  });

  it("refuses a former name that another type already uses", () => {
    const other = new EventCatalogue();
    other.register(parts.registration);
    const Clash = EventConfig.defineEvent<PartModule>()({
      type: "test.part.closed",
      formerType: "test.chunk.opened",
      v: 1,
      streamType: STREAM,
      schema: z.object({}),
    });
    expect(() => other.register([Clash.registration])).toThrow(/former name/);
  });
});

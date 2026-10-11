// Streams on the real migrations, with two synthetic stream types: each stream
// rebuilds alone, an event is refused on a stream its type does not belong to,
// each stream keeps one meta row, and the boot sweep re-folds exactly the
// streams whose slice version moved or that have no meta.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

vi.mock("better-sqlite3", () => ({ default: class { } }));

import * as TestDb from "./test_db";
import * as Envelope from "../shared/event/envelope";

import * as Schema from "./table";
import * as EventJournal from "./event_journal";

/** A journal of this suite's own, on each test's fresh database: nothing registered on the app's. */
const journal = EventJournal.createJournal({ db: TestDb.liveTestDb });

const Left = Envelope.defineStream("test.left");
const Right = Envelope.defineStream("test.right");

journal.catalogue.register([
  { type: "test.left.ticked", v: 1, streamType: Left.type, schema: z.object({}), fixture: {} },
  { type: "test.right.ticked", v: 1, streamType: Right.type, schema: z.object({}), fixture: {} },
]);

/** Which slice rebuilt which stream, over how many events. */
const rebuilds: Array<{ slice: string; streamId: string; events: number }> = [];
for (const [slice, stream] of [["leftSlice", Left], ["rightSlice", Right]] as const) {
  journal.projections.registerProjectionSlice(slice, {
    streamTypes: [stream.type],
    version: 2,
    rebuild: (streamId, events) => rebuilds.push({ slice, streamId, events: events.length }),
  });
}

const tick = (ref: Envelope.StreamRef, type: string) => {
  const appended = journal.append({ ...ref, type, v: 1, payload: {}, origin: "test", at: "2026-10-08T10:00:00.000Z" });
  expect(appended.isOk, type).toBe(true);
};

beforeEach(() => {
  TestDb.setTestCoreDb(TestDb.makeTestCoreDb());
  rebuilds.length = 0;
});
afterEach(() => TestDb.clearTestCoreDb());

describe("a stream rebuilds alone", () => {
  it("runs only the slices that fold its type, over only its own events", () => {
    tick(Left.of("a"), "test.left.ticked");
    tick(Left.of("a"), "test.left.ticked");
    tick(Right.of("a"), "test.right.ticked");
    journal.rebuildStream(Left.of("a"));
    expect(rebuilds).toEqual([{ slice: "leftSlice", streamId: "a", events: 2 }]);
  });

  it("refuses an event on a stream its type does not belong to", () => {
    const refused = journal.append({ ...Right.of("a"), type: "test.left.ticked", v: 1, payload: {}, origin: "test" });
    expect(refused.isOk).toBe(false);
  });

  it("keeps one meta row per stream", () => {
    tick(Left.of("a"), "test.left.ticked");
    journal.rebuildStream(Left.of("a"));
    journal.rebuildStream(Left.of("a"));
    expect(TestDb.liveTestDb.select().from(Schema.journalMeta).all()).toHaveLength(1);
  });
});

describe("the boot sweep, per stream", () => {
  it("re-folds a stream whose slice version moved, and leaves current ones alone", async () => {
    tick(Left.of("old"), "test.left.ticked");
    tick(Left.of("current"), "test.left.ticked");
    journal.store.upsertJournalMeta(Left.of("old"), { projectionVersions: JSON.stringify({ leftSlice: 1 }) });
    journal.store.upsertJournalMeta(Left.of("current"), { projectionVersions: JSON.stringify({ leftSlice: 2 }) });

    expect(await journal.rebuildStaleStreams()).toBe(1);
    expect(rebuilds).toEqual([{ slice: "leftSlice", streamId: "old", events: 1 }]);
    // A second boot finds nothing stale.
    rebuilds.length = 0;
    expect(await journal.rebuildStaleStreams()).toBe(0);
  });

  it("rebuilds a stream with no meta at all", async () => {
    tick(Right.of("fresh"), "test.right.ticked");
    expect(await journal.rebuildStaleStreams()).toBe(1);
    expect(rebuilds).toEqual([{ slice: "rightSlice", streamId: "fresh", events: 1 }]);
  });

  it("names only the slices that fold the stream's type as stale", () => {
    const meta = { projectionVersions: JSON.stringify({ leftSlice: 1, rightSlice: 1 }) };
    expect(journal.projections.staleDomains(meta, Left.type)).toEqual(["leftSlice"]);
  });
});

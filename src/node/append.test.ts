// The append primitive on the real migrations: a fact outlives its fold. A live
// rule that throws costs only its own savepoint and marks its domain stale; the
// stream then only appends until a rebuild repairs it.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

vi.mock("better-sqlite3", () => ({ default: class { } }));

import * as TestDb from "./test_db";
import * as Codec from "../shared/codec";
import type { NewEvent } from "../shared/event/envelope";
import { sql } from "drizzle-orm/sql";

import type * as Projection from "./projection";
import * as Schema from "./table";
import * as EventJournal from "./event_journal";

/** A journal of this suite's own, on each test's fresh database: nothing registered on the app's. */
const journal = EventJournal.createJournal({ db: TestDb.liveTestDb });

/** A stream type only this file folds. */
const STREAM = "test.append";
const DOMAIN = "testAppend";
const S1 = { streamType: STREAM, streamId: "s-1" };

/** How the live rule behaves: write its row, then throw when told to. */
const rule = { throwAfterWrite: false, calls: 0 };

journal.catalogue.register([
  {
    type: "test.append.happened",
    v: 1,
    streamType: STREAM,
    schema: z.object({ n: z.number().int() }),
    fixture: { n: 1 },
  },
  {
    type: "test.append.noted",
    v: 1,
    streamType: STREAM,
    schema: z.object({ note: z.string() }),
    fixture: { note: "x" },
  },
]);

function insertFolded(streamId: string, n: number) {
  TestDb.liveTestDb.run(sql`INSERT INTO test_append_rows (streamId, n) VALUES (${streamId}, ${n})`);
}

journal.projections.registerProjectionSlice(DOMAIN, {
  streamTypes: [STREAM],
  version: 3,
  rebuild: (streamId: string, events: readonly Codec.JournalEvent[]) => {
    TestDb.liveTestDb.run(sql`DELETE FROM test_append_rows WHERE streamId = ${streamId}`);
    for (const e of events) {
      if (e.type === "test.append.happened" && typeof e.payload.n === "number") insertFolded(streamId, e.payload.n);
    }
  },
  live: {
    types: ["test.append.happened"],
    apply: (event: Codec.JournalEvent) => {
      rule.calls++;
      insertFolded(event.streamId, event.payload.n as number);
      // edge: the test's stand-in for a fold bug that throws after a partial write.
      if (rule.throwAfterWrite) throw new Error("fold bug");
    },
  },
});

beforeEach(() => {
  TestDb.setTestCoreDb(TestDb.makeTestCoreDb());
  TestDb.liveTestDb.run(sql`CREATE TABLE test_append_rows (streamId TEXT NOT NULL, n INTEGER NOT NULL)`);
  rule.throwAfterWrite = false;
  rule.calls = 0;
});
afterEach(() => TestDb.clearTestCoreDb());

function happened(n: number, eventId?: string): NewEvent {
  return {
    ...S1,
    ...(eventId ? { eventId } : {}),
    type: "test.append.happened",
    v: 1,
    payload: { n },
    origin: "device",
    at: `2026-10-07T10:00:${String(n).padStart(2, "0")}.000Z`,
  };
}

/** Append and fail the test if the append itself was refused. */
function appendOk(event: NewEvent): Projection.Appended {
  const appended = journal.append(event);
  if (!appended.isOk) throw new Error(`append refused: ${appended.error.message}`);
  return appended.value;
}

const journalRows = () => TestDb.liveTestDb.select().from(Schema.journal).all();
const projected = () =>
  TestDb.liveTestDb.all<{ n: number }>(sql`SELECT n FROM test_append_rows WHERE streamId = ${S1.streamId} ORDER BY n`).map((r) => r.n);
const meta = () => journal.store.getJournalMeta(S1);

describe("a live fold that throws", () => {
  it("keeps the journal row and undoes the fold's half-write", () => {
    rule.throwAfterWrite = true;
    const appended = appendOk(happened(1));
    expect(appended.projection).toBe("stale");
    expect(journalRows()).toHaveLength(1);
    // The rule wrote its row before throwing; the savepoint took it back.
    expect(projected()).toEqual([]);
  });

  it("marks the domain stale in an existing version map, keeping the other entries", () => {
    journal.store.upsertJournalMeta(S1, { projectionVersions: JSON.stringify({ [DOMAIN]: 3, other: 7 }) });
    rule.throwAfterWrite = true;
    appendOk(happened(1));
    expect(JSON.parse(meta()!.projectionVersions!)).toEqual({ [DOMAIN]: 0, other: 7 });
    expect(journal.projections.staleDomains(meta(), STREAM)).toContain(DOMAIN);
  });

  it("marks a stream that had no meta yet, so the mark is visible to the next append", () => {
    rule.throwAfterWrite = true;
    appendOk(happened(1));
    expect(journal.projections.isMarkedStale(S1, DOMAIN)).toBe(true);
    expect(journal.projections.staleDomains(meta(), STREAM)).toContain(DOMAIN);
  });

  it("stops live folding on that stream: the next event only appends", () => {
    rule.throwAfterWrite = true;
    appendOk(happened(1));
    rule.throwAfterWrite = false;
    const callsBefore = rule.calls;
    const second = appendOk(happened(2));
    expect(second.projection).toBe("stale");
    expect(rule.calls).toBe(callsBefore);
    expect(journalRows()).toHaveLength(2);
    expect(projected()).toEqual([]);
  });

  it("is repaired by a rebuild, which clears the mark so live folding resumes", () => {
    rule.throwAfterWrite = true;
    appendOk(happened(1));
    rule.throwAfterWrite = false;
    appendOk(happened(2));

    expect(journal.rebuildStream(S1)).toBe(true);
    // The rebuild folds both stored facts, including the one whose live fold failed.
    expect(projected()).toEqual([1, 2]);
    expect(journal.projections.isMarkedStale(S1, DOMAIN)).toBe(false);
    expect(journal.projections.staleDomains(meta(), STREAM)).toEqual([]);

    const third = appendOk(happened(3));
    expect(third.projection).toBe("applied");
    expect(projected()).toEqual([1, 2, 3]);
  });
});

describe("the append primitive", () => {
  it("folds a healthy event live", () => {
    const appended = appendOk(happened(1));
    expect(appended.projection).toBe("applied");
    expect(appended.row.seq).toBeGreaterThan(0);
    expect(projected()).toEqual([1]);
  });

  it("stores a duplicate eventId once and folds it once", () => {
    const first = appendOk(happened(1, "evt-1"));
    const second = appendOk(happened(1, "evt-1"));
    expect(journalRows()).toHaveLength(1);
    expect(rule.calls).toBe(1);
    expect(second.row.seq).toBe(first.row.seq);
    expect(second.projection).toBe("applied");
    expect(projected()).toEqual([1]);
  });

  it("refuses an eventId already stored on another stream, instead of answering with that row", () => {
    appendOk(happened(1, "evt-1"));
    const elsewhere = journal.append({ ...happened(2, "evt-1"), streamId: "s-2" });
    expect(elsewhere.isOk).toBe(false);
    expect(journalRows()).toHaveLength(1);
  });

  it("reports a retried append on a stale stream as stale", () => {
    rule.throwAfterWrite = true;
    appendOk(happened(1, "evt-1"));
    expect(appendOk(happened(1, "evt-1")).projection).toBe("stale");
  });

  it("refuses a payload that fails its schema and stores nothing", () => {
    const refused = journal.append({ ...happened(1), payload: { n: "one" } });
    expect(refused.isOk).toBe(false);
    expect(journalRows()).toHaveLength(0);
    expect(meta()).toBeNull();
  });

  it("reports `none` for a type with no live rule", () => {
    const appended = appendOk({ ...S1, type: "test.append.noted", v: 1, payload: { note: "hi" }, origin: "app" });
    expect(appended.projection).toBe("none");
    expect(journalRows()).toHaveLength(1);
  });

  it("joins the caller's transaction: a caller that rolls back takes the row with it", () => {
    const rolledBack = (() => {
      try {
        TestDb.liveTestDb.transaction(() => {
          const appended = journal.appendInTransaction(happened(1));
          expect(appended.isOk).toBe(true);
          // edge: the caller's own failure after the append, which must undo it.
          throw new Error("caller failed after appending");
        });
        return false;
      } catch {
        return true;
      }
    })();
    expect(rolledBack).toBe(true);
    expect(journalRows()).toHaveLength(0);
    expect(projected()).toEqual([]);
  });
});

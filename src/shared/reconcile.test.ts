// `reconcile` over synthetic writers, one per table shape: it recomputes what a
// module's writer would have written from the log and reports each column the
// table disagrees on. No real module, so the rules are the journal's alone.
import { describe, expect, it } from "vitest";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";

import type { JournalEvent } from "./codec";
import * as Decider from "./decider";
import * as Envelope from "./event/envelope";
import type * as EventConfig from "./event/event_config";
import * as Reconcile from "./reconcile";

const table = sqliteTable("test_reconcile", { id: text("id").primaryKey() });

function event(seq: number, payload: Record<string, unknown>): JournalEvent {
  return {
    seq,
    eventId: `evt-${seq}`,
    streamType: "test.reconcile",
    streamId: "s",
    type: "test.reconcile.set",
    v: 1,
    ev: Envelope.ENVELOPE_VERSION,
    at: "2026-10-08T10:00:00.000Z",
    recordedAt: "2026-10-08T10:00:00.000Z",
    origin: "app",
    payload,
  };
}

describe("reconcile, one row per stream", () => {
  const write: EventConfig.TableWrite = {
    shape: "row",
    table,
    key: "id",
    toRow: (events) => (events.length === 0 ? null : Object.assign({}, ...events.map((e) => e.payload))),
  };

  it("agrees when every written column matches, ignoring columns the log never writes", () => {
    const events = [event(1, { name: "a", tags: { x: 1 } }), event(2, { name: "b" })];
    expect(Reconcile.reconcile("row", write, events, [{ id: "s", name: "b", tags: { x: 1 }, owned: 7 }])).toEqual([]);
  });

  it("reports a column that differs, and a row that is missing", () => {
    const events = [event(1, { name: "a" })];
    expect(Reconcile.reconcile("row", write, events, [{ name: "z" }])).toEqual(['row: name row="z" journal="a"']);
    expect(Reconcile.reconcile("row", write, events, [])).toEqual(["row: no row, but the journal writes one"]);
  });

  it("says nothing about a stream the log never wrote a row for", () => {
    expect(Reconcile.reconcile("row", write, [], [{ name: "pre-journal" }])).toEqual([]);
  });
});

describe("reconcile, keyed rows within a stream", () => {
  const write: EventConfig.TableWrite = {
    shape: "keyed",
    table,
    scope: "streamId",
    key: "file",
    toRows: (events) => {
      const rows = new Map<string, Record<string, unknown> | null>();
      for (const e of events) {
        const { file, dropped, ...columns } = e.payload;
        rows.set(String(file), dropped ? null : { file, ...columns });
      }
      return rows;
    },
  };

  it("compares the keys the log names and leaves the others alone", () => {
    const events = [event(1, { file: "a", size: 1 }), event(2, { file: "b", dropped: true })];
    expect(Reconcile.reconcile("keyed", write, events, [{ file: "a", size: 1 }, { file: "pre", size: 9 }])).toEqual([]);
  });

  it("reports a dropped key that still has a row, and a key with the wrong columns", () => {
    const events = [event(1, { file: "a", size: 1 }), event(2, { file: "b", dropped: true })];
    expect(Reconcile.reconcile("keyed", write, events, [{ file: "a", size: 2 }, { file: "b", size: 1 }])).toEqual([
      "keyed a: size row=2 journal=1",
      "keyed b: row present, but the journal dropped it",
    ]);
  });
});

describe("reconcile, appended rows", () => {
  const write: EventConfig.TableWrite = {
    shape: "append",
    table,
    scope: "streamId",
    supersede: ["at"],
    rowsOf: (e) => [{ at: e.payload.at, score: e.payload.score, srcSeq: e.seq }],
  };

  it("replays supersession: a later row for the same columns replaces the earlier one", () => {
    const events = [event(1, { at: "t1", score: 1 }), event(2, { at: "t1", score: 2 })];
    expect(Reconcile.reconcile("append", write, events, [{ at: "t1", score: 2, srcSeq: 2 }])).toEqual([]);
  });

  it("reports a count mismatch and the row that kept the superseded value", () => {
    const events = [event(1, { at: "t1", score: 1 }), event(2, { at: "t1", score: 2 })];
    expect(
      Reconcile.reconcile("append", write, events, [
        { at: "t1", score: 1, srcSeq: 1 },
        { at: "t9", score: 5, srcSeq: 3 },
      ]),
    ).toEqual([
      "append: 2 rows, the journal writes 1",
      'append ["t1"]: score row=1 journal=2',
      'append ["t1"]: srcSeq row=1 journal=2',
    ]);
  });
});

describe("foldAll", () => {
  it("folds one log through every evolver, each from its own initial state", () => {
    const count: Decider.Evolver<number> = { initialState: () => 0, evolve: (n) => n + 1 };
    const names: Decider.Evolver<readonly string[]> = {
      initialState: () => [],
      evolve: (s, e) => [...s, String(e.payload.name)],
    };
    const states = Decider.foldAll({ count, names }, [event(1, { name: "a" }), event(2, { name: "b" })]);
    expect(states).toEqual({ count: 2, names: ["a", "b"] });
  });
});

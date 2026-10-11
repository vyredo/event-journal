// `handle` on the real migrations, with a synthetic counter: it decides on the
// state folded from the journal, appends what was decided, stores nothing for a
// no-op or a refusal, and stores none of a decision when one of its events is
// refused.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Fail, Ok } from "@vyredo/typed-result";
import type { ReturnResult } from "@vyredo/typed-result";
import { z } from "zod";

vi.mock("better-sqlite3", () => ({ default: class { } }));

import * as TestDb from "./test_db";
import type { Decided, Decider } from "../shared/decider";
import * as Envelope from "../shared/event/envelope";
import type { NewEvent } from "../shared/event/envelope";
import * as EventJournal from "./event_journal";

/** A journal of this suite's own, on each test's fresh database: nothing registered on the app's. */
const journal = EventJournal.createJournal({ db: TestDb.liveTestDb });

const Counter = Envelope.defineStream("test.counter");
const STREAM = Counter.of("c-1");

journal.catalogue.register([
  { type: "test.counter.added", v: 1, streamType: Counter.type, schema: z.object({ n: z.number().int() }), fixture: { n: 1 } },
]);

type Add = { type: "Add"; n: number; max: number; at: string };
const added = (n: number, at: string): Decided<NewEvent> => ({ type: "test.counter.added", v: 1, payload: { n }, origin: "test", at });

/** A counter that refuses to pass `max`, and states nothing for adding zero. */
const counter: Decider<Add, number, NewEvent> = {
  initialState: () => 0,
  evolve: (total, event) => (event.type === "test.counter.added" && typeof event.payload.n === "number" ? total + event.payload.n : total),
  eventTypes: ["test.counter.added"],
  decide: ({ n, max, at }, total): ReturnResult<Decided<NewEvent>[]> => {
    if (total + n > max) return Fail(`${total} + ${n} passes ${max}`);
    const events: Decided<NewEvent>[] = n === 0 ? [] : [added(n, at)];
    return Ok(events);
  },
};

const add = (n: number, max = 10, at = "2026-10-08T10:00:00.000Z") => journal.handle(STREAM, counter, { type: "Add", n, max, at });
const types = () => journal.store.streamRowsSync(STREAM).map((r) => r.type);

beforeEach(() => TestDb.setTestCoreDb(TestDb.makeTestCoreDb()));
afterEach(() => TestDb.clearTestCoreDb());

describe("handle", () => {
  it("appends what was decided, and decides the next command on the folded journal", () => {
    const first = add(4);
    expect(first.isOk && first.value.map((a) => a.row.payload)).toEqual([{ n: 4 }]);
    // 4 is in the journal now, so 7 more passes the max of 10.
    expect(add(7).isOk).toBe(false);
    expect(add(6).isOk).toBe(true);
    expect(types()).toEqual(["test.counter.added", "test.counter.added"]);
  });

  it("stores nothing for a no-op", () => {
    const noop = add(0);
    expect(noop.isOk && noop.value).toEqual([]);
    expect(types()).toEqual([]);
  });

  it("stores nothing for a refusal, and says why", () => {
    const refused = add(11);
    expect(refused.isOk).toBe(false);
    expect(!refused.isOk && refused.error.message).toMatch(/passes 10/);
    expect(types()).toEqual([]);
  });

  it("stores none of a decision when one of its events is refused", () => {
    const halfBad: Decider<null, null, NewEvent> = {
      initialState: () => null,
      evolve: (state) => state,
      eventTypes: [],
      decide: () => {
        const decision: Decided<NewEvent>[] = [
          added(1, "2026-10-08T10:00:00.000Z"),
          // Not an integer: the codec refuses it.
          { ...added(1, "2026-10-08T10:00:01.000Z"), payload: { n: "one" } },
        ];
        return Ok(decision);
      },
    };
    expect(journal.handle(STREAM, halfBad, null).isOk).toBe(false);
    expect(types()).toEqual([]);
  });
});

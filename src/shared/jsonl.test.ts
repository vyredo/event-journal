import { describe, expect, it } from "vitest";

import * as Determinism from "./determinism";
import type { Catalogue } from "./event/contract";
import * as Envelope from "./event/envelope";
import * as Jsonl from "./jsonl";
import * as Redact from "./redact";

const row = (over: Partial<Envelope.JournalRow> = {}): Envelope.JournalRow => ({
  seq: 1,
  eventId: "evt-1",
  streamType: "test.jsonl", streamId: "s-1",
  type: "test.thing.removed",
  v: 1,
  ev: Envelope.ENVELOPE_VERSION,
  at: "2026-08-20T09:14:02.117Z",
  recordedAt: "2026-08-20T09:14:02.117Z",
  origin: "device",
  payload: { id: "a", reason: "gone" },
  ...over,
});

describe("JSONL", () => {
  it("round-trips a log", () => {
    const rows = [
      row({ seq: 1 }),
      row({ seq: 2, eventId: "evt-2", type: "test.thing.stopped", payload: {} }),
    ];
    const { rows: back, errors } = Jsonl.fromJsonl(Jsonl.toJsonl(rows));
    expect(errors).toEqual([]);
    expect(back).toEqual(rows);
  });

  it("writes in seq order whatever order it was handed", () => {
    // Line order is the only ordering an importer on another machine can see.
    const text = Jsonl.toJsonl([row({ seq: 3 }), row({ seq: 1 }), row({ seq: 2 })]);
    const { rows } = Jsonl.fromJsonl(text);
    expect(rows.map((r) => r.seq)).toEqual([1, 2, 3]);
  });

  it("is empty for an empty log, with no stray newline", () => {
    expect(Jsonl.toJsonl([])).toBe("");
  });

  it("skips blank lines rather than calling them errors", () => {
    // A trailing newline is normal, and so is a file concatenated from two
    // exports.
    const text = Jsonl.toJsonl([row()]) + "\n\n";
    const { rows, errors } = Jsonl.fromJsonl(text);
    expect(rows).toHaveLength(1);
    expect(errors).toEqual([]);
  });

  it("survives a log truncated mid-write", () => {
    // The reason the format is JSONL: a power cut loses the last line and the
    // file stays readable, which no single-JSON-document format manages.
    const text = Jsonl.toJsonl([row({ seq: 1 }), row({ seq: 2 })]);
    const truncated = text.slice(0, text.length - 40);
    const { rows, errors } = Jsonl.fromJsonl(truncated);
    expect(rows.map((r) => r.seq)).toEqual([1]);
    expect(errors).toHaveLength(1);
  });

  it("reports the line number of a bad line", () => {
    const { errors } = Jsonl.fromJsonl('{"not":"an event"}\n');
    expect(errors[0]).toContain("line 1");
  });

  it("rejects a row whose payload is not an object", () => {
    const { errors } = Jsonl.fromJsonl(JSON.stringify({ ...row(), payload: "nope" }));
    expect(errors[0]).toContain("payload must be an object");
  });
});

describe("redaction", () => {
  // Only `test.owner` declares a personal field; the journal knows no field by name.
  const catalogue: Catalogue = {
    currentV: {},
    streamTypes: {},
    schemas: {},
    upcasters: {},
    personal: { "test.owner": ["name"] },
  };

  it("replaces a declared personal field with a stable pseudonym per stream", () => {
    const rows = Redact.redactRows(
      [
        row({ type: "test.owner", payload: { name: "Ada Lovelace" } }),
        row({ eventId: "evt-2", type: "test.owner", payload: { name: "Ada Lovelace" } }),
        row({ eventId: "evt-3", streamId: "s-2", type: "test.owner", payload: { name: "Ada Lovelace" } }),
      ],
      catalogue,
    );
    expect(rows[0].payload.name).toBe(Redact.pseudonym("test.jsonl/s-1/name"));
    // Same stream, same pseudonym  -  a redacted log still has to be debuggable.
    expect(rows[0].payload.name).toBe(rows[1].payload.name);
    expect(rows[0].payload.name).not.toBe("Ada Lovelace");
    expect(rows[2].payload.name).not.toBe(rows[0].payload.name);
  });

  it("gives different ids different pseudonyms", () => {
    expect(Redact.pseudonym("p-1")).not.toBe(Redact.pseudonym("p-2"));
  });

  it("leaves a type with no declared personal field untouched, envelope included", () => {
    const original = row({ payload: { name: "not personal here" } });
    const [redacted] = Redact.redactRows([original], catalogue);
    expect(redacted).toEqual(original);
  });
});

describe("determinism seams", () => {
  it("fixedClock returns the same instant every call", () => {
    const clock = Determinism.fixedClock("2026-08-20T09:00:00.000Z");
    expect(clock.now()).toBe(clock.now());
  });

  it("scriptedClock walks the list and then holds the last", () => {
    // Holding rather than throwing: a handler making one extra call must not
    // fail a replay, and the last event's time is the least wrong answer.
    const clock = Determinism.scriptedClock(["a", "b"]);
    expect([clock.now(), clock.now(), clock.now()]).toEqual(["a", "b", "b"]);
  });

  it("fnv1a is stable and fixed-width", () => {
    expect(Determinism.fnv1a("s-1")).toBe(Determinism.fnv1a("s-1"));
    expect(Determinism.fnv1a("s-1")).not.toBe(Determinism.fnv1a("s-2"));
    expect(Determinism.fnv1a("")).toHaveLength(8);
  });

  it("derivedIds is reproducible per seed and unique per call", () => {
    const a = Determinism.derivedIds("s-1:12");
    const b = Determinism.derivedIds("s-1:12");
    expect([a.uuid(), a.uuid()]).toEqual([b.uuid(), b.uuid()]);
    expect(a.uuid()).not.toBe(a.uuid());
  });

  it("mints filesystem-safe ids, because these reach filenames", () => {
    expect(Determinism.derivedIds("s-1:12").uuid()).toMatch(/^[a-z0-9-]+$/);
  });
});

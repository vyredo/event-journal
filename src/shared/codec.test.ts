import { describe, expect, it } from "vitest";

import { EventCatalogue } from "./event/catalogue";
import { z } from "zod";

import * as Codec from "./codec";
import * as Define from "./event/define";
import type { Catalogue, PayloadSchema } from "./event/contract";
import * as Envelope from "./event/envelope";
import * as Upcasters from "./event/upcasters";

// The codec owns no vocabulary, so these tests register their own: one type on
// its own stream, shaped like the real events it stands in for. Core's frozen
// fixtures are exercised against core's registration in
// the application's own module tests.
const TEST_STREAM = "test.files";
const FileDiscarded = Define.defineJournalEvent({
  type: "test.file.discarded",
  v: 1,
  streamType: TEST_STREAM,
  schema: z.object({ fileName: z.string().min(1), reason: z.string().min(1) }),
});
const catalogue = new EventCatalogue();
catalogue.register([
  { ...FileDiscarded, fixture: { fileName: "a.mp4", reason: "no bytes" } },
]);

const NOW = "2026-08-20T09:14:02.117Z";
/** The append path's stamp: the clock, and the id used when a caller passes none. */
const STAMP = { now: NOW, eventId: "evt-generated" };

const stored = (over: Partial<Envelope.JournalRow>): Envelope.JournalRow => ({
  seq: 1,
  eventId: "evt-1",
  streamType: TEST_STREAM,
  streamId: "hvs-1",
  type: "test.file.discarded",
  v: 1,
  ev: Envelope.ENVELOPE_VERSION,
  at: NOW,
  recordedAt: NOW,
  origin: "device",
  payload: { fileName: "a.mp4", reason: "no bytes" },
  ...over,
});

const newEvent = (over: Partial<Envelope.NewEvent> = {}): Envelope.NewEvent => ({
  streamType: TEST_STREAM,
  streamId: "hvs-1",
  type: "test.file.discarded",
  v: 1,
  origin: "device",
  payload: { fileName: "a.mp4", reason: "no bytes" },
  ...over,
});

describe("encodeEvent", () => {
  it("refuses a writer built at a version the catalogue does not write", () => {
    const encoded = Codec.encodeEvent(newEvent({ v: 2 }), STAMP, catalogue);
    expect(encoded.isOk).toBe(false);
    if (!encoded.isOk) expect(encoded.error.message).toContain("built at v2");
  });

  it("stamps the envelope and defaults `at` to the append clock", () => {
    const encoded = Codec.encodeEvent(newEvent(), STAMP, catalogue);
    expect(encoded.isOk).toBe(true);
    if (!encoded.isOk) return;
    expect(encoded.value).toMatchObject({
      eventId: "evt-generated",
      streamType: TEST_STREAM,
      streamId: "hvs-1",
      type: "test.file.discarded",
      v: 1,
      ev: Envelope.ENVELOPE_VERSION,
      at: NOW,
      recordedAt: NOW,
      origin: "device",
    });
  });

  it("keeps the caller's eventId, so a retried append can be recognised", () => {
    const encoded = Codec.encodeEvent(newEvent({ eventId: "evt-retry" }), STAMP, catalogue);
    expect(encoded.isOk && encoded.value.eventId).toBe("evt-retry");
  });

  it("keeps an explicit `at` while still recording the append time", () => {
    // The relayed-observation case: a master that started three seconds ago is
    // journaled now. Losing the real `startedAt` shifts every clip by the relay
    // delay, and the reel renders perfectly from the wrong moment.
    const encoded = Codec.encodeEvent(
      newEvent({ at: "2026-08-20T09:13:59.000Z" }),
      STAMP, catalogue
    );
    expect(encoded.isOk).toBe(true);
    if (!encoded.isOk) return;
    expect(encoded.value.at).toBe("2026-08-20T09:13:59.000Z");
    expect(encoded.value.recordedAt).toBe(NOW);
  });

  it("refuses an event that names no stream", () => {
    const encoded = Codec.encodeEvent(newEvent({ streamId: "" }), STAMP, catalogue);
    expect(encoded.isOk).toBe(false);
    if (!encoded.isOk) expect(encoded.error.message).toContain("names no stream");
  });

  it("refuses an event on a stream its type does not belong to", () => {
    // An event on another stream type would never be folded by the slices that read its own.
    const encoded = Codec.encodeEvent(newEvent({ streamType: "camden.booking" }), STAMP, catalogue);
    expect(encoded.isOk).toBe(false);
    if (!encoded.isOk) expect(encoded.error.message).toContain(`belongs on a "${TEST_STREAM}" stream`);
  });

  it("refuses an unknown event type", () => {
    const encoded = Codec.encodeEvent(newEvent({ type: "player.teleported" }), STAMP, catalogue);
    expect(encoded.isOk).toBe(false);
    if (!encoded.isOk) {
      expect(encoded.error.message).toContain("unknown event type");
    }
  });

  it("refuses a payload the schema rejects", () => {
    const encoded = Codec.encodeEvent(
      newEvent({ payload: { fileName: "a.mp4" } }),
      STAMP, catalogue
    );
    expect(encoded.isOk).toBe(false);
  });

  it("refuses a payload over the size cap", () => {
    const encoded = Codec.encodeEvent(
      newEvent({
        payload: { fileName: "big.mp4", reason: "x".repeat(Envelope.PAYLOAD_MAX_BYTES) },
      }),
      STAMP, catalogue
    );
    expect(encoded.isOk).toBe(false);
    if (!encoded.isOk) {
      expect(encoded.error.message).toContain("metadata, not bytes");
    }
  });

  it("refuses a payload that cannot be serialized", () => {
    const circular: Record<string, unknown> = { fileName: "a.mp4", reason: "loop" };
    circular.self = circular;
    const encoded = Codec.encodeEvent(
      newEvent({ payload: circular }),
      STAMP, catalogue
    );
    expect(encoded.isOk).toBe(false);
  });
});

describe("decodeRow", () => {
  it("round-trips an appended event", () => {
    const encoded = Codec.encodeEvent(newEvent(), STAMP, catalogue);
    expect(encoded.isOk).toBe(true);
    if (!encoded.isOk) return;
    const decoded = Codec.decodeRow({ ...encoded.value, seq: 7 }, catalogue);
    expect(decoded.isOk).toBe(true);
    if (!decoded.isOk) return;
    expect(decoded.value.seq).toBe(7);
    expect(decoded.value.payload.reason).toBe("no bytes");
  });

  it("refuses a row with no seq", () => {
    const decoded = Codec.decodeRow(stored({ seq: undefined }), catalogue);
    expect(decoded.isOk).toBe(false);
  });

  it("refuses an envelope version from the future", () => {
    // Hard failure, not best effort: a newer build wrote fields this one cannot
    // see, and guessing produces a plausible-looking wrong projection.
    const decoded = Codec.decodeRow(stored({ ev: Envelope.ENVELOPE_VERSION + 1 }), catalogue);
    expect(decoded.isOk).toBe(false);
    if (!decoded.isOk)
      expect(decoded.error.message).toContain("update the app");
  });

  it("refuses an unknown type", () => {
    const decoded = Codec.decodeRow(stored({ type: "player.teleported" }), catalogue);
    expect(decoded.isOk).toBe(false);
  });

  it("keeps going past a bad row and reports it", () => {
    const { events, errors } = Codec.decodeRows([
      stored({ seq: 1 }),
      stored({ seq: 2, type: "nope" }),
      stored({ seq: 3 }),
    ], catalogue);
    expect(events.map((e) => e.seq)).toEqual([1, 3]);
    expect(errors).toHaveLength(1);
  });
});

describe("inFoldOrder", () => {
  it("orders by occurrence time, not arrival order", () => {
    // The relay case: a master that started first can be appended second.
    const rows = [
      stored({ seq: 1, at: "2026-08-20T09:00:02.000Z" }),
      stored({ seq: 2, at: "2026-08-20T09:00:01.000Z" }),
    ];
    const { events } = Codec.decodeRows(rows, catalogue);
    expect(Codec.inFoldOrder(events).map((e) => e.seq)).toEqual([2, 1]);
  });

  it("breaks ties on `at` by arrival order", () => {
    // Two events can land in the same millisecond; the order only has to be
    // deterministic.
    const rows = [
      stored({ seq: 5, at: NOW }),
      stored({ seq: 3, at: NOW }),
      stored({ seq: 4, at: NOW }),
    ];
    const { events } = Codec.decodeRows(rows, catalogue);
    expect(Codec.inFoldOrder(events).map((e) => e.seq)).toEqual([3, 4, 5]);
  });
});

// ---------------------------------------------------------------------------
// The upcast ladder, exercised against a synthetic type
// ---------------------------------------------------------------------------
//
// Every production type is at v1 with no upcasters, so nothing above reaches the
// chaining code. A synthetic catalogue with a real v1->v2->v3 ladder is what keeps
// it working before the first real bump  -  which is exactly when it has to.

const schemaOf = (
  check: (v: Record<string, unknown>) => string | null,
): PayloadSchema => ({
  safeParse: (value) => {
    const err = check(value as Record<string, unknown>);
    return err
      ? { success: false, error: { message: err } }
      : { success: true, data: value };
  },
});

const probeCatalogue = (over: Partial<Catalogue> = {}): Catalogue => ({
  personal: {},
  currentV: { "test.probe": 3 },
  streamTypes: { "test.probe": "test" },
  schemas: {
    "test.probe": {
      1: schemaOf((p) =>
        typeof p.a === "number" ? null : "a must be a number",
      ),
      2: schemaOf((p) =>
        typeof p.a === "number" && typeof p.b === "string"
          ? null
          : "need a:number, b:string",
      ),
      3: schemaOf((p) =>
        typeof p.a === "number" &&
          typeof p.b === "string" &&
          typeof p.c === "boolean"
          ? null
          : "need a:number, b:string, c:boolean",
      ),
    },
  },
  upcasters: {
    "test.probe": {
      1: (p) => ({ ...p, b: "" }),
      2: (p) => ({ ...p, c: false }),
    },
  },
  ...over,
});

describe("the upcast ladder", () => {
  it("chains v1 to v3 and validates against the current schema", () => {
    const decoded = Codec.decodeRow(
      stored({ type: "test.probe", v: 1, payload: { a: 1 } }),
      probeCatalogue(),
    );
    expect(decoded.isOk).toBe(true);
    if (!decoded.isOk) return;
    expect(decoded.value.v).toBe(3);
    expect(decoded.value.payload).toEqual({ a: 1, b: "", c: false });
  });

  it("leaves a payload already at the current version alone", () => {
    const upcast = Upcasters.upcastPayload(
      "test.probe",
      3,
      { a: 1, b: "x", c: true },
      probeCatalogue(),
    );
    expect(upcast.isOk).toBe(true);
    if (upcast.isOk)
      expect(upcast.value.payload).toEqual({ a: 1, b: "x", c: true });
  });

  it("fails loudly on a gap in the ladder rather than half-upcasting", () => {
    // The one failure mode that silently corrupts an event-sourced system: a
    // half-upcast payload usually still carries the fields a reducer reads.
    const gapped = probeCatalogue({
      upcasters: { "test.probe": { 1: (p) => ({ ...p, b: "" }) } },
    });
    const upcast = Upcasters.upcastPayload("test.probe", 1, { a: 1 }, gapped);
    expect(upcast.isOk).toBe(false);
    if (!upcast.isOk) expect(upcast.error.message).toContain("gap");
  });

  it("refuses a payload version newer than this build knows", () => {
    const upcast = Upcasters.upcastPayload("test.probe", 4, { a: 1 }, probeCatalogue());
    expect(upcast.isOk).toBe(false);
    if (!upcast.isOk)
      expect(upcast.error.message).toContain("newer than this build");
  });

  it("refuses a non-positive stored version", () => {
    const upcast = Upcasters.upcastPayload("test.probe", 0, { a: 1 }, probeCatalogue());
    expect(upcast.isOk).toBe(false);
  });

  it("surfaces an upcaster that produces the wrong shape", () => {
    // A wrong upcaster and a missing one look the same from the reducer's side.
    // Both have to fail at the decode, not at the fold.
    const wrong = probeCatalogue({
      upcasters: {
        "test.probe": {
          1: (p) => ({ ...p, b: 0 }),
          2: (p) => ({ ...p, c: false }),
        },
      },
    });
    const decoded = Codec.decodeRow(
      stored({ type: "test.probe", v: 1, payload: { a: 1 } }),
      wrong,
    );
    expect(decoded.isOk).toBe(false);
    if (!decoded.isOk) {
      expect(decoded.error.message).toContain("after upcasting");
    }
  });
});

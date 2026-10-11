// Append-side and read-side validation for one journal entry: every payload is
// validated at append against the current version, and again at read after
// upcasting, which asserts the upcaster chain is complete.
import type { ReturnResult } from "@vyredo/typed-result";
import { Result, Fail, Ok } from "@vyredo/typed-result";

import type { Catalogue } from "./event/contract";
import * as Envelope from "./event/envelope";
import * as Upcasters from "./event/upcasters";

/**
 * A stored row, decoded: upcast to its type's current version and validated
 * against the schema its module registered. What every fold reads. The journal
 * knows no event's shape, so a fold narrows it to its own module's types with
 * that module's guard (`Define.guardFor`).
 */
export type JournalEvent = Omit<Envelope.JournalRow, "seq"> & { seq: number };

/** Serialized size of a payload, for the `PAYLOAD_MAX_BYTES` gate. */
function payloadBytes(payload: Record<string, unknown>): ReturnResult<number> {
  // A circular reference or throwing `toJSON` is an unstorable payload, not a size problem.
  return Result.wrap(
    () => new TextEncoder().encode(JSON.stringify(payload)).byteLength,
  ).mapErr((err) => new Error(`payload is not serializable: ${err.message}`));
}

/**
 * Validate a caller's event and build the row to store. Pure, so the append
 * path supplies both what it would otherwise read from the machine: `now`, the
 * append clock that `at` defaults to, and `eventId`, the id used when the
 * caller passed none.
 */
export function encodeEvent(
  input: Envelope.NewEvent,
  stamp: { now: string; eventId: string },
  catalogue: Catalogue,
): ReturnResult<Envelope.JournalRow> {
  if (!input.streamType || !input.streamId) {
    // An unrouted row is invisible to every fold, replay and retention, so a table starts leaking.
    return Fail(`${input.type} names no stream: streamType and streamId are required`);
  }
  const eventId = input.eventId ?? stamp.eventId;
  if (!eventId) return Fail(`${input.type} has no eventId`);
  const v = catalogue.currentV[input.type];
  if (v === undefined) {
    return Fail(`unknown event type "${input.type}"`);
  }
  if (input.v !== v) {
    return Fail(`${input.type} was built at v${input.v}, but this build writes v${v}`);
  }
  const streamType = catalogue.streamTypes[input.type];
  if (streamType !== input.streamType) {
    return Fail(
      `${input.type} belongs on a "${streamType}" stream, not "${input.streamType}"`,
    );
  }
  const schema = catalogue.schemas[input.type]?.[v];
  if (!schema) {
    return Fail(`no schema for ${input.type} v${v}`);
  }

  const parsed = schema.safeParse(input.payload);
  if (!parsed.success) {
    return Fail(
      `${input.type} v${v} payload rejected: ${parsed.error.message ?? "invalid"}`,
    );
  }

  const sized = payloadBytes(input.payload);
  if (!sized.isOk) return Fail(sized.error.message);
  if (sized.value > Envelope.PAYLOAD_MAX_BYTES) {
    return Fail(
      `${input.type} payload is ${sized.value} bytes, over the ${Envelope.PAYLOAD_MAX_BYTES} limit  -  ` +
      "the journal stores metadata, not bytes; reference the artifact or split the batch",
    );
  }

  return Ok({
    eventId,
    streamType: input.streamType,
    streamId: input.streamId,
    type: input.type,
    v,
    ev: Envelope.ENVELOPE_VERSION,
    at: input.at ?? stamp.now,
    recordedAt: stamp.now,
    origin: input.origin,
    // The validated value, not the raw input, so row and validation cannot disagree.
    payload: parsed.data as Record<string, unknown>,
    ...(input.via ? { via: input.via } : {}),
    ...(input.causedBy !== undefined ? { causedBy: input.causedBy } : {}),
  });
}

/** A payload is a JSON object; the guard the validated value passes before it is typed as one. */
function isPayloadObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Turn a stored row into a decoded event: envelope checked, upcast, validated. */
export function decodeRow(
  row: Envelope.JournalRow,
  catalogue: Catalogue,
): ReturnResult<JournalEvent> {
  if (row.seq === undefined) {
    return Fail("row has no seq  -  it was never stored");
  }
  if (typeof row.ev !== "number" || row.ev < 1) {
    return Fail(
      `row ${row.seq}: envelope version is missing or invalid`,
    );
  }
  if (row.ev > Envelope.ENVELOPE_VERSION) {
    // Hard failure, not best-effort: guessing produces a plausible-looking wrong projection.
    return Fail(
      `row ${row.seq}: envelope v${row.ev} is newer than this build's v${Envelope.ENVELOPE_VERSION}  -  update the app`,
    );
  }
  // A renamed type's old rows keep their stored name; they read as the current one.
  const type = catalogue.renamed?.[row.type] ?? row.type;
  if (catalogue.currentV[type] === undefined) {
    return Fail(`row ${row.seq}: unknown event type "${row.type}"`);
  }

  const upcast = Upcasters.upcastPayload(type, row.v, row.payload, catalogue);
  if (!upcast.isOk)
    return Fail(`row ${row.seq}: ${upcast.error.message}`);

  const schema = catalogue.schemas[type]?.[upcast.value.v];
  if (!schema) {
    return Fail(
      `row ${row.seq}: no schema for ${type} v${upcast.value.v}`,
    );
  }
  const parsed = schema.safeParse(upcast.value.payload);
  if (!parsed.success) {
    // The ladder ran and the result still does not fit: an upcaster here is wrong or missing.
    return Fail(
      `row ${row.seq}: ${type} did not match v${upcast.value.v} after upcasting: ${parsed.error.message ?? "invalid"}`,
    );
  }

  const payload = parsed.data;
  if (!isPayloadObject(payload)) {
    return Fail(`row ${row.seq}: ${type} schema produced a non-object payload`);
  }
  return Ok({ ...row, type, seq: row.seq, v: upcast.value.v, payload });
}

/**
 * Decode a whole stream's log, keeping going past a bad row; `errors` is returned
 * so the caller decides whether a partial decode is usable.
 */
export function decodeRows(
  rows: Envelope.JournalRow[],
  catalogue: Catalogue,
): { events: JournalEvent[]; errors: string[] } {
  const events: JournalEvent[] = [];
  const errors: string[] = [];
  for (const row of rows) {
    const decoded = decodeRow(row, catalogue);
    if (decoded.isOk) events.push(decoded.value);
    else errors.push(decoded.error.message);
  }
  return { events, errors };
}

/**
 * Sort a decoded log into fold order: **by `at`, then `seq`**. `at` is when it
 * happened, `seq` breaks same-millisecond ties; folding by arrival order would let
 * a relay delay reorder what happened.
 */
export function inFoldOrder<E extends JournalEvent>(events: readonly E[]): E[] {
  return [...events].sort((a, b) => a.at.localeCompare(b.at) || a.seq - b.seq);
}

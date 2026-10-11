// How an event type is declared: its name, the version this build writes, the
// stream it lives on and the payload schema, stated once. The journal owns none
// of these: each module that appends events declares its own in its
// `shared/events.ts` and registers them (`journal.catalogue.register`), and the codec
// validates against whatever is registered. Pure tier.
import { z } from "zod";

import type { JournalEvent } from "../codec";
import type * as Envelope from "./envelope";

/** An ISO-8601 timestamp in a payload; the same check every module's schemas use. */
export const IsoDate = z.string().min(1);

/** A payload schema: every payload is a JSON object. */
type PayloadSchema = z.ZodType<Record<string, unknown>>;

/** One event type at the version this build writes. */
export interface EventDefinition<
  T extends string = string,
  V extends number = number,
  S extends string = string,
  P extends PayloadSchema = PayloadSchema,
> {
  readonly type: T;
  /** The payload version written today. A changed payload is a new `v`, an upcaster and a frozen fixture. */
  readonly v: V;
  /** The stream type it is appended to; an append on any other stream is refused. */
  readonly streamType: S;
  readonly schema: P;
}

/**
 * Declare an event type. The `const` parameters keep `type`, `v` and
 * `streamType` as literals, which is what lets `EventOf` pin a writer to them.
 */
export function defineJournalEvent<
  const T extends string,
  const V extends number,
  const S extends string,
  P extends PayloadSchema,
>(def: EventDefinition<T, V, S, P>): EventDefinition<T, V, S, P> {
  return def;
}

/**
 * The event a writer appends for one definition. `type`, `v` and `streamType`
 * are the definition's literals and the payload is its schema's type, so bumping
 * `v` fails to compile at every writer still building the old version.
 * Distributes over a union of definitions.
 */
export type EventOf<D extends EventDefinition> = D extends EventDefinition
  ? Envelope.NewEvent<z.infer<D["schema"]>> & {
      type: D["type"];
      v: D["v"];
      streamType: D["streamType"];
    }
  : never;

/**
 * A stored event of one written type, as a fold reads it: decoded, so upcast
 * and validated against its registered schema. Distributes over a union.
 */
export type DecodedOf<E extends Envelope.NewEvent> = E extends Envelope.NewEvent
  ? Omit<JournalEvent, "type" | "payload"> & { type: E["type"]; payload: E["payload"] }
  : never;

/**
 * A guard narrowing a decoded event to one module's own types. Checking the
 * type name is enough: the codec already validated the payload against the
 * schema registered for that name, and a name has one owner.
 */
export function guardFor<const D extends readonly EventDefinition[]>(defs: D) {
  const types: ReadonlySet<string> = new Set(defs.map((d) => d.type));
  return (e: JournalEvent): e is DecodedOf<EventOf<D[number]>> => types.has(e.type);
}

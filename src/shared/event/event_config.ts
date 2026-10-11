// One object per event, holding everything about it: its payload `schema`, how
// it moves the module's state (`evolve`), the commands that produce it
// (`commands`, each a schema and a `decide`), and which columns of the
// module's table it writes (`project`). A module lists its events, its state
// and its table in `defineEventModule`; `registerEventModule` in the node tier
// runs it.
//
// The rule lives once, in `evolve`. `project` reads the state `evolve` just
// produced and only maps it to columns, so the table cannot disagree with what
// `decide` sees. Pure tier.
import { Fail, Ok } from "@vyredo/typed-result";
import type { ReturnResult } from "@vyredo/typed-result";
import type { SQLiteTable } from "drizzle-orm/sqlite-core";
import type { z } from "zod";

import type { JournalEvent } from "../codec";
import type * as Decider from "../decider";
import type * as Catalogue from "./catalogue";
import type { Upcaster } from "./contract";
import * as Define from "./define";
import type * as Envelope from "./envelope";
import * as Sample from "./sample";

/** An event as `decide` states it; `make` builds one and `handle` puts it on the stream. */
export type DecidedEvent = Decider.Decided<Envelope.NewEvent>;

/** What `decide` returns: the events to append, `[]` for none, or a refusal. */
export type Decision = ReturnResult<DecidedEvent[]>;

const isColumns = (v: unknown): v is Columns => typeof v === "object" && v !== null && !Array.isArray(v);

/** Nothing decided: a retried command, or one the state makes moot. */
export const noEvents = (): Decision => {
  const none: DecidedEvent[] = [];
  return Ok(none);
};

/** What a decided event carries besides its payload. */
export interface EventMeta {
  origin: string;
  /** When it happened: always the command's, so `decide` stays pure. */
  at: string;
  /** Diagnostics only. */
  via?: string;
}

/**
 * How a module's table relates to its streams:
 * - `row`: one row per stream, keyed by the stream's id. Each event's `project`
 *   returns the columns it writes.
 * - `keyed`: many rows per stream, each under its own key. Each event's
 *   `project` returns `{ [key]: columns }`, or `{ [key]: null }` to drop that
 *   row. Columns update the row; it is inserted only when they form a whole one.
 * - `append`: rows that follow from one event alone, many per stream. Each
 *   event's `project` returns the rows it adds, read off the event, never the
 *   state, so a live append writes just that event's rows instead of re-folding.
 */
export type TableShape = "row" | "keyed" | "append";

/** The types a module's events are written against: its state, its table, and the table's shape. */
export interface ModuleTypes<State, Table extends SQLiteTable, Shape extends TableShape = "row"> {
  state: State;
  table: Table;
  shape: Shape;
}
export type AnyModule = ModuleTypes<unknown, SQLiteTable, TableShape>;
type InsertOf<M extends AnyModule> = M["table"]["$inferInsert"];

type PatchFor<S extends TableShape, Insert> = S extends "keyed"
  ? Readonly<Record<string, Partial<Insert> | null>>
  : S extends "append"
    ? readonly Insert[]
    : Partial<Insert>;

/** What one event's `project` writes, by the table's shape. */
export type RowPatch<M extends AnyModule> = PatchFor<M["shape"], InsertOf<M>>;

/** A row as the node runner writes it: column property to value. */
type Columns = Record<string, unknown>;

/** What the node runner needs to write a module's table, by shape. */
export type TableWrite =
  | {
    shape: "row";
    table: SQLiteTable;
    /** The key column; the runner fills it with the stream's id. */
    key: string;
    /** The stream's row: each event's columns merged in fold order; null when none wrote any. */
    toRow: (eventsInFoldOrder: readonly JournalEvent[]) => Columns | null;
  }
  | {
    shape: "keyed";
    table: SQLiteTable;
    /** The column holding the stream's id: the stream's rows are the ones under it. */
    scope: string;
    /** The column each row is keyed by within the stream. */
    key: string;
    /** The stream's rows by key, in fold order; a null was dropped. */
    toRows: (eventsInFoldOrder: readonly JournalEvent[]) => Map<string, Columns | null>;
  }
  | {
    shape: "append";
    table: SQLiteTable;
    scope: string;
    /** Columns that, with the scope, name one row: a later row with the same values replaces it. */
    supersede: readonly string[];
    /** The rows one event adds. */
    rowsOf: (event: JournalEvent) => readonly Columns[];
  };

/** Command name to the type of its data. */
export type CommandData = Record<string, Record<string, unknown>>;

/** One command an event accepts: the schema its data must pass, and the rule that decides it. */
export interface CommandRule<State, D> {
  /** Commands are validated like payloads: one built wrong is refused before anything is decided. */
  readonly schema: z.ZodType<D>;
  /**
   * `[]` is a no-op, which is how a retried command stays idempotent; a refusal is a `Fail`.
   * A property, not a method, so its parameters are checked strictly rather than bivariantly.
   */
  readonly decide: (command: D, state: State) => Decision;
}

export type PayloadSchema = z.ZodType<Record<string, unknown>>;
export type EventFor<T extends string, V extends number, S extends string, P extends PayloadSchema> =
  Define.EventOf<Define.EventDefinition<T, V, S, P>>;

/** What a module writes for one event. Leave out what the event does not do. */
export interface EventSpec<
  M extends AnyModule,
  T extends string,
  V extends number,
  S extends string,
  P extends PayloadSchema,
  C extends CommandData,
> {
  type: T;
  /** The payload version this build writes. A changed payload is a new file, `<Name>V<n+1>.ts`. */
  v: V;
  streamType: S;
  /** The payload, validated on append and again on read. */
  schema: P;
  /**
   * A payload for the test suites. Generated from `schema` when absent; give one
   * only where a realistic value matters.
   */
  example?: Record<string, unknown>;
  /** The payload fields that carry personal data; a redacted export pseudonymises them. */
  personal?: readonly (keyof z.infer<P> & string)[];
  /**
   * The version this one replaced, once an event has been bumped: its frozen
   * schema and the upcast that turns a stored payload of it into this version's.
   * The ladder is built from this chain; nothing registers upcasters by hand.
   */
  previous?: RetiredEvent<T, number, z.infer<P>>;
  /**
   * The type name this event was stored under before a rename. Old rows keep
   * that name (a stored event is immutable) and read as this type; the module's
   * folds select them too. Never written.
   */
  formerType?: string;
  /** How this event moves the state. The one place its rule is written. */
  evolve?: (state: M["state"], event: Define.DecodedOf<EventFor<T, V, S, P>>) => M["state"];
  /** The commands that produce this event, by name. */
  commands?: { [K in keyof C]: CommandRule<M["state"], C[K]> };
  /**
   * The columns of the module's table this event writes, read off the state
   * `evolve` just produced. A mapping, not a rule: no conditions belong here.
   * Null writes nothing.
   */
  project?: (state: M["state"], event: Define.DecodedOf<EventFor<T, V, S, P>>) => RowPatch<M> | null;
}

/** One defined event, as its module and its writers use it. */
export interface EventDef<M extends AnyModule, E extends Envelope.NewEvent = Envelope.NewEvent, C extends CommandData = CommandData> {
  readonly type: E["type"];
  /** What registers into the journal's catalogue. */
  readonly registration: Catalogue.RegisteredEventType;
  /** Type only: the commands' data, so the module can name its command union. */
  readonly commandData?: C;
  /**
   * Build one of this event's commands: `RecordingEnded.command("StopRecording", {...})`.
   * The call site names the event, so "find references" on an event lists every
   * place that asks for it, and the name is checked against this event's commands.
   */
  command<K extends keyof C & string>(type: K, data: C[K]): { type: K } & C[K];
  /** Whether a late arrival of this event can change the table, so its arrival must re-project. */
  readonly affectsTable: boolean;
  /** Build this event for `decide`, typed by its schema. */
  make(payload: E["payload"], meta: EventMeta): DecidedEvent;
  evolve(state: M["state"], event: JournalEvent): M["state"];
  /** The rule for `command`, or null when this event has none for it. */
  decide(command: { type: string }, state: M["state"]): Decision | null;
  project(state: M["state"], event: JournalEvent): RowPatch<M> | null;
}

/**
 * Define one event. Curried so the module's types are stated once and the rest
 * is inferred: `defineEvent<OrderModule>()({ ... })`.
 */
export function defineEvent<M extends AnyModule>() {
  return <
    const T extends string,
    const V extends number,
    const S extends string,
    P extends PayloadSchema,
    C extends CommandData = Record<never, never>,
  >(
    spec: EventSpec<M, T, V, S, P, C>,
  ): EventDef<M, EventFor<T, V, S, P>, C> => {
    const definition = Define.defineJournalEvent({ type: spec.type, v: spec.v, streamType: spec.streamType, schema: spec.schema });
    const is = Define.guardFor([definition]);
    return {
      type: spec.type,
      registration: {
        ...definition,
        // A malformed generated sample is caught where the catalogue registers it.
        fixture: spec.example ?? Sample.sampleOf(spec.schema),
        ...ladderOf(spec.previous),
        ...(spec.personal ? { personal: spec.personal } : {}),
        ...(spec.formerType ? { formerTypes: [spec.formerType] } : {}),
      },
      affectsTable: spec.evolve !== undefined || spec.project !== undefined,
      command: (type, data) => ({ ...data, type }),
      make: (payload, meta) => ({ type: spec.type, v: spec.v, payload, ...meta }),
      evolve: (state, event) => (spec.evolve && is(event) ? spec.evolve(state, event) : state),
      decide: (command, state) => {
        const rule = spec.commands?.[command.type];
        if (!rule) return null;
        const parsed = rule.schema.safeParse(command);
        if (!parsed.success) return Fail(`${command.type}: ${parsed.error.message}`);
        return rule.decide(parsed.data, state);
      },
      project: (state, event) => (spec.project && is(event) ? spec.project(state, event) : null),
    };
  };
}

/**
 * Define one device observation: an event appended directly by the thing that
 * measured it, decided by no command of this module. Its spec names no
 * `commands`, and every field an observation always has is required here:
 * `type`, `v`, `streamType`, `schema`, `evolve` and `project`. No rule decided
 * the fact, so the fold learns it from `evolve` alone; `project` is stated
 * even where the event writes no row (it returns null there).
 */
export function defineObservation<M extends AnyModule>() {
  return <
    const T extends string,
    const V extends number,
    const S extends string,
    P extends PayloadSchema,
  >(
    spec: Omit<EventSpec<M, T, V, S, P, Record<never, never>>, "evolve" | "project" | "commands"> & {
      evolve: (state: M["state"], event: Define.DecodedOf<EventFor<T, V, S, P>>) => M["state"];
      project: (state: M["state"], event: Define.DecodedOf<EventFor<T, V, S, P>>) => RowPatch<M> | null;
    },
  ): EventDef<M, EventFor<T, V, S, P>, Record<never, never>> => defineEvent<M>()<T, V, S, P>(spec);
}

/**
 * An event version no build writes any more. It keeps its frozen schema (the
 * schema lock still holds it to that) and the step that turns a stored payload
 * of it into the next version's. `Out` is that next payload, type only, so a
 * wrong upcast is a compile error where the next version names this one.
 */
export interface RetiredEvent<T extends string = string, V extends number = number, Out = Record<string, unknown>> {
  readonly type: T;
  readonly v: V;
  readonly schema: PayloadSchema;
  readonly upcast: Upcaster;
  readonly previous?: RetiredEvent<T>;
  /** Type only: what `upcast` produces. */
  readonly next?: Out;
}

/**
 * Retire a version: what its file shrinks to once the next version exists.
 * `upcast` receives the payload already checked against this version's schema;
 * a stored payload that does not fit passes through unchanged, and the next
 * version's schema refuses it on read, loudly.
 */
export function retiredEvent<
  const T extends string,
  const V extends number,
  P extends PayloadSchema,
  Out extends Record<string, unknown>,
>(spec: {
  type: T;
  v: V;
  schema: P;
  upcast: (payload: z.infer<P>) => Out;
  /** The version before this one, when this one was itself an upgrade. */
  previous?: RetiredEvent<T, number, z.infer<P>>;
}): RetiredEvent<T, V, Out> {
  return {
    type: spec.type,
    v: spec.v,
    schema: spec.schema,
    upcast: (stored) => {
      const parsed = spec.schema.safeParse(stored);
      return parsed.success ? spec.upcast(parsed.data) : stored;
    },
    ...(spec.previous ? { previous: spec.previous } : {}),
  };
}

/** The catalogue's upcaster ladder and the retired schemas, from a `previous` chain. */
function ladderOf(previous: RetiredEvent | undefined): Pick<Catalogue.RegisteredEventType, "upcasters" | "retired"> {
  if (!previous) return {};
  const upcasters: Record<number, Upcaster> = {};
  const retired: Array<{ v: number; schema: PayloadSchema }> = [];
  for (let step: RetiredEvent | undefined = previous; step; step = step.previous) {
    upcasters[step.v] = step.upcast;
    retired.push({ v: step.v, schema: step.schema });
  }
  return { upcasters, retired };
}

/** The event a writer appends for one defined event: `type OrderPlaced = EventOfDef<typeof OrderPlaced>`. */
export type EventOfDef<D> = D extends EventDef<infer _M, infer E, infer _C> ? E : never;

/** A command of a module: its name as `type`, plus its data. */
type CommandsOf<D> = D extends EventDef<infer _M, infer _E, infer C>
  ? { [K in keyof C & string]: { type: K } & C[K] }[keyof C & string]
  : never;

/** Every event the module's events define, as writers build them; derived from its list. */
export type EventsOf<Module extends { events: readonly unknown[] }> = EventOfDef<Module["events"][number]>;

/** Every command the module's events accept. */
export type CommandOf<Module extends { events: readonly unknown[] }> = CommandsOf<Module["events"][number]>;

/**
 * A module's events, assembled: the decider for `handle`, the catalogue
 * registration, and the row its table holds for one stream.
 */
export function defineEventModule<
  State,
  Table extends SQLiteTable,
  const Events extends readonly EventDef<ModuleTypes<State, Table, TableShape>>[],
>(spec: {
  /** The projection domain; `journal_meta` records its version per stream. */
  name: string;
  /** Bump when an `evolve` or a `project` changes, so every stream re-projects at boot. */
  version: number;
  /** Assumed for a stream whose recorded versions predate this module; 0 re-projects it. */
  versionWhenUnrecorded?: number;
  table: Table;
  /** How the table relates to the streams; see `TableShape`. */
  projection:
    | { shape: "row"; key: keyof Table["$inferInsert"] & string }
    | { shape: "keyed"; scope: keyof Table["$inferInsert"] & string; key: keyof Table["$inferInsert"] & string }
    | {
      shape: "append";
      scope: keyof Table["$inferInsert"] & string;
      supersede: readonly (keyof Table["$inferInsert"] & string)[];
    };
  initialState: () => State;
  events: Events;
}) {
  type Command = CommandsOf<Events[number]>;
  const { initialState, events } = spec;
  const defOf = (type: string) => events.find((d) => d.type === type);

  const evolve = (state: State, event: JournalEvent): State => defOf(event.type)?.evolve(state, event) ?? state;

  const decide = (command: Command, state: State): Decision => {
    for (const def of events) {
      const decided = def.decide(command, state);
      if (decided) return decided;
    }
    return Fail(`no event handles the command ${command.type}`);
  };

  /** Fold in order, handing each event's `project` the state its `evolve` produced. */
  const eachPatch = (eventsInFoldOrder: readonly JournalEvent[], visit: (patch: unknown) => void) => {
    let state = initialState();
    for (const event of eventsInFoldOrder) {
      const def = defOf(event.type);
      if (!def) continue;
      state = def.evolve(state, event);
      const patch = def.project(state, event);
      if (patch) visit(patch);
    }
  };

  const projection = spec.projection;
  const write: TableWrite =
    projection.shape === "row"
      ? {
        shape: "row",
        table: spec.table,
        key: projection.key,
        toRow: (events) => {
          let row: Columns | null = null;
          eachPatch(events, (patch) => {
            if (isColumns(patch)) row = { ...row, ...patch };
          });
          return row;
        },
      }
      : projection.shape === "keyed"
        ? {
          shape: "keyed",
          table: spec.table,
          scope: projection.scope,
          key: projection.key,
          toRows: (events) => {
            const rows = new Map<string, Columns | null>();
            eachPatch(events, (patch) => {
              if (!isColumns(patch)) return;
              for (const [key, row] of Object.entries(patch)) {
                const previous = rows.get(key);
                rows.set(key, isColumns(row) ? { ...previous, ...row } : null);
              }
            });
            return rows;
          },
        }
        : {
          shape: "append",
          table: spec.table,
          scope: projection.scope,
          supersede: projection.supersede,
          // An append event's rows come from the event alone; the state passed is a placeholder.
          rowsOf: (event) => {
            const patch = defOf(event.type)?.project(initialState(), event);
            return Array.isArray(patch) ? patch.filter(isColumns) : [];
          },
        };

  // A renamed event's former name too: the store selects a stream's rows by type,
  // and rows written before the rename still carry the old one.
  const eventTypes = events.flatMap((d) => [d.type, ...(d.registration.formerTypes ?? [])]);
  const decider: Decider.Decider<Command, State, Envelope.NewEvent> = { initialState, evolve, decide, eventTypes };
  return {
    name: spec.name,
    version: spec.version,
    ...(spec.versionWhenUnrecorded !== undefined ? { versionWhenUnrecorded: spec.versionWhenUnrecorded } : {}),
    write,
    events,
    initialState,
    evolve,
    decide,
    decider,
    eventTypes,
    streamTypes: [...new Set(events.map((d) => d.registration.streamType))],
    /** The types whose arrival must re-project the row. */
    liveTypes: events.filter((d) => d.affectsTable).map((d) => d.type),
    registration: events.map((d) => d.registration),
    isEvent: (event: JournalEvent) => defOf(event.type) !== undefined,
  };
}

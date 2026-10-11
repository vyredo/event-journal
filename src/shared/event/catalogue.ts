// The event catalogue: the lookup tables the codec validates against. A
// journal owns one and starts it empty; every module that appends events
// registers its own definitions into it at boot, core and client alike. Declare
// or version an event in its module, never here.
import type { Catalogue, PayloadSchema, Upcaster } from "./contract";

/**
 * One event type as its owning module registers it: its definition
 * (`defineJournalEvent`), a frozen payload at its current `v`, and, once it has been
 * bumped, the upcasters from each older version.
 */
export interface RegisteredEventType {
  type: string;
  /**
   * The stream type it is appended to, as its module declared it with
   * `defineStream`.
   */
  streamType: string;
  v: number;
  schema: PayloadSchema;
  /** A frozen v`v` payload: the fixture suites validate and replay it. */
  fixture: Record<string, unknown>;
  /** `fromV` -> upcaster producing `fromV + 1`; absent while the type is at v1. */
  upcasters?: Readonly<Record<number, Upcaster>>;
  /** The frozen schema of every version before `v`; absent while the type is at v1. */
  retired?: ReadonlyArray<{ v: number; schema: PayloadSchema }>;
  /**
   * The payload fields that carry personal data. Declared by the event, beside
   * the field, so a new personal field cannot be missed by a scan that only
   * knows the old ones; a redacted export pseudonymises exactly these.
   */
  personal?: readonly string[];
  /**
   * The names this type was stored under before it was renamed. A stored event
   * is immutable, so its type string is too: the log keeps the old name, and the
   * codec reads it as this one. Only this type is ever written.
   */
  formerTypes?: readonly string[];
}

/**
 * One journal's vocabulary: what it validates appends against, decodes rows
 * with, and redacts by. It satisfies `Catalogue`, so it is passed straight to
 * the codec.
 */
export class EventCatalogue implements Catalogue {
  /** `type` -> the version written for it. */
  readonly currentV: Record<string, number> = {};
  /** `type` -> the stream it is appended to. */
  readonly streamTypes: Record<string, string> = {};
  /** `type` -> `v` -> schema: its current version and the retired ones. */
  readonly schemas: Record<string, Record<number, PayloadSchema>> = {};
  /** `type` -> `fromV` -> upcaster producing `fromV + 1`. */
  readonly upcasters: Record<string, Record<number, Upcaster>> = {};
  /** `type` -> its payload fields that carry personal data. */
  readonly personal: Record<string, readonly string[]> = {};
  /** A renamed type's former name -> its current name. */
  readonly renamed: Record<string, string> = {};
  /** Every type registered so far, for the fixture and agreement suites. */
  readonly registered: RegisteredEventType[] = [];

  /**
   * Open the catalogue to a module's event types; once per type. Registering
   * the very same definition again is a no-op, so a test and the composition
   * root can both register without coordinating.
   */
  register = (entries: readonly RegisteredEventType[]) => {
    for (const e of entries) {
      const existing = this.registered.find((r) => r.type === e.type);
      if (existing) {
        if (existing.schema === e.schema && existing.v === e.v && existing.streamType === e.streamType) {
          continue;
        }
        // invariant: a type has one owner; a different second registration is a wiring bug or a name clash.
        throw new Error(`event type "${e.type}" is already in the catalogue`);
      }
      if (!e.schema.safeParse(e.fixture).success) {
        // invariant: a frozen fixture must fit its own schema, or the suites prove nothing.
        throw new Error(`event type "${e.type}": fixture does not match its schema`);
      }
      if (!e.streamType) {
        // invariant: an event with no stream could never be folded by anyone.
        throw new Error(`event type "${e.type}" names no stream type`);
      }
      this.currentV[e.type] = e.v;
      this.streamTypes[e.type] = e.streamType;
      // Historical versions too: the fixture corpus and the schema lock read them.
      this.schemas[e.type] = {
        ...Object.fromEntries((e.retired ?? []).map((r) => [r.v, r.schema])),
        [e.v]: e.schema,
      };
      if (e.upcasters) this.upcasters[e.type] = { ...e.upcasters };
      if (e.personal && e.personal.length > 0) this.personal[e.type] = [...e.personal];
      for (const former of e.formerTypes ?? []) {
        if (this.currentV[former] !== undefined || (this.renamed[former] ?? e.type) !== e.type) {
          // invariant: a former name points at one current type, and is not itself written.
          throw new Error(`event type "${e.type}": former name "${former}" is already taken`);
        }
        this.renamed[former] = e.type;
      }
      this.registered.push(e);
    }
  };

  /** Forget every registered type. */
  cleanup = () => {
    for (const table of [this.currentV, this.streamTypes, this.schemas, this.upcasters, this.personal, this.renamed]) {
      for (const key of Object.keys(table)) delete table[key];
    }
    this.registered.length = 0;
  };
}

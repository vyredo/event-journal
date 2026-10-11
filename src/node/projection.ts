// The projection runner: the slices bound to one journal, run against its
// rows. Two paths: a rebuild drops and re-inserts the requested domains of one
// stream from a fresh fold; a live append applies one just-appended event
// through the live rule its type's owner registered. The journal knows no
// domain: every slice, core's and a client's, registers from the composition
// root. Runs inside the caller's transaction.
import { Result } from "@vyredo/typed-result";
import { eq, getTableColumns, getTableName } from "drizzle-orm";
import type { SQLiteTable } from "drizzle-orm/sqlite-core";
import { sql } from "drizzle-orm/sql";

import * as Codec from "../shared/codec";
import type { Clock } from "../shared/determinism";
import type * as EventConfig from "../shared/event/event_config";
import type { EventCatalogue } from "../shared/event/catalogue";
import type { JournalRow, StreamRef } from "../shared/event/envelope";
import * as ErrorMessage from "../shared/errors";
import type { JournalLogger } from "./log";
import type { JournalDb, JournalStore } from "./store";

/** One projection domain as the committer sees it; its owner registers it. */
export interface ProjectionSlice {
  /**
   * The stream types this slice folds. A rebuild of a stream runs exactly the
   * slices that name its type, so a slice never sees another stream's events.
   */
  streamTypes: readonly string[];
  /**
   * Drop the stream's rows for this domain and reinsert the fold's. The events
   * are every decoded event of the stream, in fold order; the slice narrows
   * them to its own types with its module's guard.
   */
  rebuild(streamId: string, events: Codec.JournalEvent[]): void;
  /**
   * The fold version; bump it when the fold rule changes, so the boot sweep
   * re-folds this domain for every stream it folds.
   */
  version: number;
  /**
   * The version to assume when a stream's recorded map has no entry for this
   * domain. 0, the default, reads as "never folded" and rebuilds it; a slice
   * added after streams already had maps sets its first version here, so its
   * arrival does not re-fold every stream on the next boot.
   */
  versionWhenUnrecorded?: number;
  /** The live rule for the event types this slice owns, applied in the append's transaction. */
  live?: {
    types: readonly string[];
    apply(event: Codec.JournalEvent): void;
  };
}

/** One event type's live rule, and the domain it folds into. */
interface LiveRule {
  /** The slice the rule came from: the domain a failed fold marks stale. */
  domain: string;
  apply(event: Codec.JournalEvent): void;
}
/**
 * What an append did to the projection. `applied`: the live rule ran (or, on a
 * duplicate, ran when the row was first stored). `none`: the type has no live
 * rule. `stale`: the fold failed, or its domain was already stale on this
 * stream, so the row is stored and the projection waits for a rebuild.
 */
export type ProjectionOutcome = "applied" | "none" | "stale";

/** A stored row, and what became of its projection. */
export interface Appended {
  row: JournalRow;
  projection: ProjectionOutcome;
}

/** An event module as `registerEventModule` binds it: what `defineEventModule` returns. */
export interface EventModuleBinding {
  name: string;
  version: number;
  versionWhenUnrecorded?: number;
  streamTypes: readonly string[];
  eventTypes: readonly string[];
  liveTypes: readonly string[];
  write: EventConfig.TableWrite;
}

/** The savepoint one live fold runs in. */
const LIVE_FOLD_SAVEPOINT = "journal_live_fold";

/** One column of `table` by its property name. */
function columnOf(table: SQLiteTable, prop: string) {
  const column = getTableColumns(table)[prop];
  // invariant: a module's rows are typed by its table, so an unknown column is a wiring bug.
  if (!column) throw new Error(`${getTableName(table)} has no column "${prop}"`);
  return column;
}

export class ProjectionRunner {
  private readonly slices = new Map<string, ProjectionSlice>();
  /** Registered live rules by event type, from the slices' `live`. */
  private readonly liveRules = new Map<string, LiveRule>();
  /** Each event module's table and how it is written, by domain; what `projectedRows` reads. */
  private readonly moduleWrites = new Map<string, EventConfig.TableWrite>();

  constructor(
    private readonly deps: {
      db: JournalDb;
      store: JournalStore;
      catalogue: EventCatalogue;
      log: JournalLogger;
    },
  ) { }

  /**
   * The current fold version of every domain that folds `streamType`: what
   * `journal_meta.projectionVersions` records for a stream and what the boot
   * sweep compares against.
   */
  projectionVersions = (streamType: string): Record<string, number> => {
    const versions: Record<string, number> = {};
    for (const [domain, slice] of this.slices) {
      if (slice.streamTypes.includes(streamType)) versions[domain] = slice.version;
    }
    return versions;
  };

  /** The version assumed for `domain` when a stream's recorded map has none. */
  versionWhenUnrecorded = (domain: string): number => this.slices.get(domain)?.versionWhenUnrecorded ?? 0;

  /** Bind one domain; called once per domain from the composition root. */
  registerProjectionSlice = (domain: string, slice: ProjectionSlice) => {
    if (this.slices.has(domain)) {
      // invariant: the composition root binds each domain exactly once; a second bind is a wiring bug.
      throw new Error(`projection slice for "${domain}" registered twice`);
    }
    if (slice.streamTypes.length === 0) {
      // invariant: a slice that folds no stream type would never run.
      throw new Error(`projection slice for "${domain}" folds no stream type`);
    }
    for (const type of slice.live?.types ?? []) {
      if (this.liveRules.has(type)) {
        // invariant: each event type has one live rule; a second bind is a wiring bug.
        throw new Error(`live rule for "${type}" registered twice`);
      }
    }
    this.slices.set(domain, slice);
    const live = slice.live;
    if (live) for (const type of live.types) this.liveRules.set(type, { domain, apply: live.apply });
  };

  /** Whether `domain` is bound; a test registering a slice twice in one worker checks first. */
  hasProjectionSlice = (domain: string): boolean => this.slices.has(domain);

  /**
   * Write one stream's row, touching exactly the columns the row names, so a
   * column no event owns is never changed. Update first, and insert only when
   * there is no row and the patch forms a whole one: a stream whose log starts
   * after its row did (a row written before the journal existed) still gets its
   * partial patch. The key column is the stream's id.
   */
  private writeRow = (table: SQLiteTable, key: string, streamId: string, row: Record<string, unknown>) => {
    const columns = getTableColumns(table);
    const keyColumn = columns[key];
    // invariant: the module's key is typed as one of its table's columns.
    if (!keyColumn) throw new Error(`${getTableName(table)} has no key column "${key}"`);
    const named = Object.entries(row)
      .filter(([prop, value]) => value !== undefined && prop !== key)
      .map(([prop]) => ({ name: sql.identifier(columnOf(table, prop).name), value: row[prop] }));
    if (named.length === 0) return;
    const keyName = sql.identifier(keyColumn.name);
    const updated = this.deps.db.run(sql`
      UPDATE ${table} SET ${sql.join(named.map((c) => sql`${c.name} = ${c.value}`), sql`, `)}
      WHERE ${keyName} = ${streamId}`);
    if (updated.changes > 0) return;
    // A patch that cannot form a whole row (a flag for a row this database never
    // had) only ever updates one; there is nothing to insert.
    const given = new Set(named.map((c) => c.name.value));
    const formsRow = Object.values(columns).every(
      (c) => c.name === keyColumn.name || !c.notNull || c.hasDefault || given.has(c.name),
    );
    if (!formsRow) return;
    this.deps.db.run(sql`
      INSERT INTO ${table} (${keyName}, ${sql.join(named.map((c) => c.name), sql`, `)})
      VALUES (${streamId}, ${sql.join(named.map((c) => sql`${c.value}`), sql`, `)})`);
  };

  /** Insert one whole row. */
  private insertRow = (table: SQLiteTable, row: Record<string, unknown>) => {
    const named = Object.entries(row)
      .filter(([, value]) => value !== undefined)
      .map(([prop, value]) => ({ name: sql.identifier(columnOf(table, prop).name), value }));
    this.deps.db.run(sql`
      INSERT INTO ${table} (${sql.join(named.map((c) => c.name), sql`, `)})
      VALUES (${sql.join(named.map((c) => sql`${c.value}`), sql`, `)})`);
  };

  /** Delete the rows whose columns equal every value in `match`. */
  private deleteWhere = (table: SQLiteTable, match: Record<string, unknown>) => {
    const conditions = Object.entries(match).map(
      ([prop, value]) => sql`${sql.identifier(columnOf(table, prop).name)} = ${value}`,
    );
    this.deps.db.run(sql`DELETE FROM ${table} WHERE ${sql.join(conditions, sql` AND `)}`);
  };

  /**
   * One stream's rows in a module's table, keyed by column property: the rows
   * `reconcile` compares with the log. Empty for a domain that is not an event
   * module.
   */
  projectedRows = (domain: string, stream: StreamRef): Record<string, unknown>[] => {
    const write = this.moduleWrites.get(domain);
    if (!write) return [];
    const by = columnOf(write.table, write.shape === "row" ? write.key : write.scope);
    // Drizzle's select decodes each column (JSON, booleans) the way the module's `project` produced it.
    return this.deps.db.select().from(write.table).where(eq(by, stream.streamId)).all();
  };

  /**
   * Bind a module defined with `defineEventModule`, whatever its table's shape:
   * - `row`: re-fold the stream, merge each event's columns, update-then-insert
   *   the one row.
   * - `keyed`: re-fold the stream and write each key its events name: a null
   *   deletes that row, a patch updates it (inserting only a whole row). A late
   *   event lands by its `at`, and a row no event names is never touched.
   * - `append`: a live append writes just that event's rows (each replacing the
   *   row its `supersede` columns name); a rebuild clears the stream's rows and
   *   replays every event in fold order. No re-fold per append, which over a
   *   long stream would be quadratic.
   * Live and rebuild run the same writes. `afterWrite` runs once a stream's rows
   * are written, inside the transaction; anything it sends must wait for commit.
   */
  registerEventModule = (module: EventModuleBinding, opts: { afterWrite?: (streamId: string) => void } = {}) => {
    const { write } = module;
    this.moduleWrites.set(module.name, write);

    const appendRows = (streamId: string, event: Codec.JournalEvent) => {
      if (write.shape !== "append") return;
      for (const row of write.rowsOf(event)) {
        const match: Record<string, unknown> = { [write.scope]: streamId };
        for (const column of write.supersede) match[column] = row[column];
        this.deleteWhere(write.table, match);
        this.insertRow(write.table, { ...row, [write.scope]: streamId });
      }
    };

    const project = (streamId: string, eventsInFoldOrder: readonly Codec.JournalEvent[]) => {
      switch (write.shape) {
        case "row": {
          const row = write.toRow(eventsInFoldOrder);
          if (row) this.writeRow(write.table, write.key, streamId, row);
          break;
        }
        case "keyed": {
          // Only the keys the events name: a row the log never mentioned (one
          // written before the journal existed) is left alone.
          for (const [key, row] of write.toRows(eventsInFoldOrder)) {
            if (row === null) this.deleteWhere(write.table, { [write.scope]: streamId, [write.key]: key });
            else this.writeRow(write.table, write.key, key, { ...row, [write.scope]: streamId });
          }
          break;
        }
        case "append": {
          this.deleteWhere(write.table, { [write.scope]: streamId });
          for (const event of eventsInFoldOrder) appendRows(streamId, event);
          break;
        }
      }
      opts.afterWrite?.(streamId);
    };

    const refold = (event: Codec.JournalEvent) => {
      const stream = { streamType: event.streamType, streamId: event.streamId };
      const rows = this.deps.store.streamRowsSync(stream, { types: module.eventTypes });
      const { events, errors } = Codec.decodeRows(rows, this.deps.catalogue);
      if (errors.length > 0) {
        this.deps.log.error("re-projection decode errors", { ...stream, domain: module.name, errors });
      }
      project(event.streamId, Codec.inFoldOrder(events));
    };

    this.registerProjectionSlice(module.name, {
      streamTypes: module.streamTypes,
      version: module.version,
      ...(module.versionWhenUnrecorded !== undefined ? { versionWhenUnrecorded: module.versionWhenUnrecorded } : {}),
      rebuild: project,
      live: {
        types: module.liveTypes,
        apply: (event) => {
          if (write.shape !== "append") return refold(event);
          appendRows(event.streamId, event);
          opts.afterWrite?.(event.streamId);
        },
      },
    });
  };

  /**
   * Rebuild one stream's projections, without a transaction of its own: every
   * domain here drops the stream's rows before reinserting, so a caller that is
   * already in a transaction gets one atomic unit. `EventJournal.rebuildStream`
   * is the entry point that opens one. False when the stream has no rows.
   */
  rebuildDomains = (stream: StreamRef, clock: Clock, opts: { domains?: string[] }): boolean => {
    const journalRows = this.deps.store.streamRowsSync(stream);
    if (journalRows.length === 0) return false;

    for (const domain of opts.domains ?? []) {
      if (!this.slices.has(domain)) {
        // invariant: the composition root registers every slice before any rebuild names it.
        throw new Error(`rebuild of "${domain}" requested but no slice is registered for it`);
      }
    }

    // Decode once; every domain fold runs over the same events.
    const { events, errors } = Codec.decodeRows(journalRows, this.deps.catalogue);
    if (errors.length > 0) this.deps.log.error("rebuild decode errors", { ...stream, errors });

    const foldedEvents = Codec.inFoldOrder(events);
    const domains = [...this.slices.entries()].filter(
      ([domain, slice]) =>
        slice.streamTypes.includes(stream.streamType) &&
        (opts.domains === undefined || opts.domains.includes(domain)),
    );
    for (const [, slice] of domains) slice.rebuild(stream.streamId, foldedEvents);

    this.deps.store.upsertJournalMeta(stream, {
      projectionVersions: JSON.stringify(this.projectionVersions(stream.streamType)),
      rebuiltAt: clock.now(),
    });
    return true;
  };

  /** A stream's recorded fold versions, or null when it has no readable map. */
  private recordedVersions = (stream: StreamRef): Record<string, number> | null => {
    const raw = this.deps.store.getJournalMeta(stream)?.projectionVersions;
    if (!raw) return null;
    const parsed = Result.wrap(() => JSON.parse(raw) as Record<string, number>);
    return parsed.isOk ? parsed.value : null;
  };

  /**
   * Whether a failed live fold marked `domain` stale on this stream. Only an entry
   * that exists and sits below the current version counts: a stream with no map,
   * or no entry for the domain, is one no fold has failed on, and treating it as
   * stale would stop every new stream's live rules at its first event.
   */
  isMarkedStale = (stream: StreamRef, domain: string): boolean => {
    const recorded = this.recordedVersions(stream)?.[domain];
    const current = this.slices.get(domain)?.version ?? 0;
    return recorded !== undefined && recorded < current;
  };

  /**
   * Record that `domain` is stale on this stream, so the boot sweep rebuilds it
   * and live folds on the stream stop until it does. Reuses the version map: 0 is
   * below every version. A stream with no map gets one holding only this entry,
   * so `isMarkedStale` sees it; the boot sweep reads the missing entries as
   * unrecorded, as it did before. A rebuild writes the full current map, which
   * clears the mark with no code of its own.
   */
  private markStale = (stream: StreamRef, domain: string) => {
    const versions = { ...this.recordedVersions(stream), [domain]: 0 };
    this.deps.store.upsertJournalMeta(stream, { projectionVersions: JSON.stringify(versions) });
  };

  /**
   * Run a just-stored row's live rule in a savepoint. The fact outlives its
   * fold: a rule that throws rolls back only its own savepoint, so a
   * half-written projection leaves nothing behind while the journal row stays.
   * The domain is then marked stale and later events on the stream only
   * append, because a rule that steps a stored row forward would build on a
   * base it never reached. A duplicate returns what its first append did.
   */
  applyLive = (row: JournalRow, duplicate: boolean): ProjectionOutcome => {
    const rule = this.liveRules.get(row.type);
    if (!rule) return "none";
    const stream: StreamRef = { streamType: row.streamType, streamId: row.streamId };
    if (this.isMarkedStale(stream, rule.domain)) return "stale";
    if (duplicate) return "applied";

    // An explicit savepoint rather than a nested `db.transaction`: better-sqlite3
    // nests that as a savepoint too, but the claim then rests on a driver detail
    // the test database's shim does not reproduce.
    const { db } = this.deps;
    db.run(sql.raw(`SAVEPOINT ${LIVE_FOLD_SAVEPOINT}`));
    const folded = Result.wrap(() => {
      // Decode the row `encodeEvent` just validated, so the rule reads it exactly
      // as a rebuild would.
      const decoded = Codec.decodeRow(row, this.deps.catalogue);
      // edge: a decode failure must undo the savepoint, not the append.
      if (!decoded.isOk) throw new Error(decoded.error.message);
      rule.apply(decoded.value);
    });
    if (folded.isOk) {
      db.run(sql.raw(`RELEASE ${LIVE_FOLD_SAVEPOINT}`));
      return "applied";
    }
    // Undo whatever the rule wrote before it threw; the journal row is outside it.
    db.run(sql.raw(`ROLLBACK TO ${LIVE_FOLD_SAVEPOINT}`));
    db.run(sql.raw(`RELEASE ${LIVE_FOLD_SAVEPOINT}`));

    this.markStale(stream, rule.domain);
    this.deps.log.error("live fold failed; fact kept, projection stale until a rebuild", {
      ...stream,
      seq: row.seq,
      type: row.type,
      domain: rule.domain,
      err: ErrorMessage.errorMessage(folded.error),
    });
    return "stale";
  };

  /**
   * The projection domains a stream's recorded version map says are stale; null
   * when there is no readable map, so the caller rebuilds everything.
   */
  staleDomains = (meta: { projectionVersions: string | null } | null, streamType: string): string[] | null => {
    if (!meta?.projectionVersions) return null;
    const projectionVersions = meta.projectionVersions;
    const parsed = Result.wrap(() => JSON.parse(projectionVersions) as Record<string, number>);
    if (!parsed.isOk) return null;
    const recorded = parsed.value;
    const current = this.projectionVersions(streamType);
    return Object.keys(current).filter((name) => {
      // A domain missing from the map was never folded, unless its slice says
      // which version to assume (one that arrived after the maps were written).
      const missing = this.versionWhenUnrecorded(name);
      return (recorded[name] ?? missing) < current[name];
    });
  };

  /** Unbind every slice, live rule and module. */
  cleanup = () => {
    this.slices.clear();
    this.liveRules.clear();
    this.moduleWrites.clear();
  };
}

// The journal's storage edge: insert, read, count, drop. The only code that
// writes the `journal` and `journal_meta` tables. Callers append through
// `EventJournal.append`, which stores the row and folds it. Never await inside
// a transaction.
import type { BaseSQLiteDatabase } from "drizzle-orm/sqlite-core";
import { and, asc, eq, inArray } from "drizzle-orm";
import { sql } from "drizzle-orm/sql";

import type { JournalRow, StreamRef } from "../shared/event/envelope";

import * as Schema from "./table";

/**
 * The database a journal runs on: any synchronous drizzle SQLite handle
 * (better-sqlite3), whose migrations include `table.ts`'s two tables.
 */
export type JournalDb = BaseSQLiteDatabase<"sync", { changes: number }, Record<string, unknown>>;

export function fromTableRow(r: typeof Schema.journal.$inferSelect): JournalRow {
  return {
    seq: r.seq,
    eventId: r.eventId,
    streamType: r.streamType,
    streamId: r.streamId,
    type: r.type,
    v: r.v,
    ev: r.ev,
    at: r.at,
    recordedAt: r.recordedAt,
    origin: r.origin,
    payload: JSON.parse(r.payload) as Record<string, unknown>,
    ...(r.via != null ? { via: r.via } : {}),
    ...(r.causedBy != null ? { causedBy: r.causedBy } : {}),
  };
}

/** What an insert did: stored the row, or found its `eventId` already stored. */
export interface Inserted {
  row: JournalRow;
  /** True when the `eventId` was already stored; `row` is then the stored one. */
  duplicate: boolean;
}

type JournalMetaRow = typeof Schema.journalMeta.$inferSelect;

function isStream(stream: StreamRef) {
  return and(
    eq(Schema.journal.streamType, stream.streamType),
    eq(Schema.journal.streamId, stream.streamId),
  );
}

/** One journal's two tables, on the database it was given. */
export class JournalStore {
  constructor(private readonly db: JournalDb) { }

  /**
   * Insert an already-encoded row; the one place that writes `INSERT INTO journal`.
   * A row whose `eventId` is already stored inserts nothing and returns the stored
   * row, so a retried append is a no-op rather than a second event.
   */
  insertJournalRow = (row: JournalRow): Inserted => {
    const inserted = this.db
      .insert(Schema.journal)
      .values({
        eventId: row.eventId,
        streamType: row.streamType,
        streamId: row.streamId,
        type: row.type,
        v: row.v,
        ev: row.ev,
        payload: JSON.stringify(row.payload),
        origin: row.origin,
        at: row.at,
        recordedAt: row.recordedAt,
        via: row.via ?? null,
        causedBy: row.causedBy ?? null,
      })
      .onConflictDoNothing({ target: Schema.journal.eventId })
      .returning({ seq: Schema.journal.seq })
      .get();
    if (inserted) return { row: { ...row, seq: inserted.seq }, duplicate: false };
    const stored = this.db
      .select()
      .from(Schema.journal)
      .where(eq(Schema.journal.eventId, row.eventId))
      .get();
    if (!stored) {
      // invariant: the insert only skips on an eventId conflict, so the stored row exists.
      throw new Error(`journal insert of ${row.eventId} stored nothing and found nothing`);
    }
    const duplicate = fromTableRow(stored);
    if (duplicate.streamType !== row.streamType || duplicate.streamId !== row.streamId || duplicate.type !== row.type) {
      // edge: an eventId reused for another stream or type is a caller bug; answering with the
      // other stream's row would store nothing and report success.
      throw new Error(
        `eventId ${row.eventId} is already ${duplicate.type} on ${duplicate.streamType}/${duplicate.streamId}; ` +
          `refusing it for ${row.type} on ${row.streamType}/${row.streamId}`,
      );
    }
    return { row: duplicate, duplicate: true };
  };

  getJournalMeta = (stream: StreamRef): JournalMetaRow | null => {
    const row = this.db
      .select()
      .from(Schema.journalMeta)
      .where(
        and(
          eq(Schema.journalMeta.streamType, stream.streamType),
          eq(Schema.journalMeta.streamId, stream.streamId),
        ),
      )
      .get();
    return row ?? null;
  };

  upsertJournalMeta = (
    stream: StreamRef,
    meta: {
      projectionVersions?: string;
      appliedThroughSeq?: number;
      rebuiltAt?: string;
    },
  ) => {
    // The INSERT side needs real values for NOT NULL columns (0 means "no
    // version"; a NULL aborts the statement before a conflict routes to DO
    // UPDATE); the UPDATE side must let an omitted field keep the stored value.
    this.db
      .insert(Schema.journalMeta)
      .values({
        streamType: stream.streamType,
        streamId: stream.streamId,
        projectionVersions: meta.projectionVersions ?? null,
        appliedThroughSeq: meta.appliedThroughSeq ?? 0,
        rebuiltAt: meta.rebuiltAt ?? null,
      })
      .onConflictDoUpdate({
        target: [Schema.journalMeta.streamType, Schema.journalMeta.streamId],
        set: {
          projectionVersions: sql`COALESCE(${sql.raw("excluded.projectionVersions")}, ${Schema.journalMeta.projectionVersions})`,
          appliedThroughSeq:
            meta.appliedThroughSeq != null
              ? sql.raw("excluded.appliedThroughSeq")
              : sql`${Schema.journalMeta.appliedThroughSeq}`,
          rebuiltAt: sql`COALESCE(${sql.raw("excluded.rebuiltAt")}, ${Schema.journalMeta.rebuiltAt})`,
        },
      })
      .run();
  };

  /**
   * Raw rows for one stream, in arrival order. Synchronous, for a caller inside a
   * transaction. `types` narrows it to the event types a fold reads.
   */
  streamRowsSync = (
    stream: StreamRef,
    opts: { types?: readonly string[] } = {},
  ): JournalRow[] => {
    const types = opts.types;
    return this.db
      .select()
      .from(Schema.journal)
      .where(
        types && types.length > 0
          ? and(isStream(stream), inArray(Schema.journal.type, [...types]))
          : isStream(stream),
      )
      .orderBy(asc(Schema.journal.seq))
      .all()
      .map(fromTableRow);
  };

  /** Raw rows for one stream, in arrival order. */
  readStreamRows = async (stream: StreamRef): Promise<JournalRow[]> => {
    return this.streamRowsSync(stream);
  };

  /** How many rows one stream holds. */
  countStream = (stream: StreamRef): number => {
    return this.db
      .select({ c: sql<number>`count(*)` })
      .from(Schema.journal)
      .where(isStream(stream))
      .get()!.c;
  };

  /**
   * Per-stream row counts, every stream type unless one is named; aggregated here
   * because the caller only needs counts and a `GROUP BY` beats a full table scan
   * across the RPC boundary.
   */
  journalStreamCounts = async (
    streamType?: string,
  ): Promise<Array<StreamRef & { rows: number }>> => {
    const select = this.db
      .select({
        streamType: Schema.journal.streamType,
        streamId: Schema.journal.streamId,
        rows: sql<number>`count(*)`,
      })
      .from(Schema.journal);
    const filtered =
      streamType === undefined ? select : select.where(eq(Schema.journal.streamType, streamType));
    return filtered.groupBy(Schema.journal.streamType, Schema.journal.streamId).all();
  };

  /** Drop every row of both tables. */
  clearAll = () => {
    this.db.transaction((tx) => {
      tx.delete(Schema.journal).run();
      tx.delete(Schema.journalMeta).run();
    });
  };
}

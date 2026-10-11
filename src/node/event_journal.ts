// One journal: an event log on a database, the vocabulary it validates against,
// and the projections bound to it. Everything a journal needs arrives in
// `createJournal`: the database, the log sink, the clock and the id source. It
// reads no global, so two journals (an application's and a test's) never share
// a catalogue or a slice.
import { randomUUID } from "node:crypto";
import { Fail, Ok, Result } from "@vyredo/typed-result";
import type { ReturnResult } from "@vyredo/typed-result";

import * as Codec from "../shared/codec";
import * as Decider from "../shared/decider";
import type { Clock, Ids } from "../shared/determinism";
import { EventCatalogue } from "../shared/event/catalogue";
import type { JournalRow, NewEvent, StreamRef } from "../shared/event/envelope";
import * as ErrorMessage from "../shared/errors";
import * as Jsonl from "../shared/jsonl";
import * as Redact from "../shared/redact";
import * as JournalLog from "./log";
import { ProjectionRunner } from "./projection";
import type { Appended } from "./projection";
import { JournalStore } from "./store";
import type { JournalDb } from "./store";

/** The real clock, for a journal created without one. */
export const systemClock: Clock = { now: () => new Date().toISOString() };

/** The real event-id source, for a journal created without one. */
export const systemIds: Ids = { uuid: () => randomUUID() };

export interface JournalDeps {
  /** A synchronous drizzle SQLite handle whose migrations include the journal's tables. */
  db: JournalDb;
  /** Where the journal's lines go; the console when absent. */
  log?: JournalLog.JournalLogFactory;
  /** The append clock that `at` and `recordedAt` default to. */
  clock?: Clock;
  /** Mints an `eventId` for an append whose caller passed none. */
  ids?: Ids;
}

/** What an import stored, and which streams it re-folded. */
export interface ImportResult {
  /** Rows stored; a row whose `eventId` was already here is not counted. */
  inserted: number;
  streams: StreamRef[];
  rebuilt: StreamRef[];
}

export class EventJournal {
  /** The event types this journal accepts. Empty until modules register theirs. */
  readonly catalogue = new EventCatalogue();
  /** The `journal` and `journal_meta` tables. */
  readonly store: JournalStore;
  /** The slices bound to this journal, and how they run. */
  readonly projections: ProjectionRunner;
  /** The clock appends and rebuilds stamp with unless a caller passes its own. */
  readonly clock: Clock;
  private readonly ids: Ids;
  private readonly db: JournalDb;
  private readonly log: JournalLog.JournalLogger;

  constructor(deps: JournalDeps) {
    const logFactory = deps.log ?? JournalLog.consoleLog;
    this.db = deps.db;
    this.clock = deps.clock ?? systemClock;
    this.ids = deps.ids ?? systemIds;
    this.log = logFactory("Journal");
    this.store = new JournalStore(deps.db);
    this.projections = new ProjectionRunner({
      db: deps.db,
      store: this.store,
      catalogue: this.catalogue,
      log: logFactory("JournalProjection"),
    });
  }

  /**
   * Append one event inside the caller's transaction: validate, store the row,
   * then run its type's live rule in a savepoint (`ProjectionRunner.applyLive`).
   *
   * A storage failure SQLite rolls back whole (a full disk, an I/O error) is
   * not contained: the caller's commit fails and the row is gone, which is
   * right, since that is not a fold bug. A malformed payload is not a fact: it
   * is refused and nothing is stored. A duplicate `eventId` returns the stored
   * row without re-running the rule.
   */
  appendInTransaction = (input: NewEvent, clock: Clock = this.clock): ReturnResult<Appended> => {
    const encoded = Codec.encodeEvent(input, { now: clock.now(), eventId: this.ids.uuid() }, this.catalogue);
    if (!encoded.isOk) return Fail(encoded.error.message);

    const inserted = Result.wrap(() => this.store.insertJournalRow(encoded.value));
    if (!inserted.isOk) return Fail(inserted.error.message);
    const { row, duplicate } = inserted.value;
    return Ok({ row, projection: this.projections.applyLive(row, duplicate) });
  };

  /**
   * Append one event in a transaction of its own; what almost every caller wants.
   * See `appendInTransaction` for what is kept when the fold fails.
   */
  append = (input: NewEvent, clock: Clock = this.clock): ReturnResult<Appended> => {
    // `Result.wrap` flattens the inner result: a refused payload and a failed
    // commit both arrive as one failure.
    return Result.wrap(() => this.db.transaction(() => this.appendInTransaction(input, clock))).mapErr(
      (err) => new Error(`journal append of ${input.type} failed: ${err.message}`),
    );
  };

  /**
   * Run one command against one stream: read the stream, fold it with `evolve`
   * from `initialState`, `decide`, and append what was decided. One synchronous
   * transaction, so nothing can append to the stream between the read and the
   * append: the state a decision was made on is the state it lands on. `[]`
   * comes back when the decider decided nothing: a retried command, or one the
   * state makes moot.
   *
   * The decided events are all-or-nothing: one refused append refuses the
   * command and stores none of them.
   */
  handle = <Command, State, Event extends NewEvent>(
    stream: StreamRef,
    decider: Decider.Decider<Command, State, Event>,
    command: Command,
    clock: Clock = this.clock,
  ): ReturnResult<Appended[]> => {
    const run = (): ReturnResult<Appended[]> => {
      const rows = this.store.streamRowsSync(stream, { types: decider.eventTypes });
      const { events, errors } = Codec.decodeRows(rows, this.catalogue);
      // Decide on what decoded, as a rebuild folds what decoded: the state is the
      // one the projection shows, and refusing here would refuse a stop.
      if (errors.length > 0) this.log.error("decide: stream rows did not decode", { ...stream, errors });
      const state = Decider.fold(decider, Codec.inFoldOrder(events));

      const decided = decider.decide(command, state);
      if (!decided.isOk) return Fail(decided.error.message);

      const appended: Appended[] = [];
      for (const event of decided.value) {
        const one = this.appendInTransaction({ ...event, ...stream }, clock);
        // edge: one refused event refuses the command; the throw rolls back the ones appended before it.
        if (!one.isOk) throw new Error(one.error.message);
        appended.push(one.value);
      }
      return Ok(appended);
    };
    // `Result.wrap` flattens the inner result: a refused decision and a refused
    // append both arrive as one failure.
    return Result.wrap(() => this.db.transaction(run)).mapErr(
      (err) => new Error(`${stream.streamType}/${stream.streamId}: command refused: ${err.message}`),
    );
  };

  /** One stream's events, decoded and in fold order, with the rows that did not decode. */
  readStream = (stream: StreamRef): { events: Codec.JournalEvent[]; errors: string[] } => {
    const { events, errors } = Codec.decodeRows(this.store.streamRowsSync(stream), this.catalogue);
    return { events: Codec.inFoldOrder(events), errors };
  };

  /**
   * Rebuild one stream's projections from its log: every slice that folds the
   * stream's type, or the named subset, so a version bump in one domain costs
   * only that domain. False when the stream has no rows.
   *
   * The whole rebuild is one transaction. Every domain drops the stream's rows
   * before reinserting the fold's, so a failure partway through would otherwise
   * leave the dropped half gone for good while the boot sweep logged it and
   * moved on. This is the one place that opens that transaction.
   */
  rebuildStream = (stream: StreamRef, opts: { domains?: string[]; clock?: Clock } = {}): boolean => {
    return this.db.transaction(() => this.projections.rebuildDomains(stream, opts.clock ?? this.clock, opts));
  };

  /**
   * The boot re-fold: every stream is compared against the fold versions of the
   * slices that fold its type, and only the stale domains are rebuilt. Absent
   * meta, or no readable version map, rebuilds the stream whole.
   */
  rebuildStaleStreams = async (): Promise<number> => {
    const streams = await this.store.journalStreamCounts();
    let rebuilt = 0;
    for (const { streamType, streamId } of streams) {
      const stream = { streamType, streamId };
      const meta = this.store.getJournalMeta(stream);
      const stale = meta ? this.projections.staleDomains(meta, streamType) : null;
      if (stale !== null && stale.length === 0) continue;
      Result.wrap(() => {
        this.rebuildStream(stream, stale === null ? {} : { domains: stale });
        rebuilt++;
        this.log.info("rebuilt stale projections", { streamType, streamId, domains: stale ?? "all" });
      }).onFailure((err) => {
        this.log.error("projection rebuild failed", { streamType, streamId, err: ErrorMessage.errorMessage(err) });
      });
    }
    if (rebuilt > 0) this.log.info("projection rebuild sweep complete", { rebuilt });
    return rebuilt;
  };

  /**
   * One stream's log as JSONL, the export format; empty text for a stream with
   * no rows. `redact` pseudonymises each event's declared personal fields, and
   * is what any bug-report path uses.
   */
  toJsonl = (stream: StreamRef, opts: { redact?: boolean } = {}): { text: string; rows: number } => {
    const rows = this.store.streamRowsSync(stream);
    if (rows.length === 0) return { text: "", rows: 0 };
    return { text: Jsonl.toJsonl(opts.redact ? Redact.redactRows(rows, this.catalogue) : rows), rows: rows.length };
  };

  /**
   * Read a JSONL log back into the journal; every row is decoded first and the
   * import is refused outright if any row fails, since a partial import is an
   * invisible hole. `seq` is dropped and reallocated, because ordering survives
   * by `at`. A row whose `eventId` is already stored is skipped, so importing
   * the same text twice stores it once. Each imported stream is then rebuilt; a
   * failed rebuild is logged and does not roll back the log, the authority.
   */
  importJsonl = (text: string): ReturnResult<ImportResult> => {
    const { rows, errors } = Jsonl.fromJsonl(text);
    if (errors.length > 0) return Fail(`journal import rejected: ${errors.slice(0, 5).join("; ")}`);
    if (rows.length === 0) return Ok<ImportResult>({ inserted: 0, streams: [], rebuilt: [] });

    const decoded = Codec.decodeRows(rows, this.catalogue);
    if (decoded.errors.length > 0) {
      return Fail(`journal import rejected: ${decoded.errors.slice(0, 5).join("; ")}`);
    }

    const toInsert: JournalRow[] = rows.map(({ seq: _seq, ...rest }) => rest);
    const streams = [
      ...new Map(
        toInsert.map((r) => [`${r.streamType}\u0000${r.streamId}`, { streamType: r.streamType, streamId: r.streamId }]),
      ).values(),
    ];

    return Result.wrap(() => {
      const inserted = this.db.transaction(() =>
        toInsert.reduce((count, row) => count + (this.store.insertJournalRow(row).duplicate ? 0 : 1), 0),
      );
      const rebuilt: StreamRef[] = [];
      for (const stream of streams) {
        Result.wrap(() => {
          if (this.rebuildStream(stream)) rebuilt.push(stream);
        }).onFailure((err) => {
          this.log.error("import rebuild failed", { ...stream, err: ErrorMessage.errorMessage(err) });
        });
      }
      return Ok({ inserted, streams, rebuilt });
    }).mapErr((err) => new Error(`journal import failed: ${ErrorMessage.errorMessage(err)}`));
  };

  /** Forget every registered type, slice and live rule; the rows stay. */
  cleanup = () => {
    this.projections.cleanup();
    this.catalogue.cleanup();
  };
}

/** A journal on `db`. Register event types and modules on it before appending. */
export function createJournal(deps: JournalDeps): EventJournal {
  return new EventJournal(deps);
}

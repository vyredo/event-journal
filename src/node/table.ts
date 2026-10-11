// Journal tables: `journal` (the authoritative log of every stream) and
// `journal_meta` (per-stream projection metadata); `drizzle-kit` derives
// migrations from them.
import {
  index,
  integer,
  primaryKey,
  sqliteTable,
  text,
  uniqueIndex,
} from "drizzle-orm/sqlite-core";

export const journal = sqliteTable(
  "journal",
  {
    seq: integer("seq").primaryKey({ autoIncrement: true }),
    /** One event's identity; unique, so a retried append stores nothing twice. */
    eventId: text("eventId").notNull(),
    /** Which log the event is in: a stream type a module declared, and one id of it. */
    streamType: text("streamType").notNull(),
    streamId: text("streamId").notNull(),
    type: text("type").notNull(),
    v: integer("v").notNull(),
    ev: integer("ev").notNull(),
    payload: text("payload").notNull(),
    origin: text("origin").notNull(),
    at: text("at").notNull(),
    recordedAt: text("recordedAt").notNull(),
    via: text("via"),
    causedBy: integer("causedBy"),
  },
  (t) => [
    uniqueIndex("journal_eventId").on(t.eventId),
    index("journal_stream_seq").on(t.streamType, t.streamId, t.seq),
    index("journal_stream_type").on(t.streamType, t.streamId, t.type),
  ],
);

/**
 * Per-stream projection metadata: version, appliedThrough seq, rebuild timestamp;
 * the version map lets a boot rebuild target only the stale domains.
 */
export const journalMeta = sqliteTable(
  "journal_meta",
  {
    streamType: text("streamType").notNull(),
    streamId: text("streamId").notNull(),
    projectionVersions: text("projectionVersions"),
    appliedThroughSeq: integer("appliedThroughSeq").notNull().default(0),
    rebuiltAt: text("rebuiltAt"),
  },
  (t) => [primaryKey({ columns: [t.streamType, t.streamId] })],
);

// Rows against the log, for any module defined with `defineEventModule`: what
// the module's own writer would have written from the stream's events, compared
// column by column with the rows its table holds. Pure: the caller reads the
// rows. An empty list is the entry condition for trusting, or rebuilding from,
// the table.
import type { JournalEvent } from "./codec";
import type * as EventConfig from "./event/event_config";

type Row = Record<string, unknown>;

/** Every column `expected` names, compared with `actual` by value: a JSON column compares as its JSON. */
function columnDiffs(label: string, expected: Row, actual: Row | undefined): string[] {
  if (!actual) return [`${label}: no row, but the journal writes one`];
  const out: string[] = [];
  for (const [column, value] of Object.entries(expected)) {
    if (value === undefined) continue;
    const got = JSON.stringify(actual[column] ?? null);
    const want = JSON.stringify(value ?? null);
    if (got !== want) out.push(`${label}: ${column} row=${got} journal=${want}`);
  }
  return out;
}

/**
 * Disagreements between one stream's events (in fold order) and its rows in the
 * module's table, the rows already narrowed to that stream. Columns the events
 * never write (a flag owned elsewhere, an autoincrement id) are not compared,
 * and a row the log never names is not reported: rows from before the journal
 * are expected.
 */
export function reconcile(
  domain: string,
  write: EventConfig.TableWrite,
  eventsInFoldOrder: readonly JournalEvent[],
  rows: readonly Row[],
): string[] {
  switch (write.shape) {
    case "row": {
      const expected = write.toRow(eventsInFoldOrder);
      return expected ? columnDiffs(domain, expected, rows[0]) : [];
    }
    case "keyed": {
      const out: string[] = [];
      for (const [key, expected] of write.toRows(eventsInFoldOrder)) {
        const actual = rows.find((r) => r[write.key] === key);
        if (expected === null) {
          if (actual) out.push(`${domain} ${key}: row present, but the journal dropped it`);
        } else {
          out.push(...columnDiffs(`${domain} ${key}`, expected, actual));
        }
      }
      return out;
    }
    case "append": {
      // Replay the writer in memory: each row replaces the one its supersede columns name.
      const keyOf = (r: Row) => JSON.stringify(write.supersede.map((c) => r[c] ?? null));
      const expected = new Map<string, Row>();
      for (const event of eventsInFoldOrder) {
        for (const row of write.rowsOf(event)) expected.set(keyOf(row), row);
      }
      const out: string[] = [];
      if (rows.length !== expected.size) {
        out.push(`${domain}: ${rows.length} rows, the journal writes ${expected.size}`);
      }
      for (const [key, row] of expected) {
        out.push(...columnDiffs(`${domain} ${key}`, row, rows.find((r) => keyOf(r) === key)));
      }
      return out;
    }
  }
}

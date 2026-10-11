// Pseudonymise the personal data in a log before it leaves the machine; the
// redacted exporter mode is what any bug-report path uses. Which fields are
// personal is the events' to say: each declares them (`personal`), and the
// catalogue carries the list, so the journal knows no field by name.
import * as Determinism from "./determinism";
import type { Catalogue } from "./event/contract";
import type { JournalRow } from "./event/envelope";

/**
 * A stable stand-in for a value, derived from a stable id so the same person
 * reads as the same pseudonym across every event of one stream.
 */
export function pseudonym(id: string): string {
  return `p-${Determinism.fnv1a(id)}`;
}

/**
 * A copy of the row with its type's personal fields replaced; everything else
 * passes through. An allowlist declared per event, not a scan for name-shaped
 * strings: a scan would quietly stop covering a new field and leak it.
 */
export function redactRow(row: JournalRow, catalogue: Catalogue): JournalRow {
  const fields = (catalogue.personal[catalogue.renamed?.[row.type] ?? row.type] ?? []).filter((f) => typeof row.payload[f] === "string");
  if (fields.length === 0) return row;
  const payload = { ...row.payload };
  // Keyed by stream and field: one stream's person keeps one pseudonym per
  // field across its events, and two streams never share one.
  for (const f of fields) payload[f] = pseudonym(`${row.streamType}/${row.streamId}/${f}`);
  return { ...row, payload };
}

/** `redactRow` over a whole log. */
export function redactRows(rows: JournalRow[], catalogue: Catalogue): JournalRow[] {
  return rows.map((row) => redactRow(row, catalogue));
}

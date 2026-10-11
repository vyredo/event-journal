// JSONL is the journal's wire format: one row per line, append-only and diffable.
// Chosen for the failure case: a log truncated mid-write loses its last line and
// stays readable, which no single-JSON-document format manages.
import { Result, type ReturnResult, Fail, Ok } from "@vyredo/typed-result";


import type { JournalRow } from "./event/envelope";

/**
 * Serialize rows to JSONL in `seq` order; the file's line order is the only
 * ordering an importer on another machine can see.
 */
export function toJsonl(rows: JournalRow[]): string {
  const ordered = [...rows].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  return (
    ordered.map((r) => JSON.stringify(r)).join("\n") +
    (ordered.length ? "\n" : "")
  );
}

/**
 * Parse JSONL back into rows, reporting bad lines rather than failing the file;
 * only the envelope's structure is checked here, payloads are `decodeRow`'s job.
 */
export function fromJsonl(text: string): {
  rows: JournalRow[];
  errors: string[];
} {
  const rows: JournalRow[] = [];
  const errors: string[] = [];
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const parsed = parseLine(line, i + 1);
    if (parsed.isOk) rows.push(parsed.value);
    else errors.push(parsed.error.message);
  }
  return { rows, errors };
}

function parseLine(line: string, lineNo: number): ReturnResult<JournalRow> {
  const parsed = Result.wrap(() => JSON.parse(line) as unknown);
  if (!parsed.isOk) return Fail(`line ${lineNo}: not valid JSON`);
  const value: unknown = parsed.value;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return Fail(`line ${lineNo}: not an object`);
  }
  const row = value as Partial<JournalRow>;
  const missing = (
    ["eventId", "streamType", "streamId", "type", "at", "recordedAt", "origin"] as const
  ).filter((k) => typeof row[k] !== "string" || !row[k]);
  if (missing.length > 0) {
    return Fail(`line ${lineNo}: missing ${missing.join(", ")}`);
  }
  if (typeof row.v !== "number" || typeof row.ev !== "number") {
    return Fail(`line ${lineNo}: v and ev must be numbers`);
  }
  if (
    typeof row.payload !== "object" ||
    row.payload === null ||
    Array.isArray(row.payload)
  ) {
    return Fail(`line ${lineNo}: payload must be an object`);
  }
  return Ok(row as JournalRow);
}

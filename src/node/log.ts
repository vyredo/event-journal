// The journal's log sink. The journal brings no logger of its own: whoever
// creates one passes a factory (`createJournal({ log })`), and without one lines
// go to the console, so a failure is still seen.
export interface JournalLogger {
  info(message: string, metadata?: Record<string, unknown>): void;
  warn(message: string, metadata?: Record<string, unknown>): void;
  error(message: string, metadata?: Record<string, unknown>): void;
}

/** Makes the logger for one part of the journal, named like `JournalProjection`. */
export type JournalLogFactory = (component: string) => JournalLogger;

/** The default sink: the console, each line tagged with its component. */
export const consoleLog: JournalLogFactory = (component) => ({
  info: (m, d) => console.info(`[${component}] ${m}`, d ?? ""),
  warn: (m, d) => console.warn(`[${component}] ${m}`, d ?? ""),
  error: (m, d) => console.error(`[${component}] ${m}`, d ?? ""),
});

// An in-memory journal database for the library's own tests, on `node:sqlite`
// (better-sqlite3 is a native module the tests do not need). Test support only:
// not part of the build.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

import { drizzle, type BetterSQLite3Database } from "drizzle-orm/better-sqlite3";

import * as Schema from "./table";

interface SqliteStatement {
  setReturnArrays(on: boolean): void;
  run(...params: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
  close(): void;
}

const nodeRequire = createRequire(import.meta.url);
const { DatabaseSync } = nodeRequire("node:sqlite") as {
  DatabaseSync: new (path: string) => SqliteDatabase;
};

/**
 * better-sqlite3's statement surface over a `node:sqlite` statement. `.raw()`
 * is the only mismatch: the flag is set for one call and reset after.
 */
function compatibleClient(db: SqliteDatabase) {
  return {
    prepare(sql: string) {
      const stmt = db.prepare(sql);
      const raw = {
        all: (...params: unknown[]) => {
          stmt.setReturnArrays(true);
          try {
            return stmt.all(...params);
          } finally {
            stmt.setReturnArrays(false);
          }
        },
        get: (...params: unknown[]) => {
          stmt.setReturnArrays(true);
          try {
            return stmt.get(...params);
          } finally {
            stmt.setReturnArrays(false);
          }
        },
      };
      return {
        run: (...params: unknown[]) => stmt.run(...params),
        all: (...params: unknown[]) => stmt.all(...params),
        get: (...params: unknown[]) => stmt.get(...params),
        raw: () => raw,
      };
    },
    /**
     * better-sqlite3's `db.transaction(fn)` callable with variants. Rollback
     * must happen before the error keeps travelling.
     */
    transaction(fn: (...args: unknown[]) => unknown) {
      const make = (begin: string) => {
        return (...args: unknown[]) => {
          db.exec(begin);
          try {
            const out = fn(...args);
            db.exec("COMMIT");
            return out;
          } catch (err) {
            db.exec("ROLLBACK");
            // fail-safe: the rollback decision is the same for every failure.
            throw err;
          }
        };
      };
      return Object.assign(make("BEGIN"), {
        deferred: make("BEGIN"),
        immediate: make("BEGIN IMMEDIATE"),
        exclusive: make("BEGIN EXCLUSIVE"),
      });
    },
  };
}

const SCHEMA = readFileSync(new URL("./schema.sql", import.meta.url), "utf8");

export interface TestCoreDb {
  db: BetterSQLite3Database<typeof Schema>;
  close(): void;
}

/** A fresh in-memory database holding the journal's tables. */
export function makeTestCoreDb(): TestCoreDb {
  const handle = new DatabaseSync(":memory:");
  handle.exec(SCHEMA);
  return {
    db: drizzle(compatibleClient(handle) as never, { schema: Schema }),
    close: () => handle.close(),
  };
}

let current: TestCoreDb | null = null;

export function setTestCoreDb(db: TestCoreDb): void {
  current?.close();
  current = db;
}

export function testCoreDb(): TestCoreDb {
  // invariant: a suite calls `setTestCoreDb` first.
  if (!current) throw new Error("test db not initialized");
  return current;
}

export function clearTestCoreDb(): void {
  current?.close();
  current = null;
}

/** The current test database, looked up on every access, for a journal a suite creates once. */
export const liveTestDb: TestCoreDb["db"] = new Proxy({} as TestCoreDb["db"], {
  get: (_target, prop) => {
    const instance = testCoreDb().db;
    const value = Reflect.get(instance, prop);
    return typeof value === "function" ? value.bind(instance) : value;
  },
  has: (_target, prop) => prop in testCoreDb().db,
});

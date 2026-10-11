// The clock and the id generator, injected rather than ambient: no `new Date()`,
// `Date.now()`, `Math.random()` or `crypto.randomUUID()` in any module the reducer
// imports. `scripts/checks/check-determinism.ts` enforces it lexically.

/** Wall clock, as ISO-8601. */
export interface Clock {
  now(): string;
}

/** Identifier minting. */
export interface Ids {
  uuid(): string;
}

/**
 * A clock frozen at one instant; replay drives it with the current event's `at`.
 */
export function fixedClock(at: string): Clock {
  return { now: () => at };
}

/**
 * A clock that walks a prepared list of instants, one per call, holding the last;
 * an extra call must not fail the replay.
 */
export function scriptedClock(instants: string[]): Clock {
  if (instants.length === 0) {
    // invariant: a replay always drives the clock from event.at; an empty script is a wiring bug.
    throw new Error("scriptedClock needs at least one instant");
  }
  let i = 0;
  return {
    now: () => instants[Math.min(i++, instants.length - 1)],
  };
}

/**
 * FNV-1a, 32-bit; stable across runs and machines, which `crypto` is not. Not a
 * security primitive.
 */
export function fnv1a(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    // `Math.imul` for a real 32-bit multiply; `h * 16777619` loses precision past 2^53.
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

/**
 * Ids derived from a seed so a replay mints the same ones; filesystem-safe, and
 * deliberately not UUID-shaped to avoid looking like a random id.
 */
export function derivedIds(seed: string): Ids {
  let counter = 0;
  return {
    uuid: () => `det-${fnv1a(seed)}-${(counter++).toString(36)}`,
  };
}

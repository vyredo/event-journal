// Read-time payload migrations: a catalogue's `upcasters[type][fromV]` produces
// version `fromV + 1`, and `upcastPayload` chains them to `currentV`. Upcasters must be
// pure, permanent, and never invent data.
import { Fail, Ok } from "@vyredo/typed-result";
import type { ReturnResult } from "@vyredo/typed-result";

import type { Catalogue } from "./contract";

/**
 * Walk a stored payload up to the catalogue's current version; fails rather than
 * guesses on a ladder gap or a version from the future.
 */
export function upcastPayload(
  type: string,
  fromV: number,
  payload: Record<string, unknown>,
  catalogue: Catalogue,
): ReturnResult<{ v: number; payload: Record<string, unknown> }> {
  const target = catalogue.currentV[type];
  if (target === undefined) {
    return Fail(`unknown event type "${type}"`);
  }
  if (!Number.isInteger(fromV) || fromV < 1) {
    return Fail(
      `${type}: stored v must be a positive integer, got ${fromV}`,
    );
  }
  if (fromV > target) {
    // A newer build wrote this; guessing would produce a plausible-looking wrong projection.
    return Fail(
      `${type}: stored v${fromV} is newer than this build's v${target}  -  update the app rather than reading it`,
    );
  }

  const ladder = catalogue.upcasters[type] ?? {};
  let current = payload;
  for (let v = fromV; v < target; v++) {
    const step = ladder[v];
    if (!step) {
      return Fail(
        `${type}: no upcaster from v${v} to v${v + 1}  -  the ladder has a gap`,
      );
    }
    current = step(current);
  }
  return Ok({ v: target, payload: current });
}

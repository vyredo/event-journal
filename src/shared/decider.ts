// The shape every event-sourced module takes, in Emmett's words: `initialState`
// is a stream with no events, `evolve` applies one stored event to the state,
// and `decide` says what a command produces given that state. `handle`
// (`EventJournal.handle`, in `node/event_journal.ts`) is the one loop that runs
// them: read the stream, fold it, decide, append. Pure tier: no clock, no ids, no storage.
import type { ReturnResult } from "@vyredo/typed-result";

import type { JournalEvent } from "./codec";
import type { NewEvent } from "./event/envelope";

/**
 * The state of a stream and how one event moves it. A projection that takes no
 * commands is only this half; `fold` runs it over a stream.
 */
export interface Evolver<State> {
  /** The state of a stream with no events yet. */
  initialState: () => State;
  /**
   * One stored event applied to the state. Pure, and returns a new state rather
   * than changing the one it was given. Ignores event types it does not own.
   */
  evolve: (state: State, event: JournalEvent) => State;
}

/**
 * An event as `decide` states it: everything but the stream, which `handle`
 * names, so a decider never learns where its events are stored. Distributes
 * over a union, so each event keeps its own `type`, `v` and payload.
 */
export type Decided<E extends NewEvent> = E extends NewEvent
  ? Omit<E, "streamType" | "streamId">
  : never;

/** A module's write side: its state, its fold, and the decisions it makes. */
export interface Decider<Command, State, Event extends NewEvent> extends Evolver<State> {
  /**
   * What a command produces, given the state folded from the stream. `[]` is a
   * no-op, which is how a retried command stays idempotent. A refusal is a
   * `Fail`, never a throw.
   */
  decide: (command: Command, state: State) => ReturnResult<Decided<Event>[]>;
  /**
   * The event types `evolve` reads. `handle` loads only these rows of the
   * stream, so a decider sharing a stream with a high-volume event type does
   * not decode every one of those to answer a command.
   */
  eventTypes: readonly string[];
}

/** Fold events, already in fold order, from `from` (the initial state unless given). */
export function fold<State>(
  evolver: Evolver<State>,
  events: readonly JournalEvent[],
  from: State = evolver.initialState(),
): State {
  return events.reduce(evolver.evolve, from);
}

/**
 * Fold several modules over one log at once, each from its own `initialState`:
 * `{ name: evolver }` in, `{ name: state }` out. The applicative form of `fold`,
 * so an aggregate needs no hand-written empty value per module.
 */
export function foldAll<States extends Record<string, unknown>>(
  evolvers: { [K in keyof States]: Evolver<States[K]> },
  events: readonly JournalEvent[],
): States {
  const out: Partial<States> = {};
  for (const name of Object.keys(evolvers) as (keyof States)[]) out[name] = fold(evolvers[name], events);
  return out as States;
}

# `shared/event/`: what a stored event looks like, and how a module declares one

**This is event sourcing.** Everything in here describes a fact that gets written
to the `journal` table and kept forever. The journal is generic: it knows no
stream, no event type, no origin and no personal field of its own. Modules
declare all of them.

How the application uses it (which modules own which events, what it journals
and what it does not) is in `AGENT.md`, "Schemas, events and projections".

## The journal is a store for any stream

Every row belongs to one **stream**, named by `(streamType, streamId)`. A module
declares a kind of stream once, `defineStream("order")`, and addresses one log
with `of(id)`. The stream decides what folds together: a rebuild reads one
stream, `journal_meta` keeps one row per stream, and export, import and
redaction all work a stream at a time.

Two ids, two jobs:

- **The stream id** says which log an event is in. Every append names it, and
  the catalogue refuses an event on a stream its type does not belong to.
- **The event id** (`eventId`) identifies one event, and its only job is
  de-duplication. The append path mints one when the caller passes none; a
  caller passes its own to make a retry idempotent. A unique index enforces it,
  so a second append with the same `eventId` stores nothing and returns the row
  already there, and an `eventId` already stored on another stream or type is
  refused. Never use it to say which stream an event is about.

Each projection slice declares the stream types it folds. A rebuild of a stream
runs exactly the slices that fold its type.

## What is journaled, and what is not

**A log holds facts someone or something stated, never the work state derived
from them.** "An order was placed" is a fact, and a rebuild folds it back into
the tables. A queue row, a lease, a retry count, a timer: those are derived work
state. They live in their own tables, are rebuilt by re-running the work, and
never get an event.

It is deliberately **not** the same "event" as a message on a bus, alive for one
call. The two meet only in that a call is often what _triggers_ an append, and
the envelope keeps a `via` string as the diagnostic link back. No reducer may
read `via`: a projection that branched on which transport served a call would
stop being a function of the log. `origin` (who stated it) is the
application's vocabulary; the journal stores any string.

| File              | What it is |
| ----------------- | ---------- |
| `envelope.ts`     | The stored row (`seq`, `eventId`, `streamType`, `streamId`, `at`, `recordedAt`, `origin`, `payload`, `via`) and `defineStream`. |
| `define.ts`       | `defineJournalEvent`, `EventOf`, `DecodedOf`, `guardFor`: the low-level declaration of one event type. |
| `event_config.ts` | `defineEvent` (one event in one object: schema, `evolve`, `commands`, `project`, `personal`, `previous`), `defineObservation` (the same for a device observation: no `commands`, and `evolve` and `project` required), `retiredEvent`, `defineEventModule`. |
| `ejournal_event.ts` | `EJournalEvent`, the abstract class every command-deciding `<Name>.ejournal.ts` extends: its `Shape` states the event's contract, and at least one command is required. Built on `defineEvent`. |
| `catalogue.ts`    | The registry the codec validates against. **Empty at load**: every module registers its own types. |
| `upcasters.ts`    | The upcast ladder walk. The steps come from each current version's `previous` chain. |
| `sample.ts`       | A maximal payload generated from a schema, for the test suites. |
| `contract.ts`     | `Catalogue`, `PayloadSchema`, `Upcaster`: structural, so a test can inject a synthetic vocabulary. |

The machinery that _uses_ these stays a level up in `shared/`: `codec.ts`
(validate, encode, decode, fold order), `decider.ts` (`Decider`, `Evolver`,
`fold`), `determinism.ts` (the clock and id seams), `jsonl.ts`, `redact.ts`
(pseudonymises each event's declared `personal` fields), `errors.ts`.

## Three rules for editing an event

1. **A stored event is immutable.** Never migrate the log in place; a rewrite
   cannot reach a log already exported to disk. Shape changes are read-time
   upcasts, always.
2. **Adding an optional field does not bump `v`.** The reducer has to tolerate
   its absence anyway; that is what optional means.
3. **Everything else bumps `v`**: a new required field, a removed field, a
   renamed field, a changed unit, or a changed _meaning_ at the same name. The
   last is the dangerous one and the one review has to actually look for.

Bumping `v` versions that one event alone, in its own file: the old version
becomes a frozen `retiredEvent({ type, v, schema, upcast })` constant in the
same `<Name>.ejournal.ts`, and the class moves to `v: n+1` and names it in
`previous`. The ladder
registers itself, an upcast that does not produce the next payload is a compile
error, and every other event stays where it is.

### These rules are enforced, not advisory

The application keeps a schema lock: every registered `type@v`, retired
versions included, as JSON Schema. Changing a released schema fails it, and so
does editing a lock entry the base branch already has. What no check can catch
is rule 3's last clause: a **changed meaning at the same name and the same
type**, such as a value switching from 0-1 to 0-100. That is the case review
has to look for.

## Why this folder is inside `shared/`

`shared/` is the library's pure tier. Three guards key off the path:

- `tsconfig.core.json` compiles it with `lib: ["ES2021", "WebWorker"]` and
  `types: []`, so `window`, `document` and `localStorage` are compile errors.
- `scripts/checks/check-journal-boundary.ts` refuses any import from
  `shared/` into `node/`.
- `scripts/checks/check-determinism.ts` has `libs/event-journal/shared` in
  `PURE_ROOTS`, so `new Date()`, `Date.now()`, `Math.random()` and
  `crypto.randomUUID()` are rejected: a rebuild has to be a pure function of
  the log, so every instant and every id arrives as an argument.

Both are prefix matches on the directory, so this subfolder inherits them.
`upcasters.ts` carries the rules that govern an upcaster.

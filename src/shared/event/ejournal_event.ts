// The base every `<Name>.ejournal.ts` event extends. A subclass states its
// contract once, as the `Shape` in its `extends` clause (type, version, stream,
// payload schema and the data of each command it accepts), and its constructor
// is checked against that contract:
//
//   class RecordingEndedEvent extends EJournalEvent<RecordingModule, {
//     type: "recording.ended"; v: 1; streamType: typeof RecordingStream.type;
//     payload: typeof payload; commands: { RecoverRecording: z.infer<typeof recover> };
//   }> { constructor() { super({ ... }) } }
//   export const RecordingEnded = new RecordingEndedEvent();
//
// Every event is produced by at least one command, and every command is a
// schema and a `decide`. `evolve` and `project` stay optional: an event may
// move no state or write no column. The behaviour is `defineEvent`'s; this
// class only makes the contract explicit and the commands required. Pure tier.
import * as EventConfig from "./event_config";

/** What a subclass declares in its `extends` clause. */
export interface Shape {
  type: string;
  v: number;
  streamType: string;
  payload: EventConfig.PayloadSchema;
  /** Command name to the type of its data, usually `z.infer<typeof schema>`. */
  commands: EventConfig.CommandData;
}

type EventOf<S extends Shape> = EventConfig.EventFor<S["type"], S["v"], S["streamType"], S["payload"]>;
type Def<M extends EventConfig.AnyModule, S extends Shape> = EventConfig.EventDef<M, EventOf<S>, S["commands"]>;
type FullSpec<M extends EventConfig.AnyModule, S extends Shape> =
  EventConfig.EventSpec<M, S["type"], S["v"], S["streamType"], S["payload"], S["commands"]>;

/** The commands, required: each key a schema and a `decide`. */
type Commands<M extends EventConfig.AnyModule, S extends Shape> = {
  readonly [K in keyof S["commands"]]: EventConfig.CommandRule<M["state"], S["commands"][K]>;
};

/** An event with no command has nothing that decides it; its `Shape` must name one. */
type AtLeastOneCommand<S extends Shape> = [keyof S["commands"]] extends [never] ? { commands: never } : unknown;

/** What a subclass hands its constructor: the `Shape`'s values, its rules, and `commands` required. */
export type Spec<M extends EventConfig.AnyModule, S extends Shape> =
  Omit<FullSpec<M, S>, "commands"> & { commands: Commands<M, S> } & AtLeastOneCommand<S>;

export abstract class EJournalEvent<M extends EventConfig.AnyModule, S extends Shape> implements Def<M, S> {
  readonly type: Def<M, S>["type"];
  readonly registration: Def<M, S>["registration"];
  readonly affectsTable: boolean;
  /** Type only: the commands' data, so the module can name its command union. */
  declare readonly commandData?: S["commands"];
  readonly command: Def<M, S>["command"];
  readonly make: Def<M, S>["make"];
  readonly evolve: Def<M, S>["evolve"];
  readonly decide: Def<M, S>["decide"];
  readonly project: Def<M, S>["project"];

  protected constructor(spec: Spec<M, S>) {
    const def = EventConfig.defineEvent<M>()<S["type"], S["v"], S["streamType"], S["payload"], S["commands"]>(spec);
    this.type = def.type;
    this.registration = def.registration;
    this.affectsTable = def.affectsTable;
    this.command = def.command;
    this.make = def.make;
    this.evolve = def.evolve;
    this.decide = def.decide;
    this.project = def.project;
  }
}

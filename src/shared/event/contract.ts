// The three interfaces the codec talks to, a leaf so `catalogue.ts`,
// `upcasters.ts` and `codec.ts` can import them without a cycle.

/** Whatever `catalogue.ts` uses to validate one payload. zod satisfies this. */
export interface PayloadSchema {
  safeParse(
    value: unknown,
  ):
    | { success: true; data: unknown }
    | { success: false; error: { message?: string } };
}

/**
 * Turn a version-`N` payload into version `N+1`; pure and dependency-free, and
 * permanent, since exported logs can always come back with an old version.
 */
export type Upcaster = (
  payload: Record<string, unknown>,
) => Record<string, unknown>;

/**
 * Everything the codec needs to know about the event vocabulary; injectable so the
 * upcast chain can be exercised against a synthetic v1->v2->v3 ladder.
 */
export interface Catalogue {
  /** `type` -> the version this build writes and reducers expect. */
  readonly currentV: Readonly<Record<string, number>>;
  /**
   * `type` -> the stream type it is appended to. An event on the wrong stream is
   * refused at append, since a fold for that stream would never see it.
   */
  readonly streamTypes: Readonly<Record<string, string>>;
  /** `type` -> `v` -> schema. Historical versions included, for the fixture corpus. */
  readonly schemas: Readonly<
    Record<string, Readonly<Record<number, PayloadSchema>>>
  >;
  /** `type` -> `fromV` -> upcaster producing `fromV + 1`. */
  readonly upcasters: Readonly<
    Record<string, Readonly<Record<number, Upcaster>>>
  >;
  /** `type` -> the payload fields that carry personal data, which a redacted export pseudonymises. */
  readonly personal: Readonly<Record<string, readonly string[]>>;
  /**
   * A renamed type's former name -> its current name. A stored row of the former
   * name reads as the current one; nothing appends a former name. Optional so a
   * synthetic catalogue in a test need not spell it out.
   */
  readonly renamed?: Readonly<Record<string, string>>;
}

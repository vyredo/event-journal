// Generated samples replace hand-written fixtures, so they must be maximal (an
// optional field left out is a fold path no suite exercises) and must fit the
// schema's own bounds.
import { describe, expect, it } from "vitest";
import { z } from "zod";

import * as Sample from "./sample";

describe("sampleOf", () => {
  const schema = z.object({
    id: z.string().min(3),
    ratio: z.number().min(0).max(1).optional(),
    count: z.number().int().positive(),
    kind: z.enum(["a", "b"]),
    note: z.string().nullable(),
    tags: z.array(z.string()).min(2).max(5),
    extra: z.record(z.string(), z.unknown()),
    nested: z.object({ flag: z.boolean().optional() }),
  });

  it("fills every optional field and every array, within the schema's bounds", () => {
    const sample = Sample.sampleOf(schema);
    expect(schema.safeParse(sample).success).toBe(true);
    expect(sample).toMatchObject({
      ratio: expect.any(Number),
      count: expect.any(Number),
      kind: "a",
      note: expect.any(String),
      tags: [expect.any(String), expect.any(String)],
      nested: { flag: true },
    });
    expect(Object.keys(sample.extra as object)).toHaveLength(1);
  });
});

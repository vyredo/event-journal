// A payload generated from an event's schema, for the test suites: maximal,
// so every optional field is present and every array has an item, and within
// the schema's own bounds. It replaces a hand-written fixture; the catalogue
// still checks it against the schema when the event registers. Built from
// zod's public JSON Schema output rather than its internals. Pure tier.
import { z } from "zod";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type Node = Record<string, unknown>;

const isNode = (v: unknown): v is Node => typeof v === "object" && v !== null && !Array.isArray(v);
const num = (v: unknown): number | undefined => (typeof v === "number" ? v : undefined);

function numberOf(node: Node): number {
  const min = num(node.minimum) ?? (num(node.exclusiveMinimum) !== undefined ? num(node.exclusiveMinimum)! + 1 : undefined);
  const max = num(node.maximum) ?? (num(node.exclusiveMaximum) !== undefined ? num(node.exclusiveMaximum)! - 1 : undefined);
  const value = min ?? (max !== undefined ? Math.min(1, max) : 1);
  return node.type === "integer" ? Math.ceil(value) : value;
}

function valueOf(node: unknown): Json {
  if (!isNode(node)) return "sample";
  if (node.const !== undefined) return node.const as Json;
  if (Array.isArray(node.enum) && node.enum.length > 0) return node.enum[0] as Json;
  // `nullable` and unions: the first branch that is not null.
  const branches = Array.isArray(node.anyOf) ? node.anyOf : Array.isArray(node.oneOf) ? node.oneOf : null;
  if (branches) return valueOf(branches.find((b) => !(isNode(b) && b.type === "null")) ?? branches[0]);
  switch (node.type) {
    case "string":
      return "s".repeat(Math.max(num(node.minLength) ?? 1, 1));
    case "number":
    case "integer":
      return numberOf(node);
    case "boolean":
      return true;
    case "null":
      return null;
    case "array":
      return Array.from({ length: Math.max(num(node.minItems) ?? 1, 1) }, () => valueOf(node.items));
    case "object": {
      const out: { [key: string]: Json } = {};
      const props = isNode(node.properties) ? node.properties : {};
      for (const [key, child] of Object.entries(props)) out[key] = valueOf(child);
      // A record: one entry, so a fold over it has something to read.
      if (Object.keys(props).length === 0 && node.additionalProperties !== undefined && node.additionalProperties !== false) {
        out.key = valueOf(node.additionalProperties);
      }
      return out;
    }
    default:
      // `unknown` and anything unconstrained.
      return "sample";
  }
}

/** A maximal sample of `schema`. */
export function sampleOf(schema: z.ZodType): Record<string, unknown> {
  const sample = valueOf(z.toJSONSchema(schema, { unrepresentable: "any" }));
  return isNode(sample) ? sample : {};
}

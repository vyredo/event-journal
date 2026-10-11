// The message to log for a caught value, inside the journal; `catch` binds
// `unknown`. Pure tier.
import { Result } from "@vyredo/typed-result";

export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  // Circular or throwing `toJSON`: fall back rather than fail a log line.
  return Result.wrap(() => JSON.stringify(err) ?? String(err)).unwrapOrElse(() => String(err));
}

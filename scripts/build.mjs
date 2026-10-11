// tsc to dist/, then give relative specifiers their `.js` so Node's ESM loader
// resolves them (the sources import extensionless, as bundlers allow).
import { execFileSync } from "node:child_process";
import { copyFileSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

rmSync("dist", { recursive: true, force: true });
execFileSync(process.execPath, ["node_modules/typescript/bin/tsc", "-p", "tsconfig.build.json"], { stdio: "inherit" });

const walk = (dir) => readdirSync(dir).flatMap((f) => (statSync(join(dir, f)).isDirectory() ? walk(join(dir, f)) : [join(dir, f)]));
for (const file of walk("dist").filter((f) => /\.(js|d\.ts)$/.test(f))) {
  const text = readFileSync(file, "utf8");
  const fixed = text.replace(/(from\s+|import\s*\(\s*)(["'])(\.{1,2}\/[^"']+?)\2/g, (m, pre, q, spec) =>
    /\.(js|json)$/.test(spec) ? m : `${pre}${q}${spec}.js${q}`,
  );
  if (fixed !== text) writeFileSync(file, fixed);
}
copyFileSync("src/node/schema.sql", "dist/node/schema.sql");

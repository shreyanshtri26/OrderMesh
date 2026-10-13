// Deploys the storefront page to Vercel with /catalog and /orders proxied to the
// hosted Order API. Same-origin requests mean no CORS, and only these three paths
// are forwarded, so /admin/dlq is never reachable through the site.
//
//   API_URL=https://ordermesh.onrender.com node scripts/deploy-site.mjs
//   API_URL=https://ordermesh.onrender.com node scripts/deploy-site.mjs --dry-run
//
// Without API_URL the page is deployed on its own and runs in sample-data mode.
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const page = fileURLToPath(new URL("../services/order/public/index.html", import.meta.url));
const apiUrl = process.env.API_URL?.replace(/\/+$/, "");
const dryRun = process.argv.includes("--dry-run");
const scope = process.env.VERCEL_SCOPE ?? "shreyansh-tripathis-projects";
const project = process.env.VERCEL_PROJECT ?? "ordermesh-ai";

if (apiUrl && !/^https:\/\/[^/\s]+$/.test(apiUrl)) {
  console.error(`API_URL must be an https origin with no path, got: ${apiUrl}`);
  process.exit(1);
}

const dir = mkdtempSync(join(tmpdir(), "ordermesh-site-"));
copyFileSync(page, join(dir, "index.html"));
if (apiUrl) {
  const rewrites = ["/catalog", "/orders", "/orders/:id"].map((source) => ({
    source,
    destination: `${apiUrl}${source}`,
  }));
  writeFileSync(join(dir, "vercel.json"), JSON.stringify({ rewrites }, null, 2));
}

console.log(`staged ${dir} (${apiUrl ? `proxying to ${apiUrl}` : "no API_URL: sample-data mode"})`);
if (dryRun) process.exit(0);

execFileSync("vercel", ["deploy", dir, "--prod", "--yes", "--scope", scope, "--name", project], {
  stdio: "inherit",
  shell: process.platform === "win32",
});

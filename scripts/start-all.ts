import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";

// Runs all four services in ONE container, for hosts whose free tier allows a
// single web service (Render). Each service is still a separate process with
// its own database and Kafka consumer group; only the deployment is shared.
//
// The Order service takes the public $PORT; the others listen internally.
// If any process exits, the rest are stopped and the container exits, so the
// host restarts everything cleanly.

const SERVICES = [
  { name: "order", port: process.env.PORT ?? "3001" },
  { name: "inventory", port: "3002" },
  { name: "payment", port: "3003" },
  { name: "notification", port: "3004" },
];

const root = fileURLToPath(new URL("..", import.meta.url));
const children: ChildProcess[] = [];
let stopping = false;

for (const { name, port } of SERVICES) {
  const { PORT: _ignored, ...env } = process.env;
  const child = spawn(
    process.execPath,
    ["--import", "@opentelemetry/auto-instrumentations-node/register", "src/index.ts"],
    {
      cwd: `${root}services/${name}`,
      env: { ...env, [`${name.toUpperCase()}_PORT`]: port, OTEL_SERVICE_NAME: env.OTEL_SERVICE_NAME ?? name },
      stdio: "inherit",
    },
  );
  child.on("exit", (code, signal) => {
    if (stopping) return;
    console.error(`[start-all] ${name} exited (code=${code}, signal=${signal}); stopping all services`);
    stopAll(1);
  });
  children.push(child);
}

function stopAll(exitCode: number) {
  stopping = true;
  for (const child of children) child.kill("SIGTERM");
  setTimeout(() => process.exit(exitCode), 5_000).unref();
}

process.once("SIGTERM", () => stopAll(0));
process.once("SIGINT", () => stopAll(0));

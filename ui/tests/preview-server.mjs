import { createServer } from "vite";
import { randomUUID } from "node:crypto";
import { BrowserFixture } from "./fixtures/workflow-browser.js";

// This explicit test preview intercepts every API request; it never forwards to production.
const fixture = new BrowserFixture();
fixture.mode = "workflow";
fixture.schedule.name = "[隔离预览] TiZrNbV";
fixture.drafts.push({ id: randomUUID(), name: "[隔离预览] 材料并行流程", revision: 1, graph: fixture.graph });

const server = await createServer({
  cacheDir: "node_modules/.vite-workflow-preview",
  server: { host: "127.0.0.1", port: Number(process.env.WORKFLOW_PREVIEW_PORT || 5175), strictPort: true },
  plugins: [{ name: "isolated-workflow-preview", configureServer(server) {
    server.middlewares.use(async (req, res, next) => {
      if (!req.url.startsWith("/api/")) return next();
      res.setHeader("X-Smart-Scheduler-Preview", "isolated-in-memory");
      try {
        const chunks = []; let length = 0;
        for await (const chunk of req) {
          length += chunk.length;
          if (length > 1_000_000) throw new Error("preview request exceeds 1 MB");
          chunks.push(chunk);
        }
        const raw = Buffer.concat(chunks).toString("utf8"); const body = raw ? JSON.parse(raw) : null;
        await fixture.handle({ request: () => ({ url: () => `http://127.0.0.1${req.url}`, method: () => req.method, postDataJSON: () => body }),
          fulfill: async ({ status = 200, json, body = "" }) => {
            res.statusCode = status; res.setHeader("Content-Type", "application/json; charset=utf-8"); res.end(json === undefined ? body : JSON.stringify(json));
          },
        });
      } catch (error) { res.statusCode = 400; res.end(JSON.stringify({ detail: error.message })); }
    });
  } }],
});
await server.listen(); server.printUrls();
console.log("Isolated workflow preview: simulated in-memory APIs only; no MySQL, Kafka or device requests.");
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, async () => { await server.close(); process.exit(0); });

import { randomUUID } from "node:crypto";

const clone = value => JSON.parse(JSON.stringify(value));
const objectPort = { schema: { type: "object", minProperties: 1 }, required: true, use_default: false, default: null };

function backendGraph(graph) {
  const result = clone(graph);
  for (const node of result.nodes) {
    if (node.executor) node.executor.resource_id ??= null;
    for (const port of [...Object.values(node.inputs), ...Object.values(node.outputs)]) {
      port.required ??= true; port.use_default ??= false;
      if (!("default" in port)) port.default = null;
    }
  }
  const sorted = value => Array.isArray(value) ? value.map(sorted) : value && typeof value === "object"
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, sorted(value[key])])) : value;
  return sorted(result);
}

export function materialsGraph() {
  const definitions = [
    ["magnetron_sputtering", "instrument_task", "磁控溅射制备", 104, 320],
    ["xrd", "data_upload", "XRD表征结构", 576, 180],
    ["eds", "data_upload", "EDS表征成分", 576, 320],
    ["nanoindentation", "data_upload", "纳米压痕表征硬度", 576, 460],
    ["gate", "join", "表征实验未完成", 1042, 320],
    ["model_inference", "model_inference", "MAT", 1456, 320],
  ];
  return { schema_version: 1, decision_node_id: "model_inference", positions: Object.fromEntries(definitions.map(([id, , , x, y]) => [id, { x, y }])),
    nodes: definitions.map(([id, type, label]) => ({ id, type, type_version: 1, label,
      config: type === "join" ? { policy: "all_success" } : type === "model_inference" ? { model_id: "mat-materials", model_version: "browser-test-only", next_round_delay_seconds: 3600 } : { instrument_code: id, component_id: `${id}-test` },
      inputs: id === "magnetron_sputtering" ? {} : ["gate", "model_inference"].includes(id) ? { xrd: clone(objectPort), eds: clone(objectPort), nano: clone(objectPort) } : { sample: clone(objectPort) },
      outputs: id === "magnetron_sputtering" ? { sample: clone(objectPort) } : id === "gate" ? { xrd: clone(objectPort), eds: clone(objectPort), nano: clone(objectPort) } : { [type === "model_inference" ? "candidate" : "result"]: clone(objectPort) },
      input_values: {}, executor: type === "join" ? null : { kind: type === "model_inference" ? "compute" : "device", gateway_id: type === "model_inference" ? "model-worker" : "device-gateway", adapter_id: type === "model_inference" ? "mat_model" : "simulator" },
      retry_policy: { max_attempts: 1, delay_seconds: 30 }, timeouts: { delivery_seconds: 60, acceptance_seconds: 120, execution_seconds: 3600, validation_seconds: 120 },
    })),
    edges: ["xrd", "eds", "nanoindentation"].flatMap((id, index) => [
      { source: "magnetron_sputtering", target: id, source_port: "sample", target_port: "sample", route: null },
      { source: id, target: "gate", source_port: "result", target_port: ["xrd", "eds", "nano"][index], route: null },
      { source: "gate", target: "model_inference", source_port: ["xrd", "eds", "nano"][index], target_port: ["xrd", "eds", "nano"][index], route: null },
    ]),
  };
}

export class BrowserFixture {
  constructor() {
    this.drafts = []; this.versions = []; this.calls = []; this.binding = null; this.mode = "legacy";
    this.failOperationOnce = false; this.graph = materialsGraph(); this.states = {};
    this.space = { elements: Object.fromEntries([
      ["ti_at", 13.8, 15.9, 18.7], ["zr_at", 30.2, 31.6, 33.1], ["nb_at", 18, 19.3, 20.3], ["v_at", 28.4, 33.6, 36.1],
    ].map(([key, min, median, max]) => [key, { min, p10: min, median, p90: max, max }])) };
    this.payload = { experiment_round_index: 39, experiment_batch_id: "PREVIEW-R39", target_hardness_gpa: 13,
      instrument_components: { magnetron_sputtering: { component_id: "Sputter-Cluster-01" }, xrd: { component_id: "xrd-lab-01" }, eds: { component_id: "eds-lab-01" }, nanoindentation: { component_id: "nanoindentation-lab-01" } }, predicted_composition_space: this.space };
    this.schedule = { id: "preview-schedule", name: "TiZrNbV-preview", status: "active", payload: this.payload, updated_at: "2026-10-12T00:00:00Z" };
  }
  execution() {
    const result = { id: "preview-run", schedule_id: this.schedule.id, status: "awaiting_feedback", attempt: 39, dispatched_at: "2026-10-12T00:00:00Z", dispatch_payload: clone(this.payload), feedback_observation: {} };
    if (this.mode === "workflow") {
      result.dispatch_payload.workflow_version_id = "browser-pinned-version";
      result.workflow_runtime = { run_id: result.id, workflow_version_id: "browser-pinned-version", definition_id: "browser-draft", version: 1, graph: clone(this.graph), status: "running",
        nodes: this.graph.nodes.map(node => ({ node_id: node.id, node_run_id: `job-${node.id}`, attempt: node.id === "magnetron_sputtering" ? 1 : 0, status: node.id === "magnetron_sputtering" ? "completed" : "waiting_dependencies", outputs: {}, error: null, ...this.states[node.id] })) };
    }
    return result;
  }
  async install(page) {
    await page.route("**/api/**", route => this.handle(route));
  }
  async handle(route) {
    const request = route.request(); const url = new URL(request.url()); const path = url.pathname.replace("/api/v1", ""); const method = request.method();
    const body = request.postDataJSON(); this.calls.push({ path, method, body: clone(body) });
    const respond = (data, status = 200) => route.fulfill(status === 204 ? { status, body: "" } : { status, json: clone(data) });
    const fail = (detail, status = 400) => respond({ detail }, status);
    if (path === "/workflow-types") return respond([
      ["instrument_task", "device", ["simulator", "kafka_gateway"]], ["data_upload", "device", ["simulator", "kafka_gateway"]], ["data_parse", "compute", ["manage_db"]], ["model_inference", "compute", ["mat_model"]], ["condition", null, []], ["join", null, []], ["manual_approval", null, []],
    ].map(([type, executor_kind, adapter_ids]) => ({ type, type_version: 1, executor_kind, adapter_ids, config_schema: { type: "object" } })));
    if (path === "/workflow-templates/materials") return respond({ name: "Materials characterization", graph: materialsGraph() });
    if (path === "/schedules") return respond([this.schedule]);
    if (path === "/executions") return respond([this.execution()]);
    if (path === "/recommendations") return respond([]);
    if (path === "/mat-model/space") return respond({ predicted_composition_space: this.space });
    if (path === "/messaging/health") return respond({ broker: { status: "unknown" }, outbox: { pending: 3, sent: 1 }, inbox: { applied: 1 }, workers: { model: { status: "online" } } });
    if (path.startsWith("/runs/")) return respond({ execution: this.execution(), recommendations: [], data_packages: [], events: [] });
    if (path.endsWith("/sample-plan")) return respond(null);
    if (path === "/workflows") {
      if (method === "GET") return respond(this.drafts);
      const draft = { id: randomUUID(), name: body.name, revision: 1, graph: backendGraph(body.graph) };
      this.drafts.push(draft); return respond(draft, 201);
    }
    const match = path.match(/^\/workflows\/([^/]+)(?:\/(.*))?$/);
    if (match) {
      const draft = this.drafts.find(item => item.id === match[1]); const tail = match[2];
      if (!draft) return fail("draft not found", 404);
      if (tail === "versions") return respond(this.versions.filter(version => version.definition_id === draft.id).reverse());
      if (tail?.startsWith("versions/")) return respond(this.versions.find(version => version.id === tail.slice(9)));
      if (method === "GET") return respond(draft);
      const revision = body?.revision ?? Number(url.searchParams.get("revision"));
      if (revision !== draft.revision) return fail("revision conflict", 409);
      if (method === "PUT") { draft.graph = backendGraph(body.graph); draft.revision += 1; return respond(draft); }
      if (tail === "validate" || tail === "publish") {
        if (!draft.graph.nodes.length) return fail("workflow needs a node");
        if (draft.graph.nodes.some(node => node.config?.model_version === "unconfigured")) return fail("pinned model version required");
        if (tail === "validate") return respond({ valid: true, revision: draft.revision });
        const version = { id: randomUUID(), definition_id: draft.id, version: this.versions.filter(v => v.definition_id === draft.id).length + 1, digest: "browser-fixture-digest", graph: clone(draft.graph) };
        this.versions.push(version); return respond(version, 201);
      }
      if (method === "DELETE") { this.drafts = this.drafts.filter(item => item !== draft); return respond(null, 204); }
    }
    if (path.endsWith("/workflow") && path.startsWith("/schedules/")) { this.binding = clone(body); return respond({ schedule_id: this.schedule.id, ...body }); }
    if (path.endsWith("/workflow") && path.startsWith("/executions/")) return respond(this.execution().workflow_runtime);
    if (path.endsWith("/operations")) {
      if (this.failOperationOnce) { this.failOperationOnce = false; return fail("temporary service failure", 503); }
      return respond({ applied: true });
    }
    return fail(`Unexpected test request: ${method} ${path}`, 404);
  }
}

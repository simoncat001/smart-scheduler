import { cloneJson, normalizeWorkflowGraph, canonicalGraphJson } from "./workflow-graph.js";

export class WorkflowApiError extends Error {
  constructor(status, detail) {
    const message = typeof detail === "string" ? detail : JSON.stringify(detail);
    super(message || `请求失败：${status}`);
    this.status = status;
    this.detail = detail;
  }
}

export function createWorkflowApi(fetcher = (...args) => fetch(...args)) {
  async function request(path, method = "GET", body) {
    const response = await fetcher(`/api/v1${path}`, {
      method, headers: { "Content-Type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new WorkflowApiError(response.status, error.detail || response.statusText);
    }
    return response.status === 204 ? null : response.json();
  }
  const id = encodeURIComponent;
  return {
    types: () => request("/workflow-types"), template: () => request("/workflow-templates/materials"),
    list: () => request("/workflows?limit=500"), read: key => request(`/workflows/${id(key)}`),
    create: (name, graph) => request("/workflows", "POST", { name, graph }),
    save: (key, revision, graph) => request(`/workflows/${id(key)}`, "PUT", { revision, graph }),
    remove: (key, revision) => request(`/workflows/${id(key)}?revision=${revision}`, "DELETE"),
    validate: (key, revision) => request(`/workflows/${id(key)}/validate`, "POST", { revision }),
    publish: (key, revision) => request(`/workflows/${id(key)}/publish`, "POST", { revision }),
    versions: key => request(`/workflows/${id(key)}/versions`),
    version: (key, version) => request(`/workflows/${id(key)}/versions/${id(version)}`),
    schedules: () => request("/schedules"), binding: key => request(`/schedules/${id(key)}/workflow`),
    bind: (key, version) => request(`/schedules/${id(key)}/workflow`, "PUT", { workflow_version_id: version }),
    runtime: key => request(`/executions/${id(key)}/workflow`), health: () => request("/messaging/health"),
    operate: (key, operation) => request(`/workflow-nodes/${id(key)}/operations`, "POST", operation),
  };
}

export const workflowApi = createWorkflowApi();

export class WorkflowDraftSession {
  constructor() {
    this.id = ""; this.name = "新流程"; this.revision = 0;
    this.graph = normalizeWorkflowGraph(); this.savedJson = "";
    this.published = null; this.conflict = false; this.generation = 0;
  }
  get dirty() { return canonicalGraphJson(this.graph) !== this.savedJson; }
  get readonly() { return Boolean(this.published); }
  get displayGraph() { return this.published?.graph || this.graph; }
  load(draft) {
    this.generation += 1;
    this.id = draft.id || ""; this.name = draft.name; this.revision = draft.revision || 0;
    this.graph = normalizeWorkflowGraph(draft.graph); this.savedJson = this.id ? canonicalGraphJson(this.graph) : "";
    this.published = null; this.conflict = false;
  }
  viewVersion(version) { this.published = version ? { ...cloneJson(version), graph: normalizeWorkflowGraph(version.graph) } : null; }
  async save(api) {
    if (this.readonly) throw new Error("发布版本只读，请切回草稿");
    const graph = cloneJson(this.graph); const generation = this.generation;
    try {
      const saved = this.id ? await api.save(this.id, this.revision, graph) : await api.create(this.name, graph);
      if (generation !== this.generation) return saved;
      this.id = saved.id; this.revision = saved.revision; this.name = saved.name;
      if (canonicalGraphJson(this.graph) === canonicalGraphJson(graph)) this.graph = normalizeWorkflowGraph(saved.graph);
      this.savedJson = canonicalGraphJson(saved.graph); this.conflict = false;
      return saved;
    } catch (error) {
      if (error.status === 409 && generation === this.generation) this.conflict = true;
      throw error;
    }
  }
}

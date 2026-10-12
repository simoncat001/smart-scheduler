import test from "node:test";
import assert from "node:assert/strict";

const helpers = await import("../src/workflow-api.js").catch(() => ({}));
const graph = () => ({ schema_version: 1, nodes: [{ id: "approval", type: "manual_approval", type_version: 1, label: "审批", config: {}, inputs: {}, outputs: {} }], edges: [], positions: {} });

test("HTTP 409 retains structured error status and server detail", async () => {
  const api = helpers.createWorkflowApi(async () => new Response(JSON.stringify({ detail: "revision conflict" }), { status: 409 }));
  await assert.rejects(api.save("draft-1", 2, graph()), e => e.status === 409 && e.message === "revision conflict");
});

test("revision-aware save preserves dirty graph when another editor has saved", async () => {
  const session = new helpers.WorkflowDraftSession();
  session.load({ id: "draft-1", name: "流程", revision: 2, graph: graph() });
  session.graph.nodes[0].label = "本地修改";
  const api = helpers.createWorkflowApi(async () => new Response(JSON.stringify({ detail: "stale" }), { status: 409 }));
  await assert.rejects(session.save(api));
  assert.equal(session.graph.nodes[0].label, "本地修改");
  assert.equal(session.revision, 2);
  assert.equal(session.dirty, true);
  assert.equal(session.conflict, true);
});

test("published view is separate from unsaved draft and cannot be saved over it", async () => {
  const session = new helpers.WorkflowDraftSession();
  session.load({ id: "draft-1", name: "流程", revision: 3, graph: graph() });
  session.graph.nodes[0].label = "草稿";
  const version = { id: "version-1", version: 1, graph: graph() };
  session.viewVersion(version);
  session.displayGraph.nodes[0].label = "外部修改";
  assert.equal(version.graph.nodes[0].label, "审批");
  assert.equal(session.graph.nodes[0].label, "草稿");
  await assert.rejects(session.save({}), /只读/);
  session.viewVersion(null);
  assert.equal(session.displayGraph.nodes[0].label, "草稿");
});

test("an edit made during a save stays dirty against the persisted snapshot", async () => {
  let finish;
  const session = new helpers.WorkflowDraftSession();
  session.load({ id: "draft-1", name: "流程", revision: 3, graph: graph() });
  const saving = session.save({ save: (_id, _revision, snapshot) => new Promise(resolve => { finish = () => resolve({ id: "draft-1", name: "流程", revision: 4, graph: snapshot }); }) });
  session.graph.nodes[0].label = "保存时又修改";
  finish(); await saving;
  assert.equal(session.revision, 4);
  assert.equal(session.graph.nodes[0].label, "保存时又修改");
  assert.equal(session.dirty, true);
});

test("server sorted keys and schema defaults do not leave a successfully saved new workflow dirty", async () => {
  const session = new helpers.WorkflowDraftSession();
  const input = graph(); input.nodes[0].outputs = { result: { schema: { type: "object" } } };
  session.load({ name: "新流程", graph: input });
  const backendGraph = { decision_node_id: null, edges: [], nodes: [{
    config: {}, executor: null, id: "approval", input_values: {}, inputs: {}, label: "审批",
    outputs: { result: { default: null, required: true, schema: { type: "object" }, use_default: false } },
    retry_policy: { delay_seconds: 30, max_attempts: 1 },
    timeouts: { acceptance_seconds: 120, delivery_seconds: 60, execution_seconds: 3600, validation_seconds: 120 },
    type: "manual_approval", type_version: 1,
  }], positions: {}, schema_version: 1 };
  await session.save({ create: async () => ({ id: "draft-1", name: "新流程", revision: 1, graph: backendGraph }) });
  assert.equal(session.dirty, false);
  assert.equal(session.revision, 1);
});

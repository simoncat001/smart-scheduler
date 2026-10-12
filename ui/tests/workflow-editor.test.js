import test from "node:test";
import assert from "node:assert/strict";
import WorkflowBuilder from "../src/components/workflow-builder.js";

function editor(configJson) {
  const state = WorkflowBuilder.data();
  Object.assign(state, WorkflowBuilder.methods);
  Object.defineProperty(state, "readonly", { get: () => state.session.readonly });
  Object.defineProperty(state, "selectedNode", { get: () => state.session.graph.nodes.find(node => node.id === state.selectedId) });
  state.session.load({ id: "draft-1", name: "流程", revision: 1, graph: { nodes: [{ id: "node-a", label: "A", type: "data_upload", config: { instrument_code: "xrd" }, inputs: {}, outputs: {} }], edges: [] } });
  state.selectedId = "node-a"; state.configJson = configJson; state.formDirty = true;
  return state;
}

test("edge selection applies pending node JSON rather than discarding its buffers", () => {
  const state = editor('{"instrument_code":"eds"}');
  state.selectEdge(0);
  assert.equal(state.session.graph.nodes[0].config.instrument_code, "eds");
  assert.equal(state.selectedEdge, 0);
  assert.equal(state.formDirty, false);
});

test("invalid pending JSON blocks edge selection and keeps the node selected", () => {
  const state = editor("{");
  state.selectEdge(0);
  assert.equal(state.selectedId, "node-a");
  assert.equal(state.selectedEdge, null);
  assert.equal(state.formDirty, true);
  assert.ok(state.notice);
});

test("node initialization before viewport readiness preserves the pending fit intent", () => {
  const state = editor("{}");
  state.needsFit = true;
  state.nodesInitialized();
  assert.equal(state.needsFit, true);
});

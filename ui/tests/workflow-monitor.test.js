import test from "node:test";
import assert from "node:assert/strict";

const helpers = await import("../src/workflow-monitor.js").catch(() => ({}));
const runtime = () => ({ run_id: "run-1", workflow_version_id: "version-1", status: "running",
graph: { schema_version: 1, nodes: [
  { id: "xrd-a", type: "data_upload", label: "XRD A", config: { instrument_code: "xrd" }, inputs: {}, outputs: {} },
  { id: "xrd-b", type: "data_upload", label: "XRD B", config: { instrument_code: "xrd" }, inputs: {}, outputs: {} },
  { id: "mat", type: "model_inference", label: "MAT", config: {}, inputs: {}, outputs: {} },
], edges: [{ source: "xrd-a", target: "mat" }], positions: { "xrd-a": { x: 104, y: 320 } } },
nodes: [
  { node_id: "xrd-a", node_run_id: "job-a", status: "completed", attempt: 1, outputs: { result: { sample: "A" } } },
  { node_id: "xrd-b", node_run_id: "job-b", status: "queued", attempt: 1, outputs: {} },
  { node_id: "mat", node_run_id: "job-m", status: "waiting_dependencies", attempt: 0, outputs: {} },
] });

test("same-type instances retain separate ownership and results", () => {
  const monitor = helpers.runtimeToMonitor(runtime());
  assert.deepEqual(monitor.nodes.map(n => n.code), ["xrd-a", "xrd-b", "mat"]);
  assert.equal(monitor.nodes[0].nodeRunId, "job-a");
  assert.equal(monitor.nodes[0].outputs.result.sample, "A");
  assert.equal(monitor.nodes[1].statusLabel, "待上传");
  assert.deepEqual(monitor.nodes[1].outputs, {});
  assert.equal(monitor.nodes[2].statusLabel, "待预测开始");
  assert.equal(monitor.links[0].source, "xrd-a");
});

test("all parallel characterization nodes waiting for dependencies or acceptance remain pending upload", () => {
  for (const status of ["waiting_dependencies", "queued", "accepted"]) {
    assert.equal(helpers.workflowStateLabel(status, "data_upload"), "待上传");
    assert.equal(helpers.workflowStateLabel(status, "model_inference"), "待预测开始");
  }
  assert.equal(helpers.workflowStateLabel("running", "model_inference"), "预测中");
  assert.equal(helpers.workflowStateLabel("needs_attention", "data_upload"), "需处理");
  assert.equal(helpers.workflowStateLabel("waiting_approval", "manual_approval"), "待审批");
});

test("runtime rendering requires the pinned graph, never guessed fixed nodes or current draft", () => {
  const input = runtime(); delete input.graph;
  assert.throws(() => helpers.runtimeToMonitor(input), /版本|流程图/);
});

test("default center positions are preserved and status-only updates never replace graph with outputs", () => {
  const input = runtime(); const monitor = helpers.runtimeToMonitor(input);
  assert.deepEqual(monitor.graph.positions["xrd-a"], { x: 104, y: 320 });
  input.nodes[0].outputs.result.sample = "modified";
  assert.equal(monitor.nodes[0].outputs.result.sample, "A");
  assert.equal(monitor.completed, 1);
  assert.equal(monitor.total, 3);
});

test("operator controls only expose dispatched executor attempts or manual approval", () => {
  assert.deepEqual(helpers.availableNodeOperations({ rawStatus: "waiting_dependencies", attempt: 0, type: "data_upload", definition: { executor: { kind: "device" } } }), []);
  assert.deepEqual(helpers.availableNodeOperations({ rawStatus: "waiting_approval", attempt: 0, type: "manual_approval", definition: { executor: null } }), ["approve"]);
  assert.deepEqual(helpers.availableNodeOperations({ rawStatus: "needs_attention", attempt: 1, type: "data_upload", definition: { executor: { kind: "device" }, retry_policy: { max_attempts: 1 } } }), ["cancel"]);
  assert.deepEqual(helpers.availableNodeOperations({ rawStatus: "needs_attention", attempt: 1, type: "model_inference", definition: { executor: { kind: "compute" }, retry_policy: { max_attempts: 2 } } }), ["retry", "cancel"]);
});

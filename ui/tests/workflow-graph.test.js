import test from "node:test";
import assert from "node:assert/strict";

const helpers = await import("../src/workflow-graph.js").catch(() => ({}));
const port = { schema: { type: "object" }, required: true };
const graph = () => ({ schema_version: 1, nodes: [
  { id: "xrd-a", type: "data_upload", type_version: 1, label: "XRD A", config: {}, inputs: {}, outputs: { result: port } },
  { id: "xrd-b", type: "data_upload", type_version: 1, label: "XRD B", config: {}, inputs: { sample: port }, outputs: {} },
], edges: [{ source: "xrd-a", target: "xrd-b", source_port: "result", target_port: "sample" }],
positions: { "xrd-a": { x: 104, y: 320 }, "xrd-b": { x: 576, y: 180 } }, decision_node_id: null });

test("normalization preserves instance IDs, port schemas and default positions without mutating input", () => {
  const input = graph();
  const normalized = helpers.normalizeWorkflowGraph(input);
  assert.deepEqual(normalized.positions, input.positions);
  assert.equal(normalized.nodes[1].id, "xrd-b");
  normalized.nodes[0].outputs.result.schema.type = "string";
  assert.equal(input.nodes[0].outputs.result.schema.type, "object");
});

test("duplicate IDs are reported instead of silently merging two instances", () => {
  const input = graph(); input.nodes[1].id = "xrd-a";
  assert.ok(helpers.validateEditorGraph(input).some(e => e.code === "duplicate_node"));
});

test("dangling endpoints and unknown typed ports are rejected", () => {
  const input = graph(); input.edges[0].target_port = "missing";
  assert.ok(helpers.validateEditorGraph(input).some(e => e.code === "unknown_port"));
  input.edges[0].target = "deleted";
  assert.ok(helpers.validateEditorGraph(input).some(e => e.code === "unknown_node"));
});

test("deleting a linked decision node removes edges, positions and designation", () => {
  const input = graph(); input.decision_node_id = "xrd-b";
  const result = helpers.deleteWorkflowNode(input, "xrd-b");
  assert.deepEqual(result.nodes.map(n => n.id), ["xrd-a"]);
  assert.deepEqual(result.edges, []);
  assert.deepEqual(result.positions, { "xrd-a": { x: 104, y: 320 } });
  assert.equal(result.decision_node_id, null);
  assert.equal(input.nodes.length, 2);
});

test("duplication makes a stable unique instance with independent config and no inherited connections", () => {
  const input = graph(); input.nodes[0].config = { instrument_code: "xrd" };
  const result = helpers.duplicateWorkflowNode(input, "xrd-a");
  assert.equal(result.nodes.length, 3);
  assert.equal(result.nodes[2].id, "xrd-a_copy");
  assert.equal(result.edges.length, 1);
  result.nodes[2].config.instrument_code = "eds";
  assert.equal(input.nodes[0].config.instrument_code, "xrd");
  assert.equal(helpers.duplicateWorkflowNode(result, "xrd-a").nodes[3].id, "xrd-a_copy_2");
});

test("typed connections reject mixed control/data handles and multiple input writers", () => {
  const input = graph(); input.edges = [];
  assert.throws(() => helpers.connectWorkflowPorts(input, { source: "xrd-a", target: "xrd-b", source_port: "result" }), /端口/);
  const linked = helpers.connectWorkflowPorts(input, graph().edges[0]);
  assert.equal(linked.edges.length, 1);
  assert.throws(() => helpers.connectWorkflowPorts(linked, graph().edges[0]), /重复|输入/);
});

test("flow mapping preserves backend center positions and separates control handles from typed ports", () => {
  const result = helpers.toFlowGraph(graph());
  assert.deepEqual(result.nodes[0].position, { x: 4, y: 275 });
  assert.equal(result.edges[0].sourceHandle, "output:result");
  assert.equal(result.edges[0].targetHandle, "input:sample");
});

test("new device instances use gateway/adapter binding without resource registration or device retries", () => {
  const node = helpers.createWorkflowNode({ type: "data_upload", type_version: 1, executor_kind: "device", adapter_ids: ["simulator", "kafka_gateway"] }, []);
  assert.equal(node.id, "data_upload");
  assert.equal(node.executor.kind, "device");
  assert.equal(node.retry_policy.max_attempts, 1);
  assert.equal(node.executor.resource_id, undefined);
});

test("malformed port definitions report errors instead of crashing local validation", () => {
  const input = graph(); input.nodes[1].inputs.sample = 7;
  assert.ok(helpers.validateEditorGraph(input).some(error => error.code === "port"));
});

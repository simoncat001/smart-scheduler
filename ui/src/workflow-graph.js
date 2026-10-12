export const FLOW_NODE_SIZE = { width: 200, height: 90 };
export const TYPE_LABELS = {
  instrument_task: "设备任务", data_upload: "数据上传", data_parse: "数据解析",
  model_inference: "模型推理", condition: "条件分支", join: "并行汇合", manual_approval: "人工审批",
};

export function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

export function normalizeWorkflowGraph(input = {}) {
  const graph = cloneJson(input);
  const ports = definitions => Object.fromEntries(Object.entries(definitions || {}).map(([name, port]) => [name,
    port && typeof port === "object" && !Array.isArray(port) ? { required: true, use_default: false, default: null, ...port } : port,
  ]));
  const executor = value => {
    if (!value) return null;
    const { resource_id, ...binding } = value;
    return resource_id == null ? binding : { ...binding, resource_id };
  };
  return {
    schema_version: graph.schema_version ?? 1,
    nodes: (graph.nodes || []).map(node => ({
      ...node, type_version: node.type_version ?? 1, config: node.config || {},
      inputs: ports(node.inputs), outputs: ports(node.outputs), input_values: node.input_values || {},
      executor: executor(node.executor),
      retry_policy: { max_attempts: 1, delay_seconds: 30, ...node.retry_policy },
      timeouts: { delivery_seconds: 60, acceptance_seconds: 120, execution_seconds: 3600, validation_seconds: 120, ...node.timeouts },
    })),
    edges: (graph.edges || []).map(edge => ({
      source: edge.source, target: edge.target, source_port: edge.source_port ?? null,
      target_port: edge.target_port ?? null, route: edge.route ?? null,
    })),
    positions: graph.positions || {}, decision_node_id: graph.decision_node_id ?? null,
  };
}

export function canonicalGraphJson(input) {
  const ordered = value => Array.isArray(value) ? value.map(ordered) : value && typeof value === "object"
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, ordered(value[key])])) : value;
  return JSON.stringify(ordered(normalizeWorkflowGraph(input)));
}

export function validateEditorGraph(input) {
  const graph = normalizeWorkflowGraph(input);
  const errors = [];
  const nodes = new Map();
  const report = (code, message) => errors.push({ code, message });
  if (!graph.nodes.length) report("empty", "流程至少需要一个节点");
  for (const node of graph.nodes) {
    if (!/^[a-z0-9][a-z0-9_-]{0,63}$/.test(node.id)) report("node_id", `节点 ID 不合法：${node.id}`);
    if (nodes.has(node.id)) report("duplicate_node", `节点 ID 重复：${node.id}`);
    nodes.set(node.id, node);
    if (!node.label?.trim()) report("label", `${node.id} 缺少名称`);
    for (const direction of ["inputs", "outputs"]) {
      for (const [name, port] of Object.entries(node[direction])) {
        if (!/^[a-z0-9][a-z0-9_-]{0,63}$/.test(name) || !port || typeof port !== "object" || Array.isArray(port) || !("schema" in port)) {
          report("port", `${node.id} 的 ${name} 端口缺少合法名称或 schema`);
        }
      }
    }
  }
  const signatures = new Set(); const writers = new Set();
  for (const edge of graph.edges) {
    const source = nodes.get(edge.source); const target = nodes.get(edge.target);
    if (!source || !target) { report("unknown_node", `连线引用了不存在的节点：${edge.source} → ${edge.target}`); continue; }
    if (source.id === target.id) report("self_edge", `${source.id} 不能连接自身`);
    const signature = JSON.stringify(edge);
    if (signatures.has(signature)) report("duplicate_edge", "重复连线");
    signatures.add(signature);
    if (Boolean(edge.source_port) !== Boolean(edge.target_port)) report("mixed_port", "数据连线两端都必须选择端口");
    if (edge.source_port && edge.target_port) {
      const output = source.outputs[edge.source_port]; const inputPort = target.inputs[edge.target_port];
      if (!output || !inputPort) report("unknown_port", `未知端口：${edge.source}.${edge.source_port} → ${edge.target}.${edge.target_port}`);
      const key = `${edge.target}:${edge.target_port}`;
      if (writers.has(key)) report("multiple_writers", `输入端口已连接：${key}`);
      writers.add(key);
    }
  }
  if (graph.decision_node_id && nodes.get(graph.decision_node_id)?.type !== "model_inference") {
    report("decision_node", "决策节点必须是模型推理节点");
  }
  // Server validation is authoritative for DAG, branch coverage and JSON Schema compatibility.
  return errors;
}

function uniqueNodeId(base, nodes) {
  const ids = new Set(nodes.map(node => node.id));
  const stem = base.slice(0, 56);
  let id = stem; let suffix = 2;
  while (ids.has(id)) id = `${stem}_${suffix++}`;
  return id;
}

export function deleteWorkflowNode(input, id) {
  const graph = normalizeWorkflowGraph(input);
  graph.nodes = graph.nodes.filter(node => node.id !== id);
  graph.edges = graph.edges.filter(edge => edge.source !== id && edge.target !== id);
  delete graph.positions[id];
  if (graph.decision_node_id === id) graph.decision_node_id = null;
  return graph;
}

export function duplicateWorkflowNode(input, id) {
  const graph = normalizeWorkflowGraph(input);
  const source = graph.nodes.find(node => node.id === id);
  if (!source) throw new Error("节点不存在");
  const newId = uniqueNodeId(`${id.slice(0, 51)}_copy`, graph.nodes);
  graph.nodes.push({ ...cloneJson(source), id: newId, label: `${source.label} 副本`.slice(0, 120) });
  const position = graph.positions[id] || { x: 200, y: 200 };
  graph.positions[newId] = { x: position.x + 40, y: position.y + 120 };
  return graph;
}

export function connectWorkflowPorts(input, connection) {
  const graph = normalizeWorkflowGraph(input);
  const edge = normalizeWorkflowGraph({ edges: [connection] }).edges[0];
  graph.edges.push(edge);
  const errors = validateEditorGraph(graph).filter(error => [
    "mixed_port", "unknown_port", "unknown_node", "duplicate_edge", "multiple_writers", "self_edge",
  ].includes(error.code));
  if (errors.length) throw new Error(errors.map(error => error.message).join("；"));
  return graph;
}

export function createWorkflowNode(entry, nodes) {
  const defaults = {
    instrument_task: { instrument_code: "instrument", action: "execute" },
    data_upload: { instrument_code: "instrument" }, data_parse: { parser_id: "manage_db" },
    model_inference: { model_id: "mat-materials", model_version: "unconfigured", next_round_delay_seconds: 3600 },
    condition: { field: "value", operator: "eq", value: true, match_route: "matched", default_route: "default" },
    join: { policy: "all_success" }, manual_approval: {},
  };
  return normalizeWorkflowGraph({ nodes: [{
    id: uniqueNodeId(entry.type, nodes), type: entry.type, type_version: entry.type_version,
    label: TYPE_LABELS[entry.type] || entry.type, config: defaults[entry.type] || {},
    executor: entry.executor_kind ? {
      kind: entry.executor_kind, gateway_id: entry.executor_kind === "device" ? "device-gateway" : entry.type === "data_parse" ? "parser-worker" : "model-worker",
      adapter_id: entry.adapter_ids.includes("kafka_gateway") ? "kafka_gateway" : entry.adapter_ids[0],
    } : null,
  }] }).nodes[0];
}

export function toFlowGraph(input) {
  const graph = normalizeWorkflowGraph(input);
  return {
    nodes: graph.nodes.map((node, index) => {
      const center = graph.positions[node.id] || { x: 160 + (index % 4) * 280, y: 160 + Math.floor(index / 4) * 160 };
      return { id: node.id, type: "workflow", position: { x: center.x - 100, y: center.y - 45 }, data: { definition: node } };
    }),
    edges: graph.edges.map((edge, index) => ({
      id: `edge-${index}`, source: edge.source, target: edge.target, type: "smoothstep",
      sourceHandle: edge.source_port ? `output:${edge.source_port}` : "control-out",
      targetHandle: edge.target_port ? `input:${edge.target_port}` : "control-in",
      label: edge.route || (edge.source_port ? `${edge.source_port} → ${edge.target_port}` : ""),
      data: { index },
    })),
  };
}

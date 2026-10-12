import { cloneJson, normalizeWorkflowGraph } from "./workflow-graph.js";

export function workflowStateLabel(status, type) {
  if (["waiting_dependencies", "queued", "accepted"].includes(status)) {
    if (type === "data_upload") return "待上传";
    if (type === "model_inference") return "待预测开始";
  }
  if (status === "running" && type === "model_inference") return "预测中";
  if (status === "completed" && type === "model_inference") return "预测完成";
  return {
    waiting_dependencies: "等待依赖", queued: "已排队", accepted: "已接收", running: "进行中",
    completed: "已完成", failed: "失败", cancelled: "已取消", skipped: "已跳过",
    needs_attention: "需处理", waiting_approval: "待审批", cancel_requested: "取消待确认",
    paused: "已暂停",
  }[status] || "状态未知";
}

export function availableNodeOperations(node) {
  if (!node) return [];
  if (node.type === "manual_approval" && node.rawStatus === "waiting_approval") return ["approve"];
  if (!node.definition.executor || node.attempt < 1) return [];
  const operations = [];
  if (node.definition.executor.kind === "compute" && node.rawStatus === "needs_attention" && node.attempt < node.definition.retry_policy.max_attempts) operations.push("retry");
  if (["queued", "accepted", "running", "needs_attention"].includes(node.rawStatus)) operations.push("cancel");
  return operations;
}

export function runtimeToMonitor(runtime) {
  if (!runtime?.graph || !runtime.workflow_version_id) throw new Error("运行状态缺少固定版本流程图");
  const graph = normalizeWorkflowGraph(runtime.graph);
  const states = new Map((runtime.nodes || []).map(node => [node.node_id, node]));
  const nodes = graph.nodes.map(definition => {
    const state = states.get(definition.id) || {};
    const rawStatus = state.status || "unknown";
    const status = rawStatus === "completed" ? "completed" : ["failed", "needs_attention"].includes(rawStatus) ? "failed"
      : ["running", "waiting_approval", "cancel_requested"].includes(rawStatus) ? "current" : "pending";
    return {
      code: definition.id, title: definition.label, type: definition.type, status, rawStatus,
      statusLabel: workflowStateLabel(rawStatus, definition.type), nodeRunId: state.node_run_id,
      attempt: state.attempt || 0, outputs: cloneJson(state.outputs || {}), error: state.error || "",
      componentId: definition.config.component_id || definition.executor?.gateway_id || "系统节点",
      detail: state.error || "", definition,
    };
  });
  const active = nodes.filter(node => node.rawStatus !== "skipped");
  const completed = active.filter(node => node.rawStatus === "completed").length;
  const gate = nodes.find(node => node.type === "join" || node.type === "condition");
  return {
    graph, nodes, links: cloneJson(graph.edges), runId: runtime.run_id, versionId: runtime.workflow_version_id,
    completed, total: active.length, status: runtime.status,
    gateStatus: gate?.rawStatus === "completed" ? "open" : "locked",
    gateLabel: gate?.statusLabel || workflowStateLabel(runtime.status),
    hint: nodes.filter(node => !["completed", "skipped"].includes(node.rawStatus)).map(node => `${node.title}：${node.statusLabel}`).join(" / "),
  };
}

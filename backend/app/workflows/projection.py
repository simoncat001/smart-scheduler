import json
from collections import Counter

from sqlalchemy import select

from app.services.exceptions import NotFoundError
from app.workflows.models import NodeRun, WorkflowRun, WorkflowVersion
from app.workflows.schemas import WorkflowGraph


def workflow_projection(session, execution_id):
    run = session.get(WorkflowRun, execution_id)
    if run is None:
        raise NotFoundError("workflow run not found")
    version = session.get(WorkflowVersion, run.workflow_version_id)
    graph = WorkflowGraph.model_validate_json(version.graph_json)
    states = {node.node_id: node for node in session.scalars(select(NodeRun).where(NodeRun.run_id == execution_id))}
    counts = Counter(node.config.get("instrument_code") for node in graph.nodes if node.config.get("instrument_code"))
    unique_instruments = {node.config["instrument_code"]: (node, states[node.id]) for node in graph.nodes
                          if node.config.get("instrument_code") and counts[node.config["instrument_code"]] == 1}
    steps = []
    for spec in graph.nodes:
        if spec.executor is None and spec.type != "manual_approval":
            continue
        node = states[spec.id]
        code = spec.config.get("instrument_code", spec.id)
        if counts.get(code, 0) > 1:
            code = spec.id
        steps.append({"code": code, "node_id": spec.id, "title": spec.label,
                      "output": next(iter(spec.outputs), "result"),
                      "status": ("completed" if node.status == "completed" else
                                 "skipped" if node.status == "skipped" else
                                 "current" if node.status in {"queued", "accepted", "running", "waiting_approval", "cancel_requested"} else
                                 "pending")})
    active = [step for step in steps if step["status"] != "skipped"]
    missing = [{"code": step["code"], "title": step["title"]} for step in active if step["status"] != "completed"]
    current = next((step for step in active if step["status"] == "current"), None)
    completed = sum(step["status"] == "completed" for step in active)
    progress = {"workflow_kind": "configured_workflow", "workflow_version_id": run.workflow_version_id,
                "steps": steps, "current_step": current["code"] if current else None,
                "current_output": current["output"] if current else None, "missing_steps": missing,
                "completed_count": completed, "total_steps": len(active),
                "completion_ratio": round(completed / len(active), 3) if active else float(run.status == "completed"),
                "is_complete": run.status == "completed"}
    return run, graph, states, unique_instruments, progress


def workflow_runtime_read(session, execution_id: str) -> dict:
    run = session.get(WorkflowRun, execution_id)
    if run is None:
        raise NotFoundError("workflow run not found")
    nodes = list(session.scalars(select(NodeRun).where(NodeRun.run_id == execution_id).order_by(NodeRun.node_id)))
    version = session.get(WorkflowVersion, run.workflow_version_id)
    return {
        "run_id": run.execution_id, "workflow_version_id": run.workflow_version_id,
        "definition_id": version.definition_id, "version": version.version,
        "graph": json.loads(version.graph_json),
        "status": run.status,
        "nodes": [{"node_run_id": node.id, "node_id": node.node_id, "status": node.status,
                   "attempt": node.attempt, "error": node.error,
                   "outputs": {key: value for key, value in json.loads(node.outputs_json).items()
                               if key != "__route__"}}
                  for node in nodes],
    }

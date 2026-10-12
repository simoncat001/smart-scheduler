from __future__ import annotations

from app.workflows.schemas import WorkflowGraph


def default_materials_graph() -> WorkflowGraph:
    """An unbound draft; configure gateway routing and a pinned model before use."""
    result_schema = {"type": "object", "minProperties": 1}
    port = {"schema": result_schema}
    preparation = {
        "id": "magnetron_sputtering", "type": "instrument_task", "label": "磁控溅射制备",
        "config": {"instrument_code": "magnetron_sputtering"},
        "executor": {"kind": "device",
                     "gateway_id": "device-gateway", "adapter_id": "simulator"},
        "outputs": {"sample": port},
    }
    characterization = [
        {"id": node_id, "type": "data_upload", "label": label,
         "config": {"instrument_code": node_id},
         "executor": {"kind": "device",
                      "gateway_id": "device-gateway", "adapter_id": "simulator"},
         "inputs": {"sample": port}, "outputs": {"result": port}}
        for node_id, label in (
            ("xrd", "XRD表征结构"), ("eds", "EDS表征成分"),
            ("nanoindentation", "纳米压痕表征硬度"),
        )
    ]
    gate = {"id": "gate", "type": "join", "label": "表征实验完成门控",
            "inputs": {node["id"]: port for node in characterization},
            "outputs": {node["id"]: port for node in characterization}}
    inference = {
        "id": "model_inference", "type": "model_inference", "label": "模型推理",
        "config": {"model_id": "mat-materials", "model_version": "unconfigured"},
        "executor": {"kind": "compute", "gateway_id": "model-worker", "adapter_id": "mat_model"},
        "inputs": {node["id"]: port for node in characterization},
        "outputs": {"candidate": port},
    }
    edges = []
    for node in characterization:
        edges.extend([
            {"source": preparation["id"], "target": node["id"],
             "source_port": "sample", "target_port": "sample"},
            {"source": node["id"], "target": "gate", "source_port": "result", "target_port": node["id"]},
            {"source": "gate", "target": "model_inference",
             "source_port": node["id"], "target_port": node["id"]},
        ])
    return WorkflowGraph.model_validate({
        "nodes": [preparation, *characterization, gate, inference], "edges": edges,
        "positions": {
            "magnetron_sputtering": {"x": 104, "y": 320},
            "xrd": {"x": 576, "y": 180}, "eds": {"x": 576, "y": 320},
            "nanoindentation": {"x": 576, "y": 460}, "gate": {"x": 1042, "y": 320},
            "model_inference": {"x": 1456, "y": 320},
        },
        "decision_node_id": "model_inference",
    })

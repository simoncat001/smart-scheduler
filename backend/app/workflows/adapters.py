from dataclasses import dataclass
import json

from app.services.exceptions import ConflictError
from app.workflows.definitions import canonical_json
from app.workflows.executors import command_spec


@dataclass(frozen=True)
class ExecutionContext:
    execution_id: str
    workflow_version_id: str
    node_run_id: str
    attempt: int
    command_id: str
    model_id: str | None
    model_version: str | None
    _inputs_json: str
    _execution_json: str

    @classmethod
    def from_command(cls, command):
        spec = command_spec(command)
        return cls(command.run_id, command.workflow_version_id, command.node_run_id, command.attempt,
                   command.command_id, spec.config.get("model_id"), spec.config.get("model_version"),
                   canonical_json(command.payload["inputs"]), canonical_json(command.payload["execution_context"]))

    @property
    def inputs(self):
        return json.loads(self._inputs_json)

    @property
    def execution_payload(self):
        return json.loads(self._execution_json)


class AdapterRegistry:
    def __init__(self, settings, *, simulator_enabled=False):
        self.settings, self.simulator_enabled = settings, simulator_enabled

    def execute(self, command, context):
        if context != ExecutionContext.from_command(command):
            raise ConflictError("adapter context does not match pinned command")
        spec = command_spec(command)
        if command.adapter_id == "simulator":
            if not self.simulator_enabled or spec.executor.kind != "device":
                raise ConflictError("simulator is disabled or binding is not simulated")
            code = spec.config["instrument_code"]
            payload = context.execution_payload
            values = {"simulated": True, "instrument_code": code}
            if code == "magnetron_sputtering":
                values["sample_id"] = payload.get("sample_id") or f"sim-{context.execution_id}"
            elif code == "xrd":
                values["xrd_phase"] = "simulated-bcc"
            elif code == "eds":
                values["measured_composition_at_percent"] = {key: 25.0 for key in ("ti_at", "zr_at", "nb_at", "v_at")}
            elif code == "nanoindentation":
                values["hardness_gpa"] = 13.0
            return self._single_output(spec, values)
        if command.adapter_id == "mat_model" and spec.type == "model_inference":
            from app.services.recommendations import RecommendationService

            proposal = RecommendationService(None, self.settings).recommend_for_workflow(context)
            return self._single_output(spec, proposal)
        if command.adapter_id == "manage_db" and spec.type == "data_parse":
            from app.schemas import DataPackageCreate
            from app.services.closed_loop import prepare_data_package

            if len(context.inputs) != 1:
                raise ConflictError("parser requires one declared package-reference input")
            reference = next(iter(context.inputs.values()))
            if not isinstance(reference, dict):
                raise ConflictError("parser input must be a package reference")
            payload = context.execution_payload
            if (reference.get("run_id") not in (None, context.execution_id) or
                    reference.get("experiment_batch_id") not in (None, payload.get("experiment_batch_id")) or
                    reference.get("sample_id") not in (None, payload.get("sample_id"))):
                raise ConflictError("parser reference belongs to another run/sample")
            request = DataPackageCreate.model_validate({**reference, "run_id": context.execution_id,
                "sample_id": payload.get("sample_id"), "validation_status": "validated"})
            _ref, observation, _record = prepare_data_package(request,
                experiment_batch_id=payload.get("experiment_batch_id"), settings=self.settings)
            return self._single_output(spec, observation)
        raise ConflictError("unregistered domain adapter; no hardware fallback")

    @staticmethod
    def _single_output(spec, value):
        if len(spec.outputs) != 1:
            raise ConflictError("domain adapter requires one declared output port")
        return {next(iter(spec.outputs)): value}

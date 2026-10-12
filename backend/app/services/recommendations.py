from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.enums import ExecutionStatus, FeedbackOutcome, ScheduleStatus
from app.materials import (
    DEFAULT_WORKFLOW_STEPS,
    ELEMENT_NAME_MAP,
    QUATERNARY_KEYS,
    composition_deviation,
    extract_predicted_composition,
    extract_predicted_composition_space,
    format_composition_space,
    format_composition,
    is_tizrnbv_hardness_loop,
    measured_composition_from_observation,
    strip_runtime_payload_fields,
    summarize_workflow_progress,
)
from app.models import Execution, RecommendationRecord, Schedule
from app.schemas import RecommendationRead, recommendation_record_to_read
from app.services.closed_loop import ClosedLoopService
from app.services.mat_model_service import MatModelArtifacts, MatModelCandidate, MatModelService


NUMBER_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    match = NUMBER_PATTERN.search(str(value))
    if match is None:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _rounded(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _predicted_space_alignment(
    *,
    payload: dict[str, Any],
    measured: dict[str, float],
) -> tuple[bool | None, list[str], str | None]:
    space = extract_predicted_composition_space(payload)
    if space is None:
        return None, [], None

    elements = space.get("elements")
    if not isinstance(elements, dict):
        return None, [], space.get("summary")

    outside: list[str] = []
    for key in QUATERNARY_KEYS:
        value = _parse_number(measured.get(key))
        stats = elements.get(key)
        if value is None or not isinstance(stats, dict):
            return None, [], space.get("summary")

        lower = _parse_number(stats.get("min"))
        upper = _parse_number(stats.get("max"))
        if lower is None or upper is None:
            return None, [], space.get("summary")

        if value < lower or value > upper:
            label = ELEMENT_NAME_MAP.get(key, key)
            outside.append(f"{label} {value:.3f} 不在 {lower:.3f}-{upper:.3f} at.% 区间内")

    return not outside, outside, space.get("summary")


class RecommendationService:
    def __init__(self, session: Session, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.mat_model = MatModelService(self.settings)

    def recommend_for_workflow(self, context):
        """Pure pinned-context path; no latest schedule query, refresh or legacy writes."""
        payload = context.execution_payload
        return self.mat_model.recommend_for_workflow(model_id=context.model_id, model_version=context.model_version,
            execution_id=context.execution_id, base_payload=payload, inputs=context.inputs,
            used_payloads=payload.get("used_model_payloads", [payload]))

    def list_recommendations(self, *, limit: int = 100) -> list[RecommendationRead]:
        schedules = list(
            self.session.scalars(
                select(Schedule)
                .order_by(Schedule.dynamic_priority.desc(), Schedule.next_run_at.asc())
                .limit(limit)
            )
        )
        if not schedules:
            return []

        schedule_by_id = {item.id: item for item in schedules}
        records = list(
            self.session.scalars(
                select(RecommendationRecord).order_by(RecommendationRecord.updated_at.desc())
            )
        )

        latest_by_schedule: dict[str, RecommendationRecord] = {}
        for record in records:
            if record.schedule_id not in schedule_by_id:
                continue
            if record.schedule_id in latest_by_schedule:
                continue
            latest_by_schedule[record.schedule_id] = record
            if len(latest_by_schedule) >= len(schedule_by_id):
                break

        recommendations: list[RecommendationRead] = []
        for schedule in schedules:
            record = latest_by_schedule.get(schedule.id)
            if record is None:
                continue
            recommendations.append(
                recommendation_record_to_read(
                    record,
                    schedule_name=schedule.name,
                    schedule_status=schedule.status,
                )
            )
        return recommendations

    def recommend_for_schedule(self, schedule: Schedule) -> RecommendationRead:
        from app.workflows.ownership import bound_version_id
        from app.services.exceptions import ConflictError

        if bound_version_id(self.session, schedule.id) is not None:
            raise ConflictError("workflow predictions are owned by their pinned model nodes")
        recommendation = self._recommend_for_schedule(schedule)
        return self._persist_recommendation(schedule, recommendation)

    def _recommend_for_schedule(self, schedule: Schedule) -> RecommendationRead:
        latest_runs = list(
            self.session.scalars(
                select(Execution)
                .where(
                    Execution.schedule_id == schedule.id,
                    Execution.status != ExecutionStatus.AWAITING_FEEDBACK.value,
                )
                .order_by(Execution.dispatched_at.desc())
                .limit(2)
            )
        )

        current_payload = _parse_json(schedule.payload)
        if is_tizrnbv_hardness_loop(current_payload):
            return self._recommend_for_tizrnbv_loop(schedule, current_payload)

        latest = latest_runs[0] if latest_runs else None
        previous = latest_runs[1] if len(latest_runs) > 1 else None
        latest_payload = _parse_json(latest.dispatch_payload) if latest else current_payload.copy()
        latest_observation = _parse_json(latest.feedback_observation) if latest else {}

        suggested_payload = strip_runtime_payload_fields(latest_payload)
        rationale: list[str] = []
        summary = "等待第一轮实验反馈后再生成更强的参数建议。"
        confidence = 0.28
        latest_outcome = None if latest is None else ExecutionStatus(latest.status)

        stage = str(
            latest_payload.get("experiment_stage")
            or current_payload.get("experiment_stage")
            or "optimization"
        ).lower()
        optimize_goal = str(
            latest_payload.get("optimize_goal")
            or current_payload.get("optimize_goal")
            or "maximize"
        ).lower()
        temperature = _parse_number(latest_payload.get("temperature_c"))
        duration = _parse_number(latest_payload.get("duration_h"))
        target_metric = _parse_number(latest_payload.get("target_metric_value"))
        observed_metric = _parse_number(latest_observation.get("metric_value"))
        tolerance = _parse_number(latest_payload.get("target_tolerance")) or 0.0
        quality_score = latest.feedback_score if latest is not None else None
        metric_name = (
            latest_observation.get("metric_name")
            or latest_payload.get("measurement_metric")
            or "关键指标"
        )

        if latest is None:
            if temperature is not None:
                rationale.append(f"先用基线温度 {temperature}°C 跑首轮实验。")
            if duration is not None:
                rationale.append(f"维持基线保温 {duration} h。")
            if not rationale:
                rationale.append("当前建议先执行基线条件，收集第一批反馈。")
        else:
            outcome = FeedbackOutcome(latest.status)
            direction = self._infer_direction(
                latest=latest,
                previous=previous,
                optimize_goal=optimize_goal,
                target_metric=target_metric,
            )
            adjustment = self._metric_adjustment(
                observed_metric=observed_metric,
                target_metric=target_metric,
                optimize_goal=optimize_goal,
                tolerance=tolerance,
            )

            if outcome in {FeedbackOutcome.FAILURE, FeedbackOutcome.TIMEOUT}:
                summary = "上一轮实验失败或超时，建议先收缩工艺窗口并检查仪器状态。"
                confidence = 0.86
                rationale.append("失败或超时说明当前工艺窗口风险偏高，应先回退到稳定区间。")
                if stage in {"annealing", "synthesis"} and temperature is not None:
                    suggested_payload["temperature_c"] = _rounded(max(temperature - 15, 20), 1)
                    rationale.append("温度建议回退 15°C，降低副反应和烧结失控概率。")
                if duration is not None:
                    suggested_payload["duration_h"] = _rounded(max(duration * 0.85, 0.5), 2)
                    rationale.append("保温时间略缩短，优先验证工艺可重复性。")
                suggested_payload["operator_focus"] = "检查炉温校准、气氛控制与样品装载"
            elif adjustment == "met":
                summary = f"{metric_name} 已接近目标，建议沿当前窗口做小步探索。"
                confidence = 0.78
                rationale.append("指标已进入目标附近，下一轮以微调方式寻找局部最优。")
                if temperature is not None and stage in {"annealing", "synthesis"}:
                    step = 5 if direction >= 0 else -5
                    suggested_payload["temperature_c"] = _rounded(max(20, temperature + step), 1)
                    rationale.append("建议在当前方向上小步移动 5°C，确认性能曲线走势。")
            elif adjustment == "raise":
                summary = f"{metric_name} 低于目标，建议提高工艺强度并继续观察。"
                confidence = 0.82 if previous is not None else 0.7
                if temperature is not None and stage in {"annealing", "synthesis"}:
                    step = 10 if direction >= 0 else -10
                    suggested_payload["temperature_c"] = _rounded(max(20, temperature + step), 1)
                    rationale.append("建议沿更可能改善指标的方向移动 10°C。")
                if duration is not None:
                    suggested_payload["duration_h"] = _rounded(duration + 0.5, 2)
                    rationale.append("适度增加保温时间，帮助晶相充分演化。")
            elif adjustment == "lower":
                summary = f"{metric_name} 高于目标窗口，建议回退工艺强度。"
                confidence = 0.82 if previous is not None else 0.7
                if temperature is not None and stage in {"annealing", "synthesis"}:
                    step = -10 if direction <= 0 else 10
                    suggested_payload["temperature_c"] = _rounded(max(20, temperature + step), 1)
                    rationale.append("建议把温度往目标窗口回拉 10°C。")
                if duration is not None:
                    suggested_payload["duration_h"] = _rounded(max(duration - 0.5, 0.5), 2)
                    rationale.append("略微缩短保温时间，避免过度生长或副反应。")
            else:
                summary = "观测值还不足以判断方向，建议保持主体条件并继续采样。"
                confidence = 0.55
                rationale.append("当前数值反馈不足，优先补齐结构化实验观测。")

            if quality_score is not None and quality_score < 0.6:
                rationale.append("质量分偏低，建议优先检查样品重复性和测量噪声。")
                suggested_payload["operator_focus"] = "优先检查样品重复性与测量噪声"
            elif quality_score is not None and quality_score >= 0.9:
                rationale.append("质量分较高，可保持路线并在邻域小步扩展。")

        suggested_payload["recommended_from_execution_id"] = latest.id if latest else None
        suggested_payload["last_metric_name"] = metric_name
        if observed_metric is not None:
            suggested_payload["last_metric_value"] = observed_metric

        return RecommendationRead(
            schedule_id=schedule.id,
            schedule_name=schedule.name,
            schedule_status=ScheduleStatus(schedule.status),
            source_execution_id=None if latest is None else latest.id,
            latest_outcome=latest_outcome,
            confidence=round(confidence, 2),
            summary=summary,
            rationale=rationale[:4],
            suggested_payload=suggested_payload,
        )

    def _persist_recommendation(
        self,
        schedule: Schedule,
        recommendation: RecommendationRead,
    ) -> RecommendationRead:
        fingerprint = hashlib.sha1(
            json.dumps(
                {
                    "source_execution_id": recommendation.source_execution_id,
                    "latest_outcome": None
                    if recommendation.latest_outcome is None
                    else recommendation.latest_outcome.value,
                    "confidence": recommendation.confidence,
                    "summary": recommendation.summary,
                    "rationale": recommendation.rationale,
                    "suggested_payload": recommendation.suggested_payload,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        existing = self.session.scalar(
            select(RecommendationRecord).where(
                RecommendationRecord.schedule_id == schedule.id,
                RecommendationRecord.fingerprint == fingerprint,
            )
        )
        if existing is not None:
            self._persist_local_mat_model_result(
                schedule=schedule,
                recommendation=recommendation,
                recommendation_id=existing.id,
            )
            return recommendation_record_to_read(
                existing,
                schedule_name=schedule.name,
                schedule_status=schedule.status,
            )

        current_payload = _parse_json(schedule.payload)
        campaign = ClosedLoopService(self.session).ensure_campaign_for_schedule(
            schedule,
            current_payload,
        )
        suggested_payload = dict(recommendation.suggested_payload)
        model_version = suggested_payload.get("model_version")
        model_id = "mat_model" if model_version else "heuristic"
        input_dataset_version = (
            suggested_payload.get("input_dataset_version")
            or suggested_payload.get("input_dataset_path")
            or suggested_payload.get("mat_model_output_dir")
        )
        record = RecommendationRecord(
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            task_id=str(current_payload.get("task_id") or schedule.id),
            source_execution_id=recommendation.source_execution_id,
            latest_outcome=None
            if recommendation.latest_outcome is None
            else recommendation.latest_outcome.value,
            confidence=recommendation.confidence,
            summary=recommendation.summary,
            rationale_json=json.dumps(
                recommendation.rationale, ensure_ascii=False, separators=(",", ":")
            ),
            suggested_payload_json=json.dumps(
                suggested_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ),
            model_id=model_id,
            model_version=None if model_version is None else str(model_version),
            input_dataset_version=None
            if input_dataset_version is None
            else str(input_dataset_version),
            approval_status="generated",
            fingerprint=fingerprint,
        )
        self.session.add(record)
        self.session.flush()
        ClosedLoopService(self.session).record_recommendation(record)
        self.session.flush()
        self._persist_local_mat_model_result(
            schedule=schedule,
            recommendation=recommendation,
            recommendation_id=record.id,
        )
        return recommendation_record_to_read(
            record,
            schedule_name=schedule.name,
            schedule_status=schedule.status,
        )

    def _persist_local_mat_model_result(
        self,
        *,
        schedule: Schedule,
        recommendation: RecommendationRead,
        recommendation_id: str,
    ) -> None:
        payload = recommendation.suggested_payload or {}
        if not self._has_mat_model_result(payload):
            return

        batch_id = str(
            payload.get("experiment_batch_id")
            or payload.get("sample_batch")
            or payload.get("experiment_batch_root")
            or recommendation.source_execution_id
            or "unknown_batch"
        )
        round_index = payload.get("experiment_round_index")
        root = Path(self.settings.mat_model_round_result_dir).resolve() / schedule.id
        root.mkdir(parents=True, exist_ok=True)

        document = {
            "recommendation_id": recommendation_id,
            "schedule_id": schedule.id,
            "schedule_name": schedule.name,
            "source_execution_id": recommendation.source_execution_id,
            "latest_outcome": None
            if recommendation.latest_outcome is None
            else recommendation.latest_outcome.value,
            "experiment_batch_id": payload.get("experiment_batch_id"),
            "experiment_round_index": round_index,
            "summary": recommendation.summary,
            "confidence": recommendation.confidence,
            "rationale": recommendation.rationale,
            "next_action": payload.get("next_action"),
            "generated_at": payload.get("mat_model_online_refresh_generated_at"),
            "model_version": payload.get("model_version"),
            "predicted_composition_space": payload.get("predicted_composition_space"),
            "predicted_composition_space_summary": payload.get("predicted_composition_space_summary")
            or payload.get("composition_summary"),
            "predicted_composition_at_percent": payload.get("predicted_composition_at_percent"),
            "mat_model_candidate_rank": payload.get("mat_model_candidate_rank"),
            "mat_model_candidate_source": payload.get("mat_model_candidate_source"),
            "mat_model_candidate_summary": payload.get("mat_model_candidate_summary"),
            "mat_model_candidate_composition_at_percent": payload.get(
                "mat_model_candidate_composition_at_percent"
            ),
            "mat_model_predicted_hardness_gpa": payload.get("mat_model_predicted_hardness_gpa"),
            "mat_model_predicted_std_gpa": payload.get("mat_model_predicted_std_gpa"),
            "mat_model_online_refresh_applied": payload.get("mat_model_online_refresh_applied"),
            "mat_model_online_refresh_sample_count": payload.get(
                "mat_model_online_refresh_sample_count"
            ),
            "payload": payload,
        }

        batch_file = root / f"{batch_id}.json"
        latest_file = root / "latest.json"
        data = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
        batch_file.write_text(data, encoding="utf-8")
        latest_file.write_text(data, encoding="utf-8")

    @staticmethod
    def _has_mat_model_result(payload: dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        return bool(
            payload.get("predicted_composition_space")
            or payload.get("mat_model_candidate_rank") is not None
            or payload.get("mat_model_online_refresh_applied")
            or payload.get("mat_model_candidate_composition_at_percent")
        )

    def _recommend_for_tizrnbv_loop(
        self, schedule: Schedule, current_payload: dict[str, Any]
    ) -> RecommendationRead:
        all_runs = list(
            self.session.scalars(
                select(Execution)
                .where(Execution.schedule_id == schedule.id)
                .order_by(Execution.dispatched_at.desc())
            )
        )
        latest_runs = [
            run for run in all_runs if run.status != ExecutionStatus.AWAITING_FEEDBACK.value
        ][:2]
        awaiting_execution = next(
            (run for run in all_runs if run.status == ExecutionStatus.AWAITING_FEEDBACK.value),
            None,
        )
        latest = latest_runs[0] if latest_runs else None
        if awaiting_execution is not None:
            return self._recommend_for_awaiting_tizrnbv_loop(
                schedule=schedule,
                awaiting_execution=awaiting_execution,
                latest_closed=latest,
            )

        latest_payload = _parse_json(latest.dispatch_payload) if latest else current_payload.copy()
        latest_observation = _parse_json(latest.feedback_observation) if latest else {}
        predicted = extract_predicted_composition(latest_payload) or extract_predicted_composition(
            current_payload
        ) or {}
        measured = measured_composition_from_observation(latest_observation)
        target_hardness = _parse_number(
            latest_payload.get("target_hardness_gpa") or latest_payload.get("target_metric_value")
        )
        hardness = _parse_number(latest_observation.get("hardness_gpa"))
        xrd_phase = latest_observation.get("xrd_phase")
        tolerance = _parse_number(latest_payload.get("composition_tolerance_at")) or 2.0
        workflow_progress = (
            latest_observation.get("workflow_progress")
            if isinstance(latest_observation.get("workflow_progress"), dict)
            else summarize_workflow_progress(latest_payload, latest_observation)
        )
        planned_round_index = len(all_runs) + 1
        used_payloads = [current_payload] + [
            _parse_json(run.dispatch_payload) for run in reversed(all_runs)
        ]

        suggested_payload = strip_runtime_payload_fields(latest_payload or current_payload)
        suggested_payload.setdefault("workflow_steps", DEFAULT_WORKFLOW_STEPS)
        rationale: list[str] = []
        latest_outcome = None if latest is None else ExecutionStatus(latest.status)
        confidence = 0.32
        summary = "先用 mat_model 给出的 Ti-Zr-Nb-V 成分跑一轮完整的制备与表征闭环。"
        mat_model_candidate: MatModelCandidate | None = None
        mat_model_artifacts: MatModelArtifacts | None = None
        mat_model_error: str | None = None

        try:
            self.mat_model.refresh_candidates_from_closed_loop(self.session)
        except Exception:
            pass

        try:
            mat_model_artifacts = self.mat_model.artifacts()
        except FileNotFoundError as exc:
            mat_model_error = str(exc)

        if mat_model_artifacts is not None:
            self._attach_mat_model_refresh_metadata(
                suggested_payload=suggested_payload,
                artifacts=mat_model_artifacts,
            )

        if predicted:
            suggested_payload["predicted_composition_at_percent"] = predicted
            suggested_payload["composition_summary"] = format_composition(predicted)

        if latest is None:
            if mat_model_artifacts is not None:
                mat_model_candidate = self.mat_model.pick_next_candidate(
                    used_payloads=used_payloads,
                    preferred_rank=planned_round_index,
                )
            if mat_model_candidate is not None:
                suggested_payload = self.mat_model.build_schedule_payload(
                    base_payload=current_payload,
                    candidate=mat_model_candidate,
                )
                summary = (
                    f"调度已切到 mat_model，建议先执行第 {mat_model_candidate.rank} 个实验批次候选。"
                )
                confidence = self.mat_model.confidence_for_candidate(mat_model_candidate)
                rationale.extend(
                    self._mat_model_rationale(
                        candidate=mat_model_candidate,
                        artifacts=mat_model_artifacts,
                    )
                )
            elif predicted:
                rationale.append(f"当前待验证设定成分为 {format_composition(predicted)}。")
            rationale.append("本轮应依次完成磁控溅射、XRD、EDS 与纳米压痕四个实验节点。")
            suggested_payload["next_action"] = suggested_payload.get(
                "next_action", "run_full_characterization_loop"
            )
        elif latest.status in {
            ExecutionStatus.FAILURE.value,
            ExecutionStatus.TIMEOUT.value,
        }:
            summary = "本轮实验未完整闭环，优先补齐步骤或排查设备，再回传模型。"
            confidence = 0.88
            rationale.append("失败或超时说明当前流程没有完成全链路反馈。")
            rationale.append("优先检查磁控溅射设备状态、样品编号和表征预约是否齐备。")
            suggested_payload["next_action"] = "recover_workflow_and_repeat"
        else:
            confidence = 0.8
            if not workflow_progress.get("is_complete"):
                current_step = workflow_progress.get("current_step")
                missing_titles = " / ".join(
                    item["title"] for item in workflow_progress.get("missing_steps", [])
                )
                step_actions = {
                    "magnetron_sputtering": "complete_sputter_deposition",
                    "xrd": "complete_xrd",
                    "eds": "complete_eds",
                    "nanoindentation": "complete_nanoindentation",
                }
                step_labels = {
                    "magnetron_sputtering": "磁控溅射样品制备",
                    "xrd": "XRD 表征",
                    "eds": "EDS 表征",
                    "nanoindentation": "纳米压痕",
                }
                summary = (
                    f"本轮实验尚未走完整个四节点流程，当前先补 {step_labels.get(current_step, '缺失工序')}。"
                )
                rationale.append(f"当前缺失步骤: {missing_titles}。")
                rationale.append("三项表征没有齐套前，不建议把不完整结果回传模型。")
                suggested_payload["next_action"] = step_actions.get(
                    current_step, "complete_workflow_step"
                )
                if xrd_phase:
                    rationale.append(f"已记录 XRD 相结构 {xrd_phase}。")
                    suggested_payload["last_xrd_phase"] = xrd_phase
                if measured is not None:
                    suggested_payload["measured_composition_at_percent"] = measured
                if hardness is not None:
                    suggested_payload["last_hardness_gpa"] = hardness
            else:
                composition_outside_space = False
                if measured is None:
                    summary = "EDS 实测成分缺失，下一轮需要先补齐成分反馈。"
                    rationale.append("没有实测成分就无法判断模型成分设定是否真正落到样品上。")
                    suggested_payload["next_action"] = "complete_eds"
                else:
                    max_dev, total_dev = composition_deviation(predicted, measured)
                    rationale.append(
                        f"设定与实测成分最大偏差 {max_dev:.2f} at.%，总偏差 {total_dev:.2f} at.%。"
                    )
                    suggested_payload["measured_composition_at_percent"] = measured
                    suggested_payload["next_action"] = "rerun_model_inference"

                    within_space, outside_details, space_summary = _predicted_space_alignment(
                        payload=latest_payload or current_payload,
                        measured=measured,
                    )
                    if within_space is True:
                        suggested_payload["composition_space_alignment"] = "within"
                    elif within_space is False:
                        suggested_payload["composition_space_alignment"] = "outside"
                        composition_outside_space = True
                    else:
                        suggested_payload["composition_space_alignment"] = "unknown"

                    if max_dev > tolerance:
                        rationale.append(
                            "当前批次按成分空间推进，单点 EDS 偏差只作为告警，不再单独阻断 mat_model 下一轮。"
                        )
                        if within_space is True:
                            rationale.append("实测汇总成分仍位于当前预测空间内，可继续按空间闭环推进。")
                        elif within_space is False:
                            detail = "；".join(outside_details[:2])
                            rationale.append(
                                "实测汇总成分已偏出当前预测空间，说明沉积映射存在漂移；"
                                "系统继续推进模型候选，同时建议并行检查溅射校准。"
                            )
                            if detail:
                                rationale.append(detail + "。")
                            if space_summary:
                                suggested_payload["predicted_composition_space_summary"] = (
                                    space_summary
                                )
                        else:
                            rationale.append("当前仍按高通量成分空间闭环推进，不依据单个汇总点直接停掉下一轮。")
                    else:
                        suggested_payload["next_action"] = "rerun_model_inference"
                        rationale.append("成分命中度可接受，可以把结构和硬度反馈回模型。")

                if xrd_phase:
                    rationale.append(f"XRD 当前记录相结构为 {xrd_phase}。")
                    suggested_payload["last_xrd_phase"] = xrd_phase
                else:
                    rationale.append("建议在反馈里补充 XRD 相结构描述，例如 bcc、amorphous 或多相。")

                if hardness is None:
                    if (
                        measured is not None
                        and not composition_misaligned
                        and suggested_payload.get("next_action") == "rerun_model_inference"
                    ):
                        summary = "成分已闭环，但硬度缺失，先补纳米压痕数据再更新模型。"
                        suggested_payload["next_action"] = "complete_nanoindentation"
                    rationale.append("当前缺少纳米压痕硬度，模型还无法按性能目标更新。")
                else:
                    suggested_payload["last_hardness_gpa"] = hardness
                    if target_hardness is not None:
                        suggested_payload["target_hardness_gpa"] = target_hardness
                        if composition_outside_space:
                            rationale.append(
                                "当前汇总成分已偏出预测空间，但这不再阻断模型推进；建议并行复核溅射校准。"
                            )
                        if hardness >= target_hardness and mat_model_artifacts is None:
                            summary = (
                                f"当前硬度 {hardness:.2f} GPa 已达到目标 {target_hardness:.2f} GPa，"
                                "但 mat_model 资产当前不可用，建议先复验命中点。"
                            )
                            rationale.append("命中目标后需要靠 mat_model 邻域候选继续展开，但当前模型产物未加载成功。")
                            suggested_payload["next_action"] = "repeat_best_candidate"
                            confidence = 0.74
                        elif hardness >= target_hardness and mat_model_artifacts is not None:
                            anchor = measured or predicted
                            if anchor is not None:
                                mat_model_candidate = self.mat_model.pick_local_candidate(
                                    anchor_composition=anchor,
                                    used_payloads=used_payloads,
                                )
                            if mat_model_candidate is None:
                                summary = (
                                    f"当前硬度 {hardness:.2f} GPa 已达到目标 {target_hardness:.2f} GPa，"
                                    "建议先复验命中点，并在需要时刷新 mat_model 候选池。"
                                )
                                rationale.append("性能已命中目标，当前更偏向重复性验证而不是大步跳点。")
                                suggested_payload["next_action"] = "repeat_best_candidate"
                                confidence = 0.86
                            else:
                                suggested_payload = self.mat_model.build_schedule_payload(
                                    base_payload=suggested_payload,
                                    candidate=mat_model_candidate,
                                )
                                summary = (
                                    f"当前硬度 {hardness:.2f} GPa 已达到目标 {target_hardness:.2f} GPa，"
                                    "下一轮建议按 mat_model 在命中点邻域做局部验证。"
                                )
                                rationale.append("性能已命中目标，下一轮改用 mat_model 邻域候选验证重复性和局部梯度。")
                                rationale.extend(
                                    self._mat_model_rationale(
                                        candidate=mat_model_candidate,
                                        artifacts=mat_model_artifacts,
                                    )
                                )
                                confidence = max(
                                    0.78,
                                    self.mat_model.confidence_for_candidate(mat_model_candidate),
                                )
                        else:
                            if mat_model_artifacts is None:
                                summary = (
                                    f"当前硬度 {hardness:.2f} GPa 低于目标 {target_hardness:.2f} GPa，"
                                    "但 mat_model 资产当前不可用，先修复模型产物后再派发下一轮。"
                                )
                                rationale.append("闭环标签已经齐备，但当前无法读取 mat_model 候选库。")
                                suggested_payload["next_action"] = "refresh_mat_model_pipeline"
                                confidence = 0.5
                            else:
                                mat_model_candidate = self.mat_model.pick_next_candidate(
                                    used_payloads=used_payloads,
                                    preferred_rank=planned_round_index,
                                )
                            if mat_model_artifacts is not None and mat_model_candidate is None:
                                summary = (
                                    f"当前硬度 {hardness:.2f} GPa 低于目标 {target_hardness:.2f} GPa，"
                                    "但本地 mat_model 候选池已经用完，建议重新运行 pipeline。"
                                )
                                rationale.append("闭环标签已经齐备，但没有可继续派发的候选点。")
                                suggested_payload["next_action"] = "refresh_mat_model_pipeline"
                                confidence = 0.55
                            else:
                                suggested_payload = self.mat_model.build_schedule_payload(
                                    base_payload=suggested_payload,
                                    candidate=mat_model_candidate,
                                )
                                summary = (
                                    f"当前硬度 {hardness:.2f} GPa 低于目标 {target_hardness:.2f} GPa，"
                                    "下一轮建议直接切到 mat_model 候选。"
                                )
                                rationale.append("当前闭环已经拿到完整性能标签，下一轮改由 mat_model 候选继续搜索。")
                                rationale.extend(
                                    self._mat_model_rationale(
                                        candidate=mat_model_candidate,
                                        artifacts=mat_model_artifacts,
                                    )
                                )
                                confidence = self.mat_model.confidence_for_candidate(
                                    mat_model_candidate
                                )
                    else:
                        if mat_model_artifacts is None:
                            summary = f"当前硬度 {hardness:.2f} GPa 已记录，但 mat_model 资产当前不可用。"
                            rationale.append("建议先修复模型产物，再继续派发下一轮。")
                            suggested_payload["next_action"] = "refresh_mat_model_pipeline"
                            confidence = 0.5
                        else:
                            mat_model_candidate = self.mat_model.pick_next_candidate(
                                used_payloads=used_payloads,
                                preferred_rank=planned_round_index,
                            )
                        if mat_model_artifacts is not None and mat_model_candidate is None:
                            summary = f"当前硬度 {hardness:.2f} GPa 已记录，但 mat_model 候选池已耗尽。"
                            rationale.append("建议先刷新模型输出，再继续派发下一轮。")
                            suggested_payload["next_action"] = "refresh_mat_model_pipeline"
                            confidence = 0.55
                        elif mat_model_candidate is not None:
                            suggested_payload = self.mat_model.build_schedule_payload(
                                base_payload=suggested_payload,
                                candidate=mat_model_candidate,
                            )
                            summary = f"当前硬度 {hardness:.2f} GPa 已记录，下一轮继续执行 mat_model 候选。"
                            rationale.append("建议补一个目标硬度，后续就能自动判断是否命中。")
                            rationale.extend(
                                self._mat_model_rationale(
                                    candidate=mat_model_candidate,
                                    artifacts=mat_model_artifacts,
                                )
                            )
                            confidence = self.mat_model.confidence_for_candidate(
                                mat_model_candidate
                            )

        if mat_model_error is not None:
            rationale.append(f"mat_model 资产暂不可用: {mat_model_error}")

        if (
            latest is not None
            and bool(workflow_progress.get("is_complete"))
            and suggested_payload.get("next_action") == "run_mat_model_candidate"
            and suggested_payload.get("mat_model_candidate_rank") is None
        ):
            suggested_payload["next_action"] = "refresh_mat_model_pipeline"
            summary = "本轮闭环结果已齐，但 mat_model 尚未完成本轮刷新，暂不自动派发下一轮。"
            confidence = min(confidence, 0.58)
            rationale.insert(
                0,
                "只有当本轮闭环结果真正进入 mat_model 刷新后，系统才会自动派发下一轮候选。",
            )

        return RecommendationRead(
            schedule_id=schedule.id,
            schedule_name=schedule.name,
            schedule_status=ScheduleStatus(schedule.status),
            source_execution_id=None if latest is None else latest.id,
            latest_outcome=latest_outcome,
            confidence=round(confidence, 2),
            summary=summary,
            rationale=rationale[:5],
            suggested_payload=suggested_payload,
        )

    @staticmethod
    def _attach_mat_model_refresh_metadata(
        *,
        suggested_payload: dict[str, Any],
        artifacts: MatModelArtifacts,
    ) -> None:
        refresh = artifacts.online_refresh_metadata
        if not isinstance(refresh, dict) or not refresh.get("applied"):
            return

        suggested_payload["mat_model_online_refresh_applied"] = True
        suggested_payload["mat_model_online_refresh_sample_count"] = refresh.get(
            "closed_loop_sample_count"
        )
        suggested_payload["mat_model_online_refresh_generated_at"] = refresh.get("generated_at")

        manage_db_sync = refresh.get("manage_db_sync")
        if isinstance(manage_db_sync, dict):
            suggested_payload["mat_model_manage_db_training_row_count"] = manage_db_sync.get(
                "training_row_count"
            )
            suggested_payload["mat_model_manage_db_downloaded_record_count"] = manage_db_sync.get(
                "downloaded_record_count"
            )

    def _recommend_for_awaiting_tizrnbv_loop(
        self,
        *,
        schedule: Schedule,
        awaiting_execution: Execution,
        latest_closed: Execution | None,
    ) -> RecommendationRead:
        payload = _parse_json(awaiting_execution.dispatch_payload)
        suggested_payload = strip_runtime_payload_fields(payload)
        rationale: list[str] = ["当前候选已经派发，系统正在等待本轮实验结果回写。"]

        if payload.get("mat_model_candidate_rank") is not None:
            rationale.append(
                f"当前是 mat_model 第 {int(payload['mat_model_candidate_rank'])} 个候选。"
            )
        composition_space = extract_predicted_composition_space(payload)
        if composition_space is not None:
            summary = composition_space.get("summary") or format_composition_space(
                composition_space
            )
            grid_shape = composition_space.get("grid_shape") or {}
            total_samples = grid_shape.get("total_samples")
            rationale.append(
                f"当前轮次成分空间为 {summary}，批次规模 {int(total_samples or 0)} 个样本。"
            )
        else:
            composition = extract_predicted_composition(payload)
            if composition:
                rationale.append(f"当前候选成分为 {format_composition(composition)}。")
        predicted_hardness = _parse_number(payload.get("mat_model_predicted_hardness_gpa"))
        predicted_std = _parse_number(payload.get("mat_model_predicted_std_gpa"))
        if predicted_hardness is not None and predicted_std is not None:
            rationale.append(
                f"模型预测硬度 {predicted_hardness:.2f} GPa，预测标准差 {predicted_std:.2f} GPa。"
            )

        return RecommendationRead(
            schedule_id=schedule.id,
            schedule_name=schedule.name,
            schedule_status=ScheduleStatus(schedule.status),
            source_execution_id=awaiting_execution.id,
            latest_outcome=ExecutionStatus.AWAITING_FEEDBACK,
            confidence=max(0.72, self.mat_model.confidence_for_candidate(None)),
            summary="下一轮 mat_model 候选已派发，等待本轮实验反馈。",
            rationale=rationale[:5],
            suggested_payload=suggested_payload,
        )

    @staticmethod
    def _mat_model_rationale(
        *, candidate: MatModelCandidate, artifacts: MatModelArtifacts | None
    ) -> list[str]:
        lines: list[str] = []
        if artifacts is not None:
            training_rows = artifacts.summary.get("n_training_rows")
            cv_rmse = artifacts.summary.get("cv_rmse_mean")
            if training_rows is not None and cv_rmse is not None:
                lines.append(
                    f"mat_model 当前使用 {artifacts.run_name}，训练样本 {training_rows} 条，CV RMSE {float(cv_rmse):.2f} GPa。"
                )
            refresh = artifacts.online_refresh_metadata
            if refresh and refresh.get("applied"):
                sample_count = refresh.get("closed_loop_sample_count")
                if sample_count is not None:
                    lines.append(f"增量优化已纳入 {int(sample_count)} 条闭环样本。")
                manage_db_sync = refresh.get("manage_db_sync")
                if isinstance(manage_db_sync, dict):
                    training_row_count = manage_db_sync.get("training_row_count")
                    downloaded_record_count = manage_db_sync.get("downloaded_record_count")
                    if training_row_count:
                        lines.append(
                            f"推理前已从 manage_db 同步 {int(downloaded_record_count or 0)} 条记录，并解析出 {int(training_row_count)} 条可训练样本。"
                        )
            interval_summary = artifacts.optimal_intervals.get("elements")
            if isinstance(interval_summary, dict):
                space = {
                    "elements": {
                        "ti_at": interval_summary.get("Ti", {}),
                        "zr_at": interval_summary.get("Zr", {}),
                        "nb_at": interval_summary.get("Nb", {}),
                        "v_at": interval_summary.get("V", {}),
                    }
                }
                lines.append(
                    "本轮建议先按预测空间展开 13x13 高通量批次: "
                    + format_composition_space(space)
                    + "。"
                )
        if candidate.role_label:
            lines.append(f"候选来源: {candidate.role_label}。")
        elif candidate.rank is not None:
            lines.append(f"候选来源: reliable candidate 排序第 {candidate.rank} 位。")
        hardness = candidate.predicted_hardness_gpa
        std = candidate.predicted_std_gpa
        if hardness is not None and std is not None:
            lines.append(
                f"代表点 {candidate.composition_summary}，预测硬度 {hardness:.2f} GPa，预测标准差 {std:.2f} GPa。"
            )
        power_terms = []
        for element in ("Ti", "Zr", "Nb", "V"):
            value = candidate.row.get(f"{element}_power_w")
            if value is not None:
                power_terms.append(f"{element} {float(value):.1f} W")
        if power_terms:
            lines.append("建议靶功率: " + " / ".join(power_terms) + "。")
        return lines

    @staticmethod
    def _metric_adjustment(
        *,
        observed_metric: float | None,
        target_metric: float | None,
        optimize_goal: str,
        tolerance: float,
    ) -> str:
        if observed_metric is None or target_metric is None:
            return "unknown"

        if optimize_goal == "maximize":
            return "met" if observed_metric >= target_metric - tolerance else "raise"
        if optimize_goal == "minimize":
            return "met" if observed_metric <= target_metric + tolerance else "lower"

        delta = observed_metric - target_metric
        if math.fabs(delta) <= max(tolerance, 1e-9):
            return "met"
        return "raise" if delta < 0 else "lower"

    @staticmethod
    def _infer_direction(
        *,
        latest: Execution,
        previous: Execution | None,
        optimize_goal: str,
        target_metric: float | None,
    ) -> int:
        if previous is None:
            return 1

        latest_payload = _parse_json(latest.dispatch_payload)
        previous_payload = _parse_json(previous.dispatch_payload)
        latest_observation = _parse_json(latest.feedback_observation)
        previous_observation = _parse_json(previous.feedback_observation)

        latest_temperature = _parse_number(latest_payload.get("temperature_c"))
        previous_temperature = _parse_number(previous_payload.get("temperature_c"))
        latest_metric = _parse_number(latest_observation.get("metric_value"))
        previous_metric = _parse_number(previous_observation.get("metric_value"))

        if (
            latest_temperature is None
            or previous_temperature is None
            or latest_metric is None
            or previous_metric is None
            or latest_temperature == previous_temperature
        ):
            return 1

        if optimize_goal == "maximize":
            improved = latest_metric > previous_metric
        elif optimize_goal == "minimize":
            improved = latest_metric < previous_metric
        else:
            if target_metric is None:
                improved = latest_metric > previous_metric
            else:
                improved = abs(latest_metric - target_metric) < abs(previous_metric - target_metric)

        moved_up = latest_temperature > previous_temperature
        if improved and moved_up:
            return 1
        if improved and not moved_up:
            return -1
        if not improved and moved_up:
            return -1
        return 1

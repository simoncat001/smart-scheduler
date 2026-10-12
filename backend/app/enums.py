from __future__ import annotations

from enum import StrEnum


class ScheduleStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"


class ExecutionStatus(StrEnum):
    AWAITING_FEEDBACK = "awaiting_feedback"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILURE = "failure"
    TIMEOUT = "timeout"


class FeedbackOutcome(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILURE = "failure"
    TIMEOUT = "timeout"

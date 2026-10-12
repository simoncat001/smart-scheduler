from datetime import UTC
import hashlib
import json

from sqlalchemy import select

from app.models import utcnow
from app.workflows.definitions import canonical_json
from app.workflows.engine import db_time
from app.workflows.messaging import MessagingService
from app.workflows.models import MessageInbox


ROLES = ("engine", "publisher", "events", "model", "parser", "simulator")


def record_heartbeat(session, role, executor_id, now, *, instance_id="default", topic_prefix="scheduler", online=True):
    if role not in ROLES:
        raise ValueError("invalid heartbeat role")
    identity = hashlib.sha256(f"{topic_prefix}:{role}:{executor_id}:{instance_id}".encode()).hexdigest()
    row, _fresh = MessagingService(session, topic_prefix)._reserve("worker-heartbeat", identity, "0" * 64, "{}", db_time(now))
    row.status = "online" if online else "offline"
    row.created_at = db_time(now)
    row.envelope_json = canonical_json({"role": role, "executor_id": executor_id, "instance_id": instance_id,
        "topic_prefix": topic_prefix, "seen_at": db_time(now).replace(tzinfo=UTC).isoformat()})
    session.flush()


def worker_health(session, now=None, *, topic_prefix="scheduler", stale_seconds=60):
    now = db_time(now or utcnow())
    result = {role: {"status": "unknown", "last_seen_at": None, "instances": []} for role in ROLES}
    rows = session.scalars(select(MessageInbox).where(MessageInbox.handler == "worker-heartbeat")
                          .order_by(MessageInbox.created_at.desc()).limit(1000))
    for row in rows:
        data = json.loads(row.envelope_json)
        if data.get("topic_prefix") != topic_prefix or data.get("role") not in result:
            continue
        entry = result[data["role"]]
        online = row.status == "online" and (now - row.created_at).total_seconds() <= stale_seconds
        entry["instances"].append({"executor_id": data["executor_id"], "instance_id": data["instance_id"],
                                   "last_seen_at": data["seen_at"], "status": "online" if online else "offline"})
        if entry["last_seen_at"] is None:
            entry["last_seen_at"] = data["seen_at"]
            entry["status"] = "offline"
        if online:
            entry["status"] = "online"
    return result

from __future__ import annotations

import argparse
import sys
import time
import os
import json
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.models import utcnow
from app.workflows.engine import WorkflowEngine
from app.workflows.models import WorkflowRun, require_message_id_schema


def engine_cycle(database, topic_prefix, *, scheduler_mode="inline", scheduler_enabled=True):
    if scheduler_enabled:
        with database.session_factory.begin() as session:
            WorkflowEngine(session, topic_prefix).tick_schedules(utcnow())
        if scheduler_mode == "external":
            from app.services.scheduler import SchedulerService

            with database.session_factory() as session:
                SchedulerService(session).dispatch_due_schedules()
    with database.session_factory() as session:
        run_ids = list(session.scalars(select(WorkflowRun.execution_id).where(WorkflowRun.status == "running")))
    for run_id in run_ids:
        with database.session_factory.begin() as session:
            WorkflowEngine(session, topic_prefix).advance(run_id, utcnow())
    with database.session_factory.begin() as session:
        WorkflowEngine(session, topic_prefix).expire(utcnow())


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Explicit MySQL/Kafka workflow workers; no device resource management")
    parser.add_argument("role", choices=["engine", "publisher", "events", "model", "parser", "simulator"])
    parser.add_argument("--once", action="store_true", help="Perform one bounded worker cycle and exit")
    parser.add_argument("--executor-id", default=os.getenv("SMART_CHEDULER_WORKER_EXECUTOR_ID"), help="Exact gateway ID for domain workers")
    parser.add_argument("--enable-simulator", action="store_true", help="Explicitly enable simulated device jobs, never real adapters")
    parser.add_argument("--job-lease-seconds", type=int, default=120)
    parser.add_argument("--describe-model", action="store_true", help="Print read-only pinned MAT model identity without starting workers")
    args = parser.parse_args(argv)
    from app.config import get_settings
    from app.database import Database
    from app.workflows.kafka import KafkaTransport, create_command_consumer, create_event_consumer
    from app.workflows.messaging import EventConsumer, OutboxPublisher
    from app.workflows.heartbeats import record_heartbeat
    from confluent_kafka import KafkaException

    database = consumer = transport = runner = None
    instance_id = str(uuid4())

    def heartbeat(online=True):
        with database.session_factory() as session, session.begin():
            record_heartbeat(session, args.role, args.executor_id or args.role, utcnow(),
                instance_id=instance_id, topic_prefix=settings.kafka.topic_prefix, online=online)

    def receiver_for_role():
        nonlocal consumer
        if args.role == "events":
            consumer = create_event_consumer(settings.kafka)
            return EventConsumer(database.session_factory, consumer, settings.kafka.topic_prefix)
        from app.workflows.executors import CommandConsumer

        consumer = create_command_consumer(settings.kafka, args.role, args.executor_id)
        return CommandConsumer(database.session_factory, consumer, role=args.role, executor_id=args.executor_id,
            simulator_enabled=args.enable_simulator, topic_prefix=settings.kafka.topic_prefix)
    try:
        settings = get_settings()
        if args.describe_model:
            if args.role != "model":
                print("Model description requires the model role.", file=sys.stderr)
                return 1
            from app.services.mat_model_service import MatModelService

            print(json.dumps({"model_id": "mat-materials", "model_version": MatModelService(settings).workflow_model_version()}))
            return 0
        if args.role in {"model", "parser", "simulator"}:
            if not args.executor_id:
                print("Domain worker requires --executor-id matching its gateway binding.", file=sys.stderr)
                return 1
            if args.role == "simulator" and not args.enable_simulator:
                print("Simulator requires explicit --enable-simulator; no device fallback.", file=sys.stderr)
                return 1
            from pydantic import TypeAdapter
            from app.workflows.schemas import Identifier

            TypeAdapter(Identifier).validate_python(args.executor_id)
            if not 5 <= args.job_lease_seconds <= 3600:
                raise ValueError("invalid domain job lease")
        database = Database(settings.database_url)
        if database.engine.dialect.name != "mysql":
            print("Workflow workers require MySQL. No SQLite runtime fallback.", file=sys.stderr)
            return 1
        require_message_id_schema(database.engine)
        if args.role == "publisher":
            transport = KafkaTransport(settings.kafka)
            sender = OutboxPublisher(database.session_factory, transport, settings.kafka.outbox_lease_seconds)
        elif args.role in {"events", "model", "parser", "simulator"}:
            receiver = receiver_for_role()
            if args.role != "events":
                from app.workflows.adapters import AdapterRegistry
                from app.workflows.executors import ExecutorRunner

                runner = ExecutorRunner(database.session_factory, AdapterRegistry(settings, simulator_enabled=args.enable_simulator),
                    role=args.role, executor_id=args.executor_id, simulator_enabled=args.enable_simulator,
                    topic_prefix=settings.kafka.topic_prefix, lease_seconds=args.job_lease_seconds)
                if not args.once:
                    runner.start(settings.kafka.poll_interval_seconds)
        while True:
            try:
                heartbeat()
                if args.role == "engine":
                    engine_cycle(database, settings.kafka.topic_prefix,
                                 scheduler_mode=settings.scheduler_mode, scheduler_enabled=settings.scheduler_enabled)
                elif args.role == "publisher":
                    sender.publish_batch(utcnow())
                else:
                    receiver.process_one()
                    if args.once and runner is not None:
                        runner.execute_one()
            except (SQLAlchemyError, RuntimeError, KafkaException):
                print("Worker cycle failed; committed state retained and unhandled message not acknowledged.", file=sys.stderr)
                if args.once:
                    return 1
                if args.role in {"events", "model", "parser", "simulator"}:
                    # Rejoin from committed offsets after outages/rebalances; inbox dedup covers redelivery.
                    consumer.close()
                    consumer = None
                    receiver = receiver_for_role()
            if args.once:
                return 0
            time.sleep(settings.kafka.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0
    except Exception:
        print("Workflow worker failed; check MySQL, Kafka and worker configuration.", file=sys.stderr)
        return 1
    finally:
        if runner is not None:
            runner.stop()
        if database is not None and database.engine.dialect.name == "mysql":
            try:
                heartbeat(online=False)
            except Exception:
                pass
        if consumer is not None:
            consumer.close()
        if transport is not None:
            transport.close()
        if database is not None:
            database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

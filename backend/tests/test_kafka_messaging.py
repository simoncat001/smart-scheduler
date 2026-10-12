from datetime import timedelta
import importlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from app.database import Database
from app.workflows.models import MessageInbox, MessageOutbox, NodeRun
from app.workflows.schemas import EventDisposition
from workflow_fixtures import NOW, event_for, make_execution, node_run, outboxes


@pytest.fixture()
def database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'messaging.db'}")
    database.create_schema()
    yield database
    database.dispose()


def seed(database):
    from app.workflows.engine import WorkflowEngine

    with database.session_factory.begin() as session:
        execution, version = make_execution(session)
        run = WorkflowEngine(session).start(execution, version, NOW)
        event = event_for(session, run.execution_id, "magnetron_sputtering", {"sample": {"sample_id": "a"}})
        return run.execution_id, event


def messaging():
    return importlib.import_module("app.workflows.messaging")


def test_inbox_deduplicates_results_and_downstream_commands(database):
    run_id, result = seed(database)
    for disposition in (EventDisposition.APPLIED, EventDisposition.DUPLICATE):
        with database.session_factory.begin() as session:
            assert messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW) == disposition
    with database.session_factory() as session:
        assert len(outboxes(session)) == 4
        assert len(list(session.scalars(select(MessageInbox)))) == 1
        assert node_run(session, run_id, "xrd").status == "queued"


def test_message_id_reuse_with_changed_payload_is_quarantined(database):
    run_id, result = seed(database)
    with database.session_factory.begin() as session:
        messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW)
    conflicting = result.model_copy(update={"payload": {"outputs": {"sample": {"sample_id": "different"}}}})
    with database.session_factory.begin() as session:
        assert messaging().MessagingService(session).ingest(conflicting.model_dump_json().encode(), "events", NOW) == EventDisposition.QUARANTINED
    with database.session_factory() as session:
        assert json.loads(node_run(session, run_id, "magnetron_sputtering").outputs_json)["sample"]["sample_id"] == "a"
        assert len([row for row in outboxes(session) if row.topic == "scheduler.dlq"]) == 1


@pytest.mark.parametrize("raw", [b"not json", b"null", b'{"schema_version":9}', b"x" * 1_000_001],
                         ids=["invalid-json", "null", "unknown-version", "oversized"])
def test_poison_message_is_durably_quarantined_and_not_replayed(database, raw):
    with database.session_factory.begin() as session:
        assert messaging().MessagingService(session).ingest(raw, "events", NOW) == EventDisposition.QUARANTINED
    with database.session_factory.begin() as session:
        assert messaging().MessagingService(session).ingest(raw, "events", NOW) == EventDisposition.DUPLICATE
    with database.session_factory() as session:
        assert len(list(session.scalars(select(MessageInbox)))) == 1
        assert len(outboxes(session)) == 1
        assert len(outboxes(session)[0].envelope_json) < 100_000


def test_invalid_result_goes_to_dlq_without_mutating_node(database):
    run_id, result = seed(database)
    result = result.model_copy(update={"payload": {"outputs": {"sample": {}}}})
    with database.session_factory.begin() as session:
        assert messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW) == EventDisposition.QUARANTINED
    with database.session_factory() as session:
        assert node_run(session, run_id, "magnetron_sputtering").status == "queued"
        assert len(outboxes(session)) == 2


class TransportDouble:
    def __init__(self, error=False):
        self.sent = []
        self.error = error

    def publish(self, topic, key, value):
        self.sent.append((topic, key, value))
        if self.error:
            raise RuntimeError("broker refused delivery")


def publisher(database, transport, **kwargs):
    return messaging().OutboxPublisher(database.session_factory, transport, clock=lambda: NOW, **kwargs)


def test_committed_outbox_is_sent_and_marked_only_after_delivery(database):
    seed(database)
    transport = TransportDouble()
    assert publisher(database, transport).publish_batch(NOW) == 1
    with database.session_factory() as session:
        row = outboxes(session)[0]
        assert row.status == "sent"
        assert row.sent_at == NOW
        assert json.loads(transport.sent[0][2])["command_id"] == row.message_id


def test_failed_publication_retries_same_command_id_after_backoff(database):
    seed(database)
    transport = TransportDouble(error=True)
    sender = publisher(database, transport)
    assert sender.publish_batch(NOW) == 0
    assert sender.publish_batch(NOW) == 0
    assert len(transport.sent) == 1
    transport.error = False
    assert sender.publish_batch(NOW + timedelta(seconds=10)) == 1
    assert json.loads(transport.sent[0][2])["command_id"] == json.loads(transport.sent[1][2])["command_id"]


def test_uncommitted_command_is_not_visible_to_publisher(database):
    from app.workflows.engine import WorkflowEngine

    with database.session_factory() as session:
        execution, version = make_execution(session)
        WorkflowEngine(session).start(execution, version, NOW)
        transport = TransportDouble()
        assert publisher(database, transport).publish_batch(NOW) == 0
        assert not transport.sent
        session.rollback()


def test_publisher_recovers_expired_claim_after_crash_without_new_command(database):
    seed(database)
    with database.session_factory.begin() as session:
        row = outboxes(session)[0]
        row.status, row.lease_owner, row.leased_until = "publishing", "old-worker", NOW + timedelta(seconds=20)
        command_id = row.message_id
    transport = TransportDouble()
    sender = publisher(database, transport)
    assert sender.publish_batch(NOW) == 0
    assert sender.publish_batch(NOW + timedelta(seconds=21)) == 1
    assert json.loads(transport.sent[0][2])["command_id"] == command_id


def test_expired_command_is_not_sent_to_gateway(database):
    seed(database)
    transport = TransportDouble()
    assert publisher(database, transport).publish_batch(NOW + timedelta(hours=1)) == 0
    assert not transport.sent
    with database.session_factory() as session:
        assert outboxes(session)[0].status == "expired"


def test_recovery_after_broker_ack_before_db_mark_uses_identical_command(database):
    seed(database)
    transport = TransportDouble()
    sender = publisher(database, transport)
    original = sender._finish

    def unavailable(*args):
        raise SQLAlchemyError("database unavailable after broker ack")

    sender._finish = unavailable
    with pytest.raises(SQLAlchemyError):
        sender.publish_batch(NOW)
    sender._finish = original
    assert sender.publish_batch(NOW + timedelta(seconds=61)) == 1
    assert len(transport.sent) == 2
    assert transport.sent[0][2] == transport.sent[1][2]


def test_competing_publisher_cannot_claim_active_lease(database):
    seed(database)
    other_transport = TransportDouble()
    other = publisher(database, other_transport)
    transport = TransportDouble()
    original = transport.publish

    def publish(*args):
        assert other.publish_batch(NOW) == 0
        original(*args)

    transport.publish = publish
    assert publisher(database, transport).publish_batch(NOW) == 1
    assert not other_transport.sent


def test_completed_command_is_not_replayed_even_if_outbox_was_pending(database):
    _, result = seed(database)
    with database.session_factory.begin() as session:
        messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW)
    transport = TransportDouble()
    assert publisher(database, transport).publish_batch(NOW) == 3
    assert all(json.loads(item[2])["node_id"] != "magnetron_sputtering" for item in transport.sent)


def test_publisher_rechecks_clock_for_each_command_in_a_batch(database):
    _, result = seed(database)
    with database.session_factory.begin() as session:
        messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW)
    current = [NOW]
    transport = TransportDouble()
    original = transport.publish

    def publish(*args):
        original(*args)
        current[0] = NOW + timedelta(hours=1)

    transport.publish = publish
    sender = messaging().OutboxPublisher(database.session_factory, transport, clock=lambda: current[0])
    assert sender.publish_batch(NOW) == 1
    assert len(transport.sent) == 1


def test_inbox_reservation_rolls_back_when_state_application_fails(database, monkeypatch):
    from app.workflows.engine import WorkflowEngine

    _, result = seed(database)

    def failed(*_):
        raise RuntimeError("state update failed")

    monkeypatch.setattr(WorkflowEngine, "apply_event", failed)
    with pytest.raises(RuntimeError):
        with database.session_factory.begin() as session:
            messaging().MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW)
    with database.session_factory() as session:
        assert not list(session.scalars(select(MessageInbox)))
        assert len(outboxes(session)) == 1


class BrokerMessage:
    def __init__(self, raw, offset=0):
        self.raw, self.number = raw, offset

    def value(self):
        return self.raw

    def topic(self):
        return "scheduler.events"

    def partition(self):
        return 0

    def offset(self):
        return self.number

    def error(self):
        return None


class ConsumerDouble:
    def __init__(self, messages):
        self.messages = list(messages)
        self.committed = []
        self.polls = 0
        self.commit_error = False
        self.closed = False

    def close(self):
        self.closed = True

    def poll(self, timeout):
        self.polls += 1
        return self.messages.pop(0) if self.messages else None

    def commit(self, message, asynchronous):
        assert asynchronous is False
        if self.commit_error:
            raise RuntimeError("offset commit failed")
        self.committed.append(message.offset())
        return []


def test_consumer_commits_database_before_offset(database):
    _, result = seed(database)
    consumer = ConsumerDouble([BrokerMessage(result.model_dump_json().encode())])
    order = []
    event.listen(database.session_factory.class_, "after_commit", lambda *_: order.append("db"))
    original = consumer.commit

    def commit(**kwargs):
        order.append("offset")
        return original(**kwargs)

    consumer.commit = commit
    receiver = messaging().EventConsumer(database.session_factory, consumer, clock=lambda: NOW)
    assert receiver.process_one()
    assert order[-2:] == ["db", "offset"]
    assert consumer.committed == [0]


def test_database_failure_keeps_same_message_before_polling_next_offset(database):
    _, result = seed(database)
    consumer = ConsumerDouble([BrokerMessage(result.model_dump_json().encode()), BrokerMessage(b"bad", 1)])

    def unavailable():
        raise SQLAlchemyError("database unavailable")

    receiver = messaging().EventConsumer(unavailable, consumer, clock=lambda: NOW)
    with pytest.raises(SQLAlchemyError):
        receiver.process_one()
    assert consumer.committed == []
    receiver.session_factory = database.session_factory
    assert receiver.process_one()
    assert consumer.polls == 1
    assert consumer.committed == [0]


def test_offset_failure_retries_same_message_without_duplicate_commands(database):
    _, result = seed(database)
    consumer = ConsumerDouble([BrokerMessage(result.model_dump_json().encode())])
    consumer.commit_error = True
    receiver = messaging().EventConsumer(database.session_factory, consumer, clock=lambda: NOW)
    with pytest.raises(RuntimeError):
        receiver.process_one()
    consumer.commit_error = False
    assert receiver.process_one()
    assert consumer.polls == 1
    with database.session_factory() as session:
        assert len(outboxes(session)) == 4


def test_poison_message_offset_is_committed_after_quarantine(database):
    consumer = ConsumerDouble([BrokerMessage(b"bad")])
    assert messaging().EventConsumer(database.session_factory, consumer, clock=lambda: NOW).process_one()
    assert consumer.committed == [0]
    with database.session_factory() as session:
        assert outboxes(session)[0].topic == "scheduler.dlq"


class ProducerDouble:
    def __init__(self, error=None, callback=True):
        self.error, self.callback = error, callback
        self.sent = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        self.on_delivery = on_delivery

    def flush(self, timeout):
        if self.callback:
            self.on_delivery(self.error, None)
        return 0


@pytest.mark.parametrize("error, callback", [("broker error", True), (None, False)])
def test_kafka_publish_requires_successful_delivery_callback(error, callback):
    module = importlib.import_module("app.workflows.kafka")
    config = importlib.import_module("app.config").KafkaSettings()
    transport = module.KafkaTransport(config, producer=ProducerDouble(error, callback))
    with pytest.raises(module.DeliveryError):
        transport.publish("scheduler.events", "run", b"{}")


def test_kafka_client_configuration_disables_automatic_offset_commit_and_hides_secrets():
    module = importlib.import_module("app.workflows.kafka")
    settings = importlib.import_module("app.config").KafkaSettings(
        security_protocol="SASL_SSL", sasl_mechanism="PLAIN", sasl_username="private-user",
        sasl_password="private-password",
    )
    config = module.consumer_config(settings)
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert module.producer_config(settings)["enable.idempotence"] is True
    assert module.producer_config(settings)["acks"] == "all"
    assert "private-password" not in repr(settings)
    assert "private-user" not in repr(settings)


def test_worker_help_has_no_database_or_broker_side_effects():
    module = importlib.import_module("app.workflows.worker")
    with pytest.raises(SystemExit) as result:
        module.main(["--help"])
    assert result.value.code == 0


def test_worker_rejects_sqlite_without_initialization(tmp_path, monkeypatch, capsys):
    from app.config import Settings

    path = tmp_path / "never-created.db"
    monkeypatch.setattr("app.config.get_settings", lambda: Settings(database_url=f"sqlite:///{path}"))
    assert importlib.import_module("app.workflows.worker").main(["engine", "--once"]) == 1
    assert not path.exists()
    assert "MySQL" in capsys.readouterr().err


@pytest.mark.parametrize("failure", ["database", "kafka"])
def test_event_worker_rebuilds_consumer_after_db_failure_for_rebalance_recovery(database, monkeypatch, failure):
    from app.config import Settings

    _, result = seed(database)
    consumers = []
    calls = [0]
    sleeps = [0]

    def session_factory():
        calls[0] += 1
        if calls[0] == 1 and failure == "database":
            raise SQLAlchemyError("temporary database failure")
        return database.session_factory()

    wrapper = SimpleNamespace(engine=SimpleNamespace(dialect=SimpleNamespace(name="mysql")),
                              session_factory=session_factory, dispose=lambda: None)

    def make_consumer(_settings):
        consumer = ConsumerDouble([BrokerMessage(result.model_dump_json().encode())])
        if not consumers and failure == "kafka":
            from confluent_kafka import KafkaError, KafkaException

            def failed_commit(**_kwargs):
                raise KafkaException(KafkaError(KafkaError._TRANSPORT))

            consumer.commit = failed_commit
        consumers.append(consumer)
        return consumer

    def sleep(_seconds):
        sleeps[0] += 1
        if sleeps[0] == 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("app.config.get_settings", lambda: Settings(database_url="mysql+pymysql://unused"))
    monkeypatch.setattr("app.database.Database", lambda _: wrapper)
    monkeypatch.setattr("app.workflows.kafka.create_event_consumer", make_consumer)
    monkeypatch.setattr("app.workflows.worker.time.sleep", sleep)
    monkeypatch.setattr("app.workflows.worker.require_message_id_schema", lambda _: None)
    assert importlib.import_module("app.workflows.worker").main(["events"]) == 0
    assert len(consumers) == 2
    assert all(consumer.closed for consumer in consumers)
    assert consumers[0].committed == []
    assert consumers[1].committed == [0]
    with database.session_factory() as session:
        assert len(outboxes(session)) == 4


def test_mysql_message_identity_preserves_case_unicode_and_trailing_spaces():
    column_type = MessageInbox.__table__.c.message_id.type
    assert "VARBINARY(512)" in str(CreateTable(MessageInbox.__table__).compile(dialect=mysql.dialect()))
    encode = column_type.bind_processor(mysql.dialect())
    decode = column_type.result_processor(mysql.dialect(), None)
    for value in ("Event-1", "event-1", "event-1 ", "\u00e9vent-1"):
        assert encode(value) == value.encode("utf-8")
        assert decode(encode(value)) == value


def test_case_distinct_event_ids_are_independent_inbox_records(database):
    run_id, result = seed(database)
    with database.session_factory.begin() as session:
        started = event_for(session, run_id, "magnetron_sputtering", kind="node.started", message_id="Event-1")
        assert messaging().MessagingService(session).ingest(started.model_dump_json().encode(), "events", NOW) == EventDisposition.APPLIED
    with database.session_factory.begin() as session:
        completed = result.model_copy(update={"message_id": "event-1"})
        assert messaging().MessagingService(session).ingest(completed.model_dump_json().encode(), "events", NOW) == EventDisposition.APPLIED
    with database.session_factory() as session:
        assert node_run(session, run_id, "magnetron_sputtering").status == "completed"
        assert len(list(session.scalars(select(MessageInbox)))) == 2


def test_existing_collation_dependent_inbox_schema_is_rejected(monkeypatch):
    from app.workflows import models

    inspector = SimpleNamespace(has_table=lambda _: True,
                               get_columns=lambda _: [{"name": "message_id", "type": mysql.VARCHAR(128)}])
    monkeypatch.setattr(models, "inspect", lambda _: inspector, raising=False)
    with pytest.raises(ValueError, match="VARBINARY"):
        models.require_message_id_schema(SimpleNamespace())
    inspector.get_columns = lambda _: [{"name": "message_id", "type": mysql.VARBINARY(512)}]
    models.require_message_id_schema(SimpleNamespace())

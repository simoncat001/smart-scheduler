from __future__ import annotations

from app.config import KafkaSettings


class DeliveryError(RuntimeError):
    pass


def _connection_config(settings: KafkaSettings) -> dict:
    config = {"bootstrap.servers": settings.bootstrap_servers,
              "security.protocol": settings.security_protocol, "log_level": 0}
    if settings.security_protocol.startswith("SASL"):
        config.update({"sasl.mechanism": settings.sasl_mechanism,
                       "sasl.username": settings.sasl_username, "sasl.password": settings.sasl_password})
    if settings.ssl_ca_location:
        config["ssl.ca.location"] = settings.ssl_ca_location
    return config


def producer_config(settings: KafkaSettings) -> dict:
    return {**_connection_config(settings), "enable.idempotence": True, "acks": "all",
            "delivery.timeout.ms": settings.delivery_timeout_ms, "message.max.bytes": 1_100_000}


def consumer_config(settings: KafkaSettings) -> dict:
    return {**_connection_config(settings), "group.id": settings.group_id,
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest", "allow.auto.create.topics": False,
            "max.poll.interval.ms": 300000}


def create_event_consumer(settings: KafkaSettings):
    from confluent_kafka import Consumer

    consumer = Consumer(consumer_config(settings))
    consumer.subscribe([f"{settings.topic_prefix}.events"])
    return consumer


def create_command_consumer(settings: KafkaSettings, role: str, executor_id: str):
    import hashlib
    from confluent_kafka import Consumer

    if role not in {"model", "parser", "simulator"}:
        raise ValueError("invalid executor consumer role")
    config = consumer_config(settings)
    config["group.id"] = f"{settings.group_id[:64]}.{role}.{hashlib.sha256(executor_id.encode()).hexdigest()[:24]}"
    topic = f"{settings.topic_prefix}.commands.{executor_id}" if role == "simulator" else f"{settings.topic_prefix}.{'parse' if role == 'parser' else 'model'}.commands"
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer


class KafkaTransport:
    def __init__(self, settings: KafkaSettings, producer=None):
        if producer is None:
            from confluent_kafka import Producer

            producer = Producer(producer_config(settings))
        self.producer = producer
        self.timeout_seconds = settings.delivery_timeout_ms / 1000 + 1

    def publish(self, topic: str, key: str, value: bytes) -> None:
        if len(value) > 1_000_000:
            raise DeliveryError("outgoing message exceeds envelope limit")
        delivered = []

        def callback(error, _message):
            delivered.append(error is None)

        try:
            self.producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=callback)
            remaining = self.producer.flush(self.timeout_seconds)
        except Exception:
            raise DeliveryError("Kafka publication failed; no delivery confirmation") from None
        if remaining or delivered != [True]:
            raise DeliveryError("Kafka publication failed; no delivery confirmation")

    def close(self):
        self.producer.flush(self.timeout_seconds)

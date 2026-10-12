from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_DIR / "backend" / ".env"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


_load_env_file(ENV_FILE)
DEFAULT_DATABASE_URL = f"sqlite:///{(PROJECT_DIR / 'data.db').resolve()}"


def _default_path(*parts: str) -> str:
    return str((PROJECT_DIR / Path(*parts)).resolve())


def _as_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class KafkaSettings:
    bootstrap_servers: str = "127.0.0.1:9092"
    topic_prefix: str = "scheduler"
    group_id: str = "scheduler-engine"
    delivery_timeout_ms: int = 30000
    outbox_lease_seconds: int = 60
    poll_interval_seconds: float = 1.0
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = field(default=None, repr=False)
    sasl_password: str | None = field(default=None, repr=False)
    ssl_ca_location: str | None = None

    def __post_init__(self):
        from app.workflows.engine import validate_topic_prefix

        validate_topic_prefix(self.topic_prefix)
        if not self.bootstrap_servers or len(self.bootstrap_servers) > 1024:
            raise ValueError("invalid Kafka bootstrap servers")
        if not self.group_id or len(self.group_id) > 128:
            raise ValueError("invalid Kafka consumer group")
        if not 1000 <= self.delivery_timeout_ms <= 120000:
            raise ValueError("Kafka delivery timeout must be 1000..120000 ms")
        if not self.delivery_timeout_ms / 1000 + 5 <= self.outbox_lease_seconds <= 600:
            raise ValueError("outbox lease must exceed delivery timeout by at least five seconds")
        if not 0.1 <= self.poll_interval_seconds <= 30:
            raise ValueError("Kafka polling interval must be 0.1..30 seconds")
        if self.security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("unsupported Kafka security protocol")
        if self.security_protocol.startswith("SASL") and (
            self.sasl_mechanism not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} or
            not self.sasl_username or not self.sasl_password
        ):
            raise ValueError("Kafka SASL settings are incomplete")


def _kafka_settings() -> KafkaSettings:
    prefix = "SMART_CHEDULER_KAFKA_"
    return KafkaSettings(
        bootstrap_servers=os.getenv(prefix + "BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic_prefix=os.getenv(prefix + "TOPIC_PREFIX", "scheduler"),
        group_id=os.getenv(prefix + "GROUP_ID", "scheduler-engine"),
        delivery_timeout_ms=int(os.getenv(prefix + "DELIVERY_TIMEOUT_MS", "30000")),
        outbox_lease_seconds=int(os.getenv(prefix + "OUTBOX_LEASE_SECONDS", "60")),
        poll_interval_seconds=float(os.getenv(prefix + "POLL_INTERVAL_SECONDS", "1")),
        security_protocol=os.getenv(prefix + "SECURITY_PROTOCOL", "PLAINTEXT"),
        sasl_mechanism=os.getenv(prefix + "SASL_MECHANISM"),
        sasl_username=os.getenv(prefix + "SASL_USERNAME"),
        sasl_password=os.getenv(prefix + "SASL_PASSWORD"),
        ssl_ca_location=os.getenv(prefix + "SSL_CA_LOCATION"),
    )


@dataclass(slots=True)
class Settings:
    database_url: str = DEFAULT_DATABASE_URL
    scheduler_poll_interval_seconds: int = 2
    scheduler_enabled: bool = True
    scheduler_mode: str = "inline"
    auto_create_schema: bool = True
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    mat_model_auto_dispatch_delay_seconds: int = 3600
    mat_model_online_refresh_enabled: bool = False
    mat_model_manage_db_sync_enabled: bool = True
    mat_model_manage_db_sync_data_category: str | None = "development"
    mat_model_manage_db_sync_template_type: str | None = "source"
    mat_model_manage_db_sync_page_size: int = 100
    mat_model_manage_db_sync_max_records: int = 500
    manage_db_base_url: str | None = None
    manage_db_username: str | None = None
    manage_db_password: str | None = None
    manage_db_timeout_seconds: int = 15
    xrd_data_dir: str = _default_path("四元数据")
    mat_model_config_path: str = _default_path(
        "model_assets", "mat_model", "config", "ti_zr_nb_v_combined_bcc.json"
    )
    mat_model_output_dir: str = _default_path(
        "model_assets", "mat_model", "outputs", "ti_zr_nb_v_combined_bcc"
    )
    mat_model_round_result_dir: str = _default_path(
        "runtime", "mat_model_round_results"
    )

    def __post_init__(self):
        if self.scheduler_mode not in {"inline", "external"}:
            raise ValueError("scheduler mode must be inline or external")


def get_settings() -> Settings:
    return Settings(
        kafka=_kafka_settings(),
        scheduler_mode=os.getenv("SMART_CHEDULER_SCHEDULER_MODE", "inline"),
        database_url=os.getenv("SMART_CHEDULER_DATABASE_URL", DEFAULT_DATABASE_URL),
        scheduler_poll_interval_seconds=int(
            os.getenv("SMART_CHEDULER_POLL_INTERVAL_SECONDS", "2")
        ),
        scheduler_enabled=_as_bool(
            os.getenv("SMART_CHEDULER_SCHEDULER_ENABLED"), True
        ),
        auto_create_schema=_as_bool(
            os.getenv("SMART_CHEDULER_AUTO_CREATE_SCHEMA"), True
        ),
        mat_model_auto_dispatch_delay_seconds=int(
            os.getenv("SMART_CHEDULER_MAT_MODEL_AUTO_DISPATCH_DELAY_SECONDS", "3600")
        ),
        mat_model_online_refresh_enabled=_as_bool(
            os.getenv("SMART_CHEDULER_MAT_MODEL_ONLINE_REFRESH_ENABLED"), False
        ),
        mat_model_manage_db_sync_enabled=_as_bool(
            os.getenv("SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_ENABLED"), True
        ),
        mat_model_manage_db_sync_data_category=os.getenv(
            "SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_DATA_CATEGORY",
            "development",
        ),
        mat_model_manage_db_sync_template_type=os.getenv(
            "SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_TEMPLATE_TYPE",
            "source",
        ),
        mat_model_manage_db_sync_page_size=int(
            os.getenv("SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_PAGE_SIZE", "100")
        ),
        mat_model_manage_db_sync_max_records=int(
            os.getenv("SMART_CHEDULER_MAT_MODEL_MANAGE_DB_SYNC_MAX_RECORDS", "500")
        ),
        manage_db_base_url=os.getenv("SMART_CHEDULER_MANAGE_DB_BASE_URL"),
        manage_db_username=os.getenv("SMART_CHEDULER_MANAGE_DB_USERNAME"),
        manage_db_password=os.getenv("SMART_CHEDULER_MANAGE_DB_PASSWORD"),
        manage_db_timeout_seconds=int(
            os.getenv("SMART_CHEDULER_MANAGE_DB_TIMEOUT_SECONDS", "15")
        ),
        xrd_data_dir=os.getenv("SMART_CHEDULER_XRD_DATA_DIR", _default_path("四元数据")),
        mat_model_config_path=os.getenv(
            "SMART_CHEDULER_MAT_MODEL_CONFIG_PATH",
            _default_path(
                "model_assets", "mat_model", "config", "ti_zr_nb_v_combined_bcc.json"
            ),
        ),
        mat_model_output_dir=os.getenv(
            "SMART_CHEDULER_MAT_MODEL_OUTPUT_DIR",
            _default_path(
                "model_assets", "mat_model", "outputs", "ti_zr_nb_v_combined_bcc"
            ),
        ),
        mat_model_round_result_dir=os.getenv(
            "SMART_CHEDULER_MAT_MODEL_ROUND_RESULT_DIR",
            _default_path("runtime", "mat_model_round_results"),
        ),
    )

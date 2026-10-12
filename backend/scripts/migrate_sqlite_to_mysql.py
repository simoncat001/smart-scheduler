from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine

from app.database import Base
import app.models  # noqa: F401


TABLE_ORDER = [
    "schedules",
    "campaigns",
    "campaign_schedule_links",
    "executions",
    "feedback_events",
    "data_packages",
    "recommendation_records",
    "closed_loop_events",
]


def _create_mysql_database(admin_url: str, database_name: str) -> None:
    engine = create_engine(admin_url, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
                )
            )
    finally:
        engine.dispose()


def _copy_table(sqlite_engine: Engine, mysql_engine: Engine, table_name: str) -> int:
    source_metadata = MetaData()
    source_metadata.reflect(bind=sqlite_engine, only=[table_name])
    source_table = source_metadata.tables[table_name]
    target_table = Base.metadata.tables[table_name]

    with sqlite_engine.connect() as sqlite_connection:
        rows = [
            dict(row._mapping)
            for row in sqlite_connection.execute(source_table.select()).fetchall()
        ]

    with mysql_engine.begin() as mysql_connection:
        mysql_connection.execute(text(f"DELETE FROM `{table_name}`"))
        if rows:
            mysql_connection.execute(target_table.insert(), rows)
    return len(rows)


def migrate(sqlite_path: Path, admin_url: str, database_url: str, database_name: str) -> dict[str, int]:
    sqlite_engine = create_engine(f"sqlite:///{sqlite_path}", future=True)
    mysql_engine = create_engine(database_url, pool_pre_ping=True, future=True)
    try:
        _create_mysql_database(admin_url, database_name)
        Base.metadata.create_all(mysql_engine)
        counts: dict[str, int] = {}
        for table_name in TABLE_ORDER:
            counts[table_name] = _copy_table(sqlite_engine, mysql_engine, table_name)
        return counts
    finally:
        sqlite_engine.dispose()
        mysql_engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate smart_scheduler SQLite data into MySQL.")
    parser.add_argument(
        "--sqlite-path",
        default=str((Path(__file__).resolve().parents[2] / "data.db").resolve()),
        help="Path to the existing SQLite database.",
    )
    parser.add_argument(
        "--mysql-admin-url",
        required=True,
        help="SQLAlchemy URL pointing at MySQL server without database name.",
    )
    parser.add_argument(
        "--mysql-database-url",
        required=True,
        help="SQLAlchemy URL pointing at the target MySQL database.",
    )
    parser.add_argument(
        "--mysql-database-name",
        default="smart_scheduler",
        help="Target MySQL database name.",
    )
    args = parser.parse_args()

    counts = migrate(
        sqlite_path=Path(args.sqlite_path).resolve(),
        admin_url=args.mysql_admin_url,
        database_url=args.mysql_database_url,
        database_name=args.mysql_database_name,
    )
    for table_name in TABLE_ORDER:
        print(f"{table_name}: {counts[table_name]}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import sys

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import SQLAlchemyError

from app.database import Base, Database
from app.workflows.definitions import graph_json
from app.workflows.models import WORKFLOW_TABLES, WorkflowDefinition, require_message_id_schema
from app.workflows.templates import default_materials_graph


TEMPLATE_ID = "939702fe-b688-5ea5-81a5-4994b95d58ce"


def initialize_workflows(database: Database) -> None:
    if database.engine.dialect.name != "mysql":
        raise ValueError("workflow initialization requires MySQL; SQLite is for isolated unit tests only")
    inspector = inspect(database.engine)
    if any(not inspector.has_table(name) for name in ("schedules", "executions", "data_packages")):
        raise ValueError("initialize the existing application schema before workflow tables")
    if inspector.has_table("workflow_message_inbox"):
        require_message_id_schema(database.engine)
    Base.metadata.create_all(database.engine, tables=WORKFLOW_TABLES)
    with database.session_factory.begin() as session:
        statement = insert(WorkflowDefinition).values(
            id=TEMPLATE_ID, name="Materials workflow template",
            draft_graph_json=graph_json(default_materials_graph()),
        )
        # Duplicate initialization never resets an edited template or its revision.
        session.execute(statement.on_duplicate_key_update(id=TEMPLATE_ID))


def main() -> int:
    parser = argparse.ArgumentParser(description="Create additive MySQL workflow tables and an unbound draft")
    parser.parse_args()
    from app.config import get_settings

    database = None
    try:
        database = Database(get_settings().database_url)
        initialize_workflows(database)
    except ValueError as exc:
        message = str(exc) if database is not None else "Invalid database configuration; check local MySQL settings."
        print(message, file=sys.stderr)
        return 1
    except (SQLAlchemyError, ImportError):
        # Driver errors can contain connection details; never echo a configured URL or credentials.
        print("Workflow initialization failed; check MySQL availability and schema permissions.", file=sys.stderr)
        return 1
    finally:
        if database is not None:
            database.dispose()
    print("Workflow tables ready; default draft is unbound and no workers were started.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Workflow Foundation and Lightweight Kafka Runtime

Phase 3 scheduling integration is implemented; see
[Workflow scheduling](workflow-scheduling.md). The historical phase-1/2 boundaries
below remain recorded for context. Schedule/API isolation is no longer pending;
Phase 4 [HTTP results and workflow APIs](workflow-api.md) are also implemented;
builder/domain-executor integration remains pending.

## Current Scope

The user approved removing equipment resource control on 2026-10-10. Phase 2 adds
lightweight node coordination and Kafka transport, without resource registration,
capacity pools, device reservations or fencing. Device availability and safe
execution belong to the receiving gateway. Existing resource mappings remain
unused for compatibility; no existing resource tables or data are dropped.

Phase 2 implements the backend services and explicit workers described in
[Lightweight Kafka runtime](kafka-workflows.md). It does not yet connect the
workflow engine to the application's legacy schedule dispatcher, HTTP uploads,
model/parser execution or homepage. Do not activate production workflow bindings
until that isolation/integration phase is complete.

## Implemented

This first phase provides a versioned workflow definition layer. It does not
replace the running legacy scheduler, publish Kafka commands, or add the visual
builder yet. The current homepage and its themes are unchanged.

- Seven registered node types: `instrument_task`, `data_upload`, `data_parse`,
  `model_inference`, `condition`, `join`, and `manual_approval`.
- Independent node IDs, even when multiple nodes use the same instrument type.
- Bounded, strict graph/message schemas and explicit input/output ports.
- DAG validation, configuration validation, required input/default validation,
  named condition routes and registered executor/adapter checks. Gateway routing
  does not require a resource registration record.
- Editable drafts with revisions and immutable published snapshots. Running
  executions retain their pinned version; active schedules cannot change binding.
- Ten active additive InnoDB tables for definitions, bindings, runs, node attempts,
  data links, outbox/inbox and durable executor jobs. The two previous resource
  mappings are compatibility-only; the workflow initializer no longer creates them.
- An unbound materials draft preserving the existing six node positions and
  preparation -> parallel XRD/EDS/nanoindentation -> join -> MAT structure.

Device adapters are `simulator` and `kafka_gateway`. These identify receiver
contracts; they do not implement a hardware driver. The default draft deliberately
has an unconfigured model version; it cannot be published until that version is
pinned. Configure the receiving gateway separately. Initialization never enables
devices, binds schedules or starts experiments.

## MySQL Initialization

Use the application's existing local MySQL database and existing
`SMART_CHEDULER_DATABASE_URL` setting. Never use the production database as an
integration-test database. This command is an explicit schema write: it creates
missing workflow tables and an unbound template draft, not a database reset.

Run from `smart_scheduler/backend` using the confirmed environment interpreter:

```sh
/opt/anaconda3/envs/dev/bin/python -m pip install -r requirements.txt
/opt/anaconda3/envs/dev/bin/python -m app.workflows.initialize
```

Initialize the existing application tables first. The command refuses SQLite or a
database missing `schedules`, `executions`, or `data_packages`. Repeating it keeps
existing tables/data and does not overwrite an edited default draft. It creates
tables, not migrations for incompatible existing workflow tables. MySQL DDL is
not atomically rolled back, so take a normal backup before a production migration.
An existing phase-1 inbox `message_id VARCHAR(128)` requires the explicit binary
identity migration in [Kafka runtime](kafka-workflows.md#persistence-and-initialization);
initializer/workers refuse unsafe existing storage instead of altering it silently.

The implementation work has not executed this command against your application
database. Database credentials are neither rewritten nor printed.

## Definition Service

The backend API and builder follow in later phases. The definition service is
already available to backend callers:

```python
from app.workflows.definitions import DefinitionService
from app.workflows.schemas import WorkflowGraph

with database.session_factory.begin() as session:
    service = DefinitionService(session)
    draft = service.create("Experiment workflow", WorkflowGraph.model_validate(graph_data))
    version = service.publish(draft.id, draft.revision)
    service.bind(schedule_id, version.id)
```

The caller owns commit/rollback; services only flush. Publication validates
registered types, their pinned versions and adapters under row locks. Draft
saves require the revision read by the caller, preventing stale overwrites.
Published graphs include a position snapshot; positions are excluded from the
executable digest. Draft edits never mutate previous published snapshots.

Binding persists configuration only in this phase. Do not enable workflow-bound
schedules for execution until the external engine and legacy isolation are
implemented. Existing execution continues on the unchanged legacy path.

JSON Schema validation uses Draft 2020-12. Remote/dynamic/recursive references are
unsupported; acyclic local JSON pointers work without network access. Port
compatibility is deliberately conservative: identical schemas, an unconstrained
target, or simple integer-to-number conversion. Schema version 1 also disallows
regex `pattern`/`patternProperties`, `uniqueItems: true`, and `unevaluatedItems`/
`unevaluatedProperties`, bounds reference
expansion complexity to 10,000 units and combined schema/value validation work
to 1,000,000 units. These are conservative admission limits, not a wall-clock
timeout guarantee or full JSON Schema vocabulary support. Literal metadata and
fields named `pattern` remain supported.

Selected branches can receive required inputs when activation is provable.
Reconvergent inputs whose producer may be skipped require an optional input or a
validated default. An optional output also cannot supply a required input without
a default. Complex nested activation that cannot be proven conservatively needs
explicit defaults or a simpler graph. Route activation proof uses shared route
vertices and dominators for common cases. Exhaustive reconvergence is checked with
a bounded truth table (at most eight unresolved ancestor conditions and 512 cached
graph evaluations across the validation). Larger unresolved cases require explicit
defaults rather than unbounded route enumeration.

Publication allocates versions with a current locking read after locking the
definition. Schedule rebinding locks/refetches active execution/run state and the
binding rather than relying on a cached ORM value. Future runtime transitions
must respect schedule -> execution/run -> node/attempt lock order. These SQL boundary
checks are covered by unit tests; live MySQL repeatable-read races remain untested.
See [MySQL locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html).

## Verification

```sh
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_definition.py -v
/opt/anaconda3/envs/dev/bin/python -m pytest
```

Focused unit tests use isolated temporary databases, never a production SQLite
configuration. Compiling all table definitions with the MySQL dialect checks DDL
shape; it is not proof of MySQL concurrent locking or Kafka operation. Real broker
and disposable-MySQL integration remain separate later-phase acceptance checks.

The pre-implementation legacy baseline has ten failures in recommendation,
auto-dispatch and Manage DB upload tests, recorded in the implementation plan.
They are not hidden or relabeled as passing.

Final phase-1 verification on 2026-10-10: 49 focused tests passed; the full suite
reported 77 passed and the same ten baseline failures. Independent review verified
the phase-1 fixes. Actual MySQL concurrency and Kafka connectivity were not run.

## Next Phase

Integrate the lightweight coordinator with schedule dispatch and HTTP results,
isolate workflow-owned runs from legacy completion/inference hooks, then add
workflow APIs, durable model/parser/simulator executors and the builder UI.
Resource occupancy control is removed from this roadmap. Real MySQL/Kafka
integration checks remain required before production activation.

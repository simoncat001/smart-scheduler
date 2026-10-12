# Workflow Scheduling Integration: Phase 3

Phase 4 adds [workflow CRUD, operator actions and node-aware HTTP results](workflow-api.md).
The phase-3 deferred boundaries below are historical; exactly correlated uploads
with stable request IDs are no longer categorically blocked.

Date: 2026-10-12. Backend scheduling integration only; homepage layout/themes
are unchanged. Resource occupancy control remains out of scope.

## Implemented

- Published workflow bindings now select the workflow engine rather than the
  legacy dispatcher. Binding remains explicit; no existing schedule is converted.
- Manual trigger uses the existing schedule API. External engine workers also
  tick due bound schedules. Different bound schedules do not pause each other.
- A schedule-row lock, current execution reads and an awaiting-feedback flag
  prevent another round while any execution is active or uncertain. Runs pin the
  published version, preserve campaign/round/batch/sample metadata and copy inputs
  into their own stable-ID outbox commands in one outer transaction.
  Bound batch roots include the full schedule UUID to disambiguate same-named
  workflows/legacy batches; legacy batch IDs are unchanged.
- Unbound schedules retain the legacy path. Its global pause, dispatch and timeout
  logic exclude bound schedules; explicit owned executions also reject legacy
  feedback, uploads and inference. Historical unbound execution records remain
  legacy records even if their schedule is later bound.
- Owned run finalization updates feedback events, execution status, health,
  next-run timing, campaign events and compatibility snapshots together, without
  committing inside the engine or starting a legacy model prediction.
- An uncertain timeout remains `needs_attention`, holding the next round. Failed
  workflows wait for already-active sibling attempts to settle. Finalized failure
  pauses automatic dispatch; it never silently starts another hardware attempt.
- Compatibility progress is derived from node states, not dispatch timestamps or
  legacy observation fields. Repeated instrument types retain separate node IDs;
  ambiguous instances are not flattened into one instrument-state row.
- Legacy read-side projection/history backfill/Manage DB refresh do not mutate
  owned state. Only the engine/finalizer writes owned snapshots under its locks.

Schedule pause prevents new downstream commands while recording received results.
Resume continues the existing run with the configured Kafka topic prefix. Pause
does not cancel already queued or executing equipment commands.
If the last in-flight result completes the graph while paused, the run/execution
finalize normally but the schedule stays paused and no next round is dispatched.

## Operational Preconditions

Use local MySQL and the existing `SMART_CHEDULER_DATABASE_URL`. Follow the
[Kafka runtime initialization and inbox storage requirements](kafka-workflows.md#persistence-and-initialization).
No database migration, environment-file rewrite or production activation was
performed by this increment. Workers still refuse a SQLite runtime; test clients
use isolated temporary databases only.

Published definitions are currently created through `DefinitionService`; public
draft CRUD and the visual builder are not implemented yet. Configure actual
gateway routing, output schemas and a pinned model version before publishing.
The template adapter names alone do not install simulator/hardware/model jobs.

**Do not use production equipment for preview.** With a live gateway connected,
triggering a bound workflow may execute its commands. Before deployment, require
the real Kafka/disposable-MySQL integration checks, gateway command deduplication
and the remaining result/domain-executor implementation.

## API Surface

### Bind a Published Version

`PUT /api/v1/schedules/{schedule_id}/workflow`

```json
{"workflow_version_id":"<published-canonical-UUID>"}
```

Binding a schedule with an active execution returns `409`. Missing schedules or
published versions return `404`. Invalid IDs/extra request fields return `422`.
Binding does not itself trigger work or resume a paused schedule.

### Trigger and Control

`POST /api/v1/schedules/{schedule_id}/trigger` keeps the existing response shape
and adds `workflow_runtime` for owned executions. Duplicate active-round triggers
return `409`. This explicit action is available in inline and external modes.

Existing `/pause` and `/resume` schedule endpoints also control the active owned
run; no layout/UI control changes were made.

### Read Runtime

`GET /api/v1/executions/{execution_id}/workflow` returns pinned version, aggregate
status and per-instance node/attempt/output/error fields. An unbound legacy
execution has no workflow runtime and returns `404` on this route.

`GET /api/v1/executions` retains legacy fields and adds `workflow_runtime` (null
for legacy records). Its compatibility snapshots cannot report preparation as
successful just because dispatch occurred or reuse another round's model result.

The current homepage still uses its fixed graph; custom graph monitoring and
editing are the later frontend phase. New runtime data is available to API clients
now, not a claim that the visual builder or all frontend mappings are complete.

## Scheduler Modes

Both the API process and all worker processes must use consistent database,
topic-prefix and scheduling settings. The existing default is `inline`:

| Mode | API Background Loop | Engine Worker |
| --- | --- | --- |
| `inline` | Legacy unbound schedules only | Bound workflow schedules and node deadlines |
| `external` | None | Bound workflows plus legacy unbound scheduling |

Recommended external deployment settings, supplied explicitly without changing
the existing `.env` automatically:

```sh
export SMART_CHEDULER_SCHEDULER_MODE=external
export SMART_CHEDULER_SCHEDULER_ENABLED=true
```

`SMART_CHEDULER_SCHEDULER_ENABLED=false` disables automatic creation of rounds in
both paths. Existing node messages/deadlines can still be processed and explicit
manual triggers remain possible. In external mode `POST /api/v1/engine/tick`
returns `409` rather than creating a second automatic tick owner in the API.
In inline mode that endpoint retains its legacy-only behavior.

From `/Users/liye/Code/feed_back_sys/smart_scheduler/backend`, the explicit workers:

```sh
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker engine
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker publisher
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker events
```

Start with one engine process; real concurrent MySQL scheduling races have not
been integration-tested. No background workers start merely by importing the
workflow API, and external-mode API reloads do not launch scheduling workers.

## Deferred Work

Node-aware HTTP uploads are the next integration phase. For now legacy
`/instrument-uploads`, `/closed-loop/data-packages` and manual `/feedback` reject
owned runs with `409` before parsing or changing their state. This preserves old
endpoints for unbound runs without assigning a result to the wrong/latest node.
Workflow results currently arrive through the correlated Kafka event path.

Full workflow CRUD/audited operations, HTTP result correlation/parsing,
model/parser/simulator durable executors and designated model-candidate application
are not implemented. A graph completion score reflects successful coordination,
not a physical-material-quality calculation or an automatically applied candidate.
Regular successful schedule intervals can create a later bound round, but this
phase does not transform a model output into new experiment settings.

Real MySQL concurrency/migration and Kafka/gateway round trips remain unverified.
No real hardware workflow, broker or production worker was started.

## Verification

```sh
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_scheduling.py tests/test_workflow_definition.py tests/test_workflow_runtime.py tests/test_kafka_messaging.py
/opt/anaconda3/envs/dev/bin/python -m pytest
```

Final verification: 141 focused checks passed; the full suite had 169 passes and
the same ten pre-existing failures (110.07s), listed in the implementation plan.
Independent bounded recheck confirmed both material findings resolved with seven
targeted checks. Compilation and worker help checks passed. The legacy failures
are not skipped or relabeled as passing. Unit locking/statement checks are not
proof of live MySQL races, broker delivery or physical safety.

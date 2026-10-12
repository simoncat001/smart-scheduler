# Workflow APIs and HTTP Results: Phase 4

Phase 6 adds [durable domain workers and recorded heartbeat freshness](workflow-executors.md).
It supersedes historical unknown-worker/deferred-domain statements below. Broker
liveness and real hardware verification remain unproven.

Phase 7 adds the separate Vue Flow builder and per-instance dynamic monitoring.
The runtime read now also returns `definition_id`, `version`, and `graph` from the
run's pinned `WorkflowVersion`, never the current draft. The executions projection
retains these fields inside `workflow_runtime`; legacy runs still return null.
No schema migration is needed for these additive read fields.

Date: 2026-10-12. Homepage layout and both themes are unchanged.
No resource registration/capacity API, production DDL, broker startup or device
execution was introduced. Runtime database remains local MySQL.

## Drafts and Published Versions

| Method | Path | Request / Purpose |
| --- | --- | --- |
| GET | `/api/v1/workflow-types` | Types, config schemas, executor kind, adapters |
| GET | `/api/v1/workflow-templates/materials` | Unbound draft; configure pinned model before publishing |
| POST | `/api/v1/workflows` | `name`, `graph`; returns `201` |
| GET | `/api/v1/workflows` | List drafts; `limit` 1-500, default 100 |
| GET | `/api/v1/workflows/{id}` | Draft graph and current revision |
| PUT | `/api/v1/workflows/{id}` | `graph`, expected `revision`; increments revision |
| DELETE | `/api/v1/workflows/{id}?revision=N` | Unpublished drafts only; returns `204` |
| POST | `/api/v1/workflows/{id}/validate` | Expected `revision`; validates stored graph |
| POST | `/api/v1/workflows/{id}/publish` | Expected `revision`; immutable version, returns `201` |
| GET | `/api/v1/workflows/{id}/versions` | Immutable versions, newest first |
| GET | `/api/v1/workflows/{id}/versions/{version_id}` | One version within its definition |
| GET, PUT | `/api/v1/schedules/{id}/workflow` | Read binding or bind `workflow_version_id` |
| GET | `/api/v1/executions/{id}/workflow` | Pinned version and per-instance runtime |

Drafts may hold unfinished graphs. Validation/publication checks DAG structure,
ports, branch coverage, adapter/config and pinned model identity. Stale revisions
and active-run binding changes return `409`; missing records return `404`; domain
validation returns `400`; malformed strict bodies return `422`. Published
definitions cannot be deleted. Draft edits never affect published versions/runs.

`instrument_task` and `data_upload` config may pin `component_id` alongside
`instrument_code`. If absent, compatibility lookup uses the **attempt command's
copied** instrument components, never the schedule's current payload. Prefer
explicit component config for multiple same-type devices. Structural gateway
validation does not prove device availability or install an executor.

## HTTP Upload Correlation

Existing `POST /api/v1/instrument-uploads` and
`POST /api/v1/closed-loop/data-packages` accept `node_run_id`, `attempt` and
`request_id`. Instrument uploads also accept `run_id`. Unbound legacy executions
retain the old feedback/result path.

Workflow-owned requests require a stable request ID, matching component and an
explicit execution/node/batch. Node ID and attempt must be supplied together.
All supplied run/batch/task identities must agree; an unknown explicit run never
falls back to another round. Task-only/latest lookup is refused. Without node/
attempt, exactly one compatible waiting attempt may match, otherwise `409`.

Example (replace placeholders with actual command/runtime context):

```json
{
  "run_id": "<execution UUID>",
  "experiment_batch_id": "<this execution's batch>",
  "node_run_id": "<XRD node instance UUID>",
  "attempt": 1,
  "request_id": "<stable upload ID>",
  "instrument_code": "xrd",
  "component_id": "xrd-lab-01",
  "status": "uploaded",
  "observation": {"xrd_phase": "bcc"}
}
```

`triggered` records start; `failed` records failure. Normalization reuses existing
XRD/EDS/hardness/preparation parsers. The standard template wraps observations in
its single output port. Generic multi-port `ResultService.ingest` callers supply
the declared output map; Kafka envelopes already use `outputs`.

Data packages reuse Manage DB references and parsing. Uploaded/unvalidated
packages are stored and linked without completing a node. Only validated/
accepted/ready/synced packages apply completion. Another attempt's package,
unvalidated package or mismatched package observation cannot complete a node.

Exact durable request identity makes identical retries return the saved result/
package **without re-reading files or Manage DB**. Changed body reuse returns
`409`, including changed note, metadata or validation status. Use a new ID for a
new validation/upload action; retries retain the original body/node/attempt.

HTTP and Kafka share engine ownership, output-schema and state validation.
Package/link, inbox, node transition, downstream command and final feedback share
the caller's transaction. One characterization upload cannot invoke legacy
inference or mark the round successful; all required parallel/model nodes must
finish.

### Parsing and Transactions

Public routes preflight in a dedicated request session, end its read transaction,
then parse/fetch raw data without database transactions or state locks. Persistence
rechecks current attempt ownership under engine locks. Programmatic callers use
`prepare_workflow_upload` / `prepare_workflow_data_package` in a clean dedicated
session before writes, then pass the immutable `PreparedResult` to ingestion.
It pins the execution, node/attempt/command identity and request digest selected
before parsing. A concurrent advance to a serial successor produces a conflict,
never a new fallback match. Remote Manage
DB parsing without a prepared result is refused by workflow persistence.
`commit=False` leaves commit/rollback to the caller; any exception requires a
rollback of the whole state transaction.

## Audited Operations

`POST /api/v1/workflow-nodes/{node_run_id}/operations` accepts `action`, stable
`request_id`, nonblank `actor`, `reason`, and optional `outputs`/`attempt`.

- `approve`: waiting manual approval only; schema-valid outputs; no executor attempt.
- `retry`: explicit compute-only `needs_attention` retry, within pinned attempt
  limit, active schedule, no failed-run reopening. New attempt/command rejects
  late old-attempt results. Expected `attempt` is required.
- `cancel`: expected attempt required; sends Kafka cancel and remains
  `cancel_requested` until executor completion/cancel/failure. Active siblings of
  failed runs can cancel. The publisher suppresses pending executes whose final
  sendability check sees cancellation. It cannot retract an earlier publisher's
  buffered bytes or guarantee broker ordering. Gateways **must persist a
  command-ID cancellation tombstone before acknowledging cancellation**, including
  cancel-before-execute delivery, and reject every later execute for that command.
  Cancel payload includes `require_cancellation_tombstone: true`. Already executing
  equipment still needs gateway reconciliation. Late start/progress cannot clear
  cancellation. This gateway behavior is a deployment prerequisite, not an
  implemented hardware adapter or a claim of physical cancellation safety.

Full request, actor, reason and result are stored in durable inbox handler
`operator`. Identical retries return the saved response; conflicting ID reuse is
`409`. Actor is a caller-supplied audit label, **not authentication**; local APIs
need access control before external exposure.

Hardware retry is refused after timeout. Cancel timeout becomes `needs_attention`,
not proof of inactivity, and holds active-round exclusion. No automatic execution
retry; transport retries retain command identity.

## Health and Remaining Work

`GET /api/v1/messaging/health` returns persisted inbox/outbox counts. Broker and
worker liveness explicitly remain `unknown`; no live probe or recorded heartbeat
in this phase. No credentials, bootstrap addresses or message bodies are exposed.
This is backlog visibility, not an end-to-end health guarantee.

Meet the [binary inbox identity requirement](kafka-workflows.md#persistence-and-initialization)
before MySQL activation. No environment file or production schema was changed.
Isolated SQLite tests do not prove MySQL races/migration or live Kafka delivery.
Durable model/parser/simulator jobs, designated candidate application and
heartbeats were implemented in Phase 6. The visual builder is now implemented
in Phase 7; real disposable MySQL/Kafka integration remains the next phase.
Do not activate real equipment for preview.

## Verification

Final verification: 189 focused tests passed (6.76 seconds); full suite 217 passed /
the same ten pre-existing failures (108.49 seconds), exit 1. Evidence:
`.superpowers/sdd/2026-10-10-kafka-workflow-implementation/phase4-suite-final.log`.
Independent review reproduced and rechecked serial-upload reassignment,
cancellation-only timeout, node-only context and publisher ordering requirements;
48 API tests passed in its bounded recheck, with no remaining P1/P2 findings.
No legacy assertions were weakened. Live gateway tombstones, Kafka delivery and
MySQL concurrency remain unverified activation prerequisites.

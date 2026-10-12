# Kafka Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build configurable, versioned node workflows with durable Kafka commands/results while preserving the existing materials application and its homepage layout.

**Architecture:** MySQL owns workflow/node/attempt state and transactional outbox/inbox records. Explicit workers orchestrate nodes and publish/consume Kafka messages; device/model/parser executors durably accept jobs and report results asynchronously. New workflow-bound executions are isolated from legacy scheduling and inference hooks.

**Tech Stack:** Python >=3.12, FastAPI, Pydantic 2, SQLAlchemy 2, MySQL/PyMySQL, NetworkX, jsonschema, confluent-kafka, Vue 3, Vue Flow, existing D3/ECharts, pytest and browser verification.

**Spec:** `docs/superpowers/specs/2026-10-10-kafka-workflow-design.md`, approved by the user with "implement according to the document". Native inline implementation authorized on 2026-10-10 with "start implementing step by step".

**Plan Review:** Self-review completed against the approved spec: persistence,
runtime safety, legacy isolation, HTTP/Kafka convergence, executor durability,
builder geometry, deployment, and real integration each have an owning task.
The baseline failures below must be diagnosed rather than hidden by changing
assertions or weakening the approved acceptance criteria.

## Global Constraints

- User-approved scope amendment: no resource registration, capacity reservation,
  fencing or resource management APIs. The spec amendment overrides older task
  wording below. Keep existing resource mappings only for non-destructive compatibility.
- Completed increments cover lightweight Task 2, scheduling/API/HTTP integration
  (Tasks 3/4), Kafka transport/heartbeats (Task 5) and durable domain workers (Task 6).
  Visual builder/monitor (Task 7) is implemented; real activation/integration
  remains pending (Task 8).
  Do not adopt legacy runs. Preserve the current execution ledger between phases.

## Current Increment: Lightweight Coordination and Kafka

- [x] Remove device resource checks/queries; retain adapter and gateway validation.
- [x] Add RED tests for dependency advancement, parallel completion, branch skipping,
  manual approval, pause, stale/mismatched events, invalid outputs and no timeout retry.
- [x] Implement flush-only run coordinator, immutable input snapshots and runtime reads.
- [x] Add RED tests for durable outbox claims/delivery, inbox deduplication/conflicts,
  quarantine and DB-before-offset commit, including same-message retry on failure.
- [x] Implement Kafka transport, leased publisher and sequential event consumer.
- [x] Add opt-in MySQL-only worker roles; no API-lifespan worker or automatic DB reset.
- [x] Run focused/full tests and final independent review; record real-broker limits.

- "Keep FastAPI, SQLAlchemy, local MySQL, existing data references, and the current homepage geometry and light/dark themes. Keep existing HTTP upload endpoints."
- "Do not install RabbitMQ, silently use SQLite for the new runtime, change device settings, or start real experiments during development and tests."
- "Published versions are immutable." A run pins its definition and node-type versions.
- "Executions without a workflow binding stay on the legacy path." No in-flight legacy run adoption or historical hardware dispatch.
- "Run background roles in explicit worker processes, outside FastAPI lifespan."
- "Transport retries reuse the same command ID." Hardware execution retries require reconciliation or an explicit safe operator action.
- "Kafka transactions do not atomically include MySQL or physical device effects."
- "The inspected workspace currently has no Git repository." Do not initialize Git or invent commits; maintain the task checkboxes and execution ledger instead.
- Actual Kafka/MySQL concurrency/device connectivity are separate integration prerequisites. A fake transport does not satisfy the real Kafka smoke test.

## Environment and Verification Commands

The workspace's `backend/.venv/bin/python` exists as a broken interpreter link.
Use the confirmed executable `/opt/anaconda3/envs/dev/bin/python` for current
Python checks; do not delete or reconstruct the user's environment without need.

Run Python commands from `backend/`:

```sh
/opt/anaconda3/envs/dev/bin/python -m pytest
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_definition.py -v
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_runtime.py -v
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_api.py -v
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_kafka_messaging.py -v
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_executor_jobs.py -v
```

Install declared dependencies into the selected environment only at execution
time. Update both `backend/pyproject.toml` and `backend/requirements.txt` together.
Use the existing Python package boundaries rather than moving unrelated modules.

Run frontend commands from `ui/`: `npm test`, then `npm run build`. Add the test
script in Task 7; use Node's test runner for pure graph/mapping helpers and a
browser-based check for Vue Flow interactions. Use a free port if an existing dev
server is already running; do not terminate unrelated processes.

## Review Focus

1. Two nodes of one instrument/model type must retain distinct input/output and result ownership: Tasks 1, 2, 4 and 6.
2. A callback without a node ID must not assign data to the latest round or the first matching instrument: Task 4.
3. Late started/progress events after completion, and reused message IDs with different payloads, must not overwrite valid results: Tasks 2 and 5.
4. An offline device, timeout or recovered message claim must not trigger a new physical execution attempt: Tasks 2 and 6.
5. Edited workflow drafts, parallel completion callbacks, and a newly created round must not change inputs of a running model job: Tasks 1, 2 and 6.

## File Responsibilities

New backend package: `backend/app/workflows/`.

- `schemas.py`: strict graph, port, policy, command/event and operation request models.
- `registry.py`: registered node types/adapters and configuration/output validation.
- `templates.py`: current materials graph with original UI positions and explicit output ports.
- `models.py`: additive workflow/message/executor-job tables and unused legacy resource mappings.
- `definitions.py`: draft CRUD, version publication, schedule binding and graph validation.
- `engine.py`: locked node state transitions, dependency advancement and run completion.
- `results.py`: common HTTP/Kafka result correlation, validation and deduplication.
- `projection.py`: runtime reads and explicitly designated legacy summaries.
- `messaging.py`: outbox/inbox persistence, claims, quarantining and commit boundaries.
- `kafka.py`: confluent-kafka producer/consumer integration, no workflow policy.
- `executors.py`: durable job acceptance/claim/result publication and registered execution handlers.
- `adapters.py`: registered wrappers around existing parser/model services; simulator isolated from hardware.
- `api.py`: workflow routers built with an injected session dependency.
- `worker.py`: explicit engine/publisher/events/model/parser/simulator CLI roles.
- `initialize.py`: explicit additive schema/template initialization, never a reset.

New frontend units: `ui/src/workflow-api.js`, `ui/src/workflow-graph.js`,
`ui/src/workflow-monitor.js`, `ui/src/components/workflow-builder.js`, and
`ui/src/components/dynamic-flow-panel.js`. They use the existing JavaScript and
Vue Options/component registration approach; no framework migration.

---

### Task 1: Versioned Workflow Definitions and Additive Persistence

**Files:** Create `backend/app/workflows/{__init__,schemas,registry,templates,models,definitions,initialize}.py`; modify `backend/app/database.py`, `backend/pyproject.toml`, `backend/requirements.txt`; create `backend/tests/test_workflow_definition.py` and `backend/tests/workflow_fixtures.py`.

**Interfaces:**
- `WorkflowGraph`, `NodeDefinition`, `EdgeDefinition`, `MessageEnvelope` in `schemas.py`; schema version is `1`, IDs are nonempty and length-bounded, unknown request properties are forbidden.
- `EventDisposition` in `schemas.py` is an enum with `applied`, `duplicate`, `ignored_stale` and `quarantined`. `ExecutionContext` is a frozen dataclass in `adapters.py` with execution/version/node/attempt IDs, pinned model identity, a copied input payload and approved data references; Task 6 creates it before adapter execution.
- `validate_graph(graph: WorkflowGraph, resources: dict | None = None) -> None` uses NetworkX and registered JSON Schema/config validators. The old resources argument is ignored for compatibility.
- `default_materials_graph() -> WorkflowGraph` uses distinct IDs for preparation, XRD, EDS, nanoindentation, all-three join and designated MAT decision.
- `DefinitionService(session).create(name: str, graph: WorkflowGraph) -> WorkflowDefinition`, `.save(definition_id: str, revision: int, graph: WorkflowGraph) -> WorkflowDefinition`, `.publish(definition_id: str, revision: int) -> WorkflowVersion`, `.bind(schedule_id: str, version_id: str) -> WorkflowScheduleBinding`.
- New tables implement the spec's identities plus `ExecutorJob` for durable accepted jobs; all message and attempt uniqueness constraints are database-backed.
- `initialize_workflows(database: Database) -> None` requires a MySQL URL for production and creates only new tables and an unbound default template draft. It does not enable real device resources or publish/bind/trigger a workflow automatically; unit tests may reuse legacy temporary DB fixtures, but cannot claim MySQL locking correctness.

- [x] Write failing tests for cycles, duplicate IDs, missing ports, unknown type versions, malformed config, duplicate XRD instances and mutually exclusive branch inputs without optional/default handling.
- [x] Write failing draft/version tests asserting stale revision saves fail, published graphs never mutate, existing schedules cannot bind while awaiting feedback, and a run's pinned version survives later publication.
- [x] Run `pytest tests/test_workflow_definition.py -v` and record the missing-feature failures.
- [x] Add validated schemas/registry and SQLAlchemy mappings; use UUID IDs, explicit foreign keys and unique constraints. Import mappings before schema initialization without circular imports.
- [x] Implement publication and binding under row locks with consistent lock order. Persist positions independently from executable fields/digest.
- [x] Add the additive initialization entry point and dependency declarations for NetworkX/jsonschema. Reject production initialization against SQLite and never echo database credentials.
- [x] Run the focused tests and the legacy suite; record results and schema changes in the execution ledger. All task-specific checks pass; the unchanged ten baseline failures remain tracked for later full-suite acceptance.

### Task 2: Lightweight Transactional Node Coordination

**Files:** Create `backend/app/workflows/{engine,projection}.py`; create `backend/tests/test_workflow_runtime.py`.

**Consumes:** Task 1 graph/version tables and `MessageEnvelope`.
**Produces:** `WorkflowEngine(session, topic_prefix).start(execution, version, now) -> WorkflowRun`, `.advance(run_id, now)`, `.apply_event(event, now) -> EventDisposition`, `.expire(now) -> int`, `.pause(run_id, now)`, `.resume(run_id, now)`, `.approve(node_run_id, outputs, now)`; `workflow_runtime_read(session, execution_id) -> dict`. Caller creates a new execution with `dispatch_payload.workflow_version_id`; legacy runs are not adopted. Public audited operations and legacy finalization belong to Tasks 3-4.

- [x] Establish RED for real preparation completion, parallel branches, validated join and one pinned MAT command.
- [x] Establish RED for same-type instance isolation, condition route skipping, manual approval, pause and monotonic terminal states.
- [x] Establish RED for timeout uncertainty, no execution retry and refusal to adopt legacy executions.
- [x] Implement locked run transitions, validated ports, immutable snapshots and unique attempt/outbox creation, without networking, computation or resource control in the transaction.
- [x] Implement internal nodes, delivery/acceptance/execution deadlines and synchronous bounded result validation. No asynchronous validation job in this increment.
- [x] Preserve sibling results on failure, block downstream work, and expose current-instance runtime reads.
- [x] Run focused/full tests. Real MySQL races remain unverified; ten legacy baseline failures remain unresolved.

### Task 3: Schedule Binding and Legacy Orchestration Isolation

**Files:** Modify `backend/app/services/{scheduler,feedback,state_projection,closed_loop,acquisition,model_inference,recommendations}.py`, `backend/app/{main,schemas,config}.py`; add `backend/app/workflows/{ownership,scheduling,api}.py` and `backend/tests/test_workflow_scheduling.py`.

**Consumes:** Definition binding and engine start/read operations.
**Produces:** `WorkflowEngine(session).tick_schedules(now: datetime, limit: int = 100) -> list[WorkflowRun]`; `FeedbackService(session).finalize_workflow_execution(execution_id: str, feedback: FeedbackCreate, now: datetime) -> Execution`, flush-only and without legacy auto-inference; additive `workflow_runtime` on execution read responses.

- [x] Write failing tests that an unbound schedule retains all existing behavior, bound schedules do not pause each other, legacy global pause does not pause bound schedules, and one schedule cannot create two active rounds.
- [x] Write failing tests that API reload does not create external worker instances, legacy timeout/reconciliation/read-side backfill cannot close a bound run, and manual legacy feedback cannot bypass the workflow engine.
- [x] Run `pytest tests/test_workflow_scheduling.py -v` and existing scheduler tests to establish the boundary.
- [x] Separate bound/unbound due-schedule and timeout queries; create an execution and pinned run under a schedule row lock with one outer transaction. Preserve campaign/sample/batch generation.
- [x] Add explicit external-worker mode. In that mode FastAPI does not launch `BackgroundSchedulerLoop`; external engine owns ticking. Legacy inline mode excludes bound schedules from automatic dispatch.
- [x] Guard legacy feedback/inference/reconciliation/backfill hooks for bound executions. Factor shared health/campaign finalization so the engine can close a run once without retriggering model work.
- [x] Add runtime reads and designated compatibility projection fields without changing historical rows or suppressing active legacy UI data.
- [x] Run focused tests plus `pytest`; record required worker-mode configuration and confirm current APIs remain backward-compatible for unbound executions.

### Task 4: Workflow APIs and Common HTTP Result Processing

**Files:** Create `backend/app/workflows/{api,results}.py`; modify `backend/app/services/{acquisition,closed_loop}.py`, `backend/app/schemas.py`, `backend/app/main.py`; create `backend/tests/test_workflow_api.py`.

**Consumes:** Tasks 1-3 definitions/runtime operations.
**Produces:** `build_workflow_router(session_dependency: Callable) -> APIRouter`; `ResultService(session).resolve(execution_id: str, instrument_code: str, component_id: str, node_run_id: str | None, attempt: int | None) -> NodeAttempt`; `.ingest(attempt: NodeAttempt, request_id: str, observation: dict, data_package_id: str | None, now: datetime) -> EventDisposition`.

**Endpoints:** `GET /api/v1/workflow-types`, `GET /api/v1/workflow-templates/materials`, CRUD draft endpoints under `/api/v1/workflows`, `POST /api/v1/workflows/{id}/publish`, `GET /api/v1/workflows/{id}/versions`, `PUT /api/v1/schedules/{id}/workflow`, `GET /api/v1/executions/{id}/workflow`, `POST /api/v1/workflow-nodes/{id}/operations`, and `GET /api/v1/messaging/health`. No resource-management endpoints.

- [x] Write failing API tests for draft save/publish/bind, stale revisions, invalid gateway/adapter configuration, runtime reads and audited approve/retry/cancel requests. Device availability and occupancy are delegated to the gateway.
- [x] Write failing correlation tests: ambiguous two-XRD uploads return 409, an explicit node/attempt receives only its own data, previous-round uploads cannot match a new round, and identical HTTP retries return the same result while conflicting ID reuse fails.
- [x] Write a failing test proving workflow uploads reuse parsing/data-package storage but do not invoke legacy success feedback or inference while required nodes remain incomplete.
- [x] Run `pytest tests/test_workflow_api.py -v` to establish RED.
- [x] Add optional `node_run_id`, `attempt` and stable `request_id` to existing upload schemas. Require complete correlation for bound runs; use batch/component lookup only for an unambiguous current waiting attempt.
- [x] Make result parsing finish outside the state transaction. Persist data-package links and validated results through the shared handler; caller owns commit. Preserve legacy endpoints and error mapping.
- [x] Register the router using injected session dependency rather than importing `app.main` into workflow modules. Return 409/400/404 consistently; expose no credential values in health output.
- [x] Run focused and full legacy tests, including existing upload APIs and Manage DB parser tests. Final: 189 focused passes; full 217 passes / same ten baseline failures; independent bounded recheck clean.

### Task 5: Kafka Transport and Transactional Outbox/Inbox

**Files:** Create `backend/app/workflows/{messaging,kafka}.py`; modify `backend/app/config.py`, dependency declarations; create `backend/tests/test_kafka_messaging.py`.

**Consumes:** Message tables, engine event application and registered executor routing.
**Produces:** `KafkaTransport(settings: Settings).publish(topic: str, key: str, event: MessageEnvelope) -> None` returns only after successful delivery callback; `OutboxPublisher(session_factory: Callable, transport: KafkaTransport).publish_batch(now: datetime, limit: int = 100) -> int`; `EventConsumer(session_factory: Callable, consumer: Consumer).process_one() -> bool`; `MessagingService(session).ingest(raw: bytes, source: str, now: datetime) -> EventDisposition`.

- [ ] Write failing tests for crash-before-publish, callback failure, confirmed-send-before-mark-sent, competing publisher claims, malformed envelopes, conflicting ID reuse, terminal-state regression and stale attempts.
- [ ] Write failing consumer tests that commit occurs only after MySQL commit, a database failure prevents offset commit, poison messages persist quarantine/DLQ outbox atomically, and an unfinished earlier message prevents a later partition offset from committing.
- [ ] Run `pytest tests/test_kafka_messaging.py -v` to establish RED. Use a transport test double only at the external broker boundary; assert database state and transition behavior.
- [ ] Implement leased outbox claims, bounded backoff and retry deadlines with stable message IDs. Configure `acks=all` and producer idempotence; do not mark sent on client-buffer enqueue.
- [ ] Implement per-partition sequential event handling with auto-commit/auto-store disabled, inbox uniqueness/digest checks and transactional invalid-message quarantine. Commit offsets only for durably handled messages.
- [ ] Add bounded configuration for bootstrap servers, topic prefix, group ID, timeouts, TLS/SASL and outbox polling; keep the existing `SMART_CHEDULER_*` namespace spelling. Do not log authentication values or modify the user's `.env`.
- [x] Add messaging health showing persisted pending/sent/quarantined counts, with unknown broker/worker liveness rather than simulated success (Task 4).
- [x] Record worker heartbeats and expose per-instance freshness/offline status; broker status remains unknown and no real activation was performed (Task 6).
- [ ] Run focused/full tests; the actual broker round trip remains a Task 8 integration requirement.

### Task 6: Durable Executors, Explicit Worker CLI and Pinned Domain Adapters

**Files:** Create `backend/app/workflows/{executors,adapters,worker}.py`; modify `backend/app/services/recommendations.py`, `backend/app/services/model_inference.py`, `backend/app/services/mat_model_service.py`, `backend/pyproject.toml`; create `backend/tests/test_executor_jobs.py`.

**Consumes:** Engine commands, Kafka transport, outbox service and `ExecutorJob` persistence.
**Produces:** `ExecutorService(session).accept(command: MessageEnvelope, executor_id: str, now: datetime) -> ExecutorJob`; `.claim(executor_id: str, now: datetime) -> ExecutorJob | None`; `.finish(job_id: str, outputs: dict, now: datetime) -> None`; `AdapterRegistry.execute(command: MessageEnvelope, context: ExecutionContext) -> dict`; `main(argv: list[str] | None = None) -> int` for `smart-scheduler-worker`.

- [x] Write failing tests for accepted-job durability before offset commit, duplicate commands, conflicting duplicate payloads, lost result publication, stale computation leases, expired hardware commands and no hardware auto-resume after a process crash.
- [x] Write failing model tests showing two inference nodes preserve independent outputs, a job uses its source execution even after another round appears, and only the designated decision node schedules one next round.
- [x] Write failing ownership tests that a gateway cannot accept another gateway's commands and simulator execution cannot target a real device binding.
- [x] Write failing cancellation-before-execute/restart tests. Persist a command-ID cancellation tombstone before cancel acknowledgement/offset commit, and reject every late execute for that command even if it arrives after a publisher race.
- [x] Run `pytest tests/test_executor_jobs.py -v` to establish RED.
- [x] Persist accepted jobs and acknowledgement outbox rows before command offset commit. Keep the poll/accept loop separate from long-running execution; claim computational work with leases and require device reconciliation for uncertain hardware work.
- [x] Enforce `require_cancellation_tombstone` cancel semantics in durable model/parser/simulator jobs; real gateways must implement the same contract before activation.
- [x] Add remaining roles `model`, `parser`, and `simulator`; `engine`, `publisher` and `events` are present in the current increment. Require configured executor IDs and explicit simulator enablement; no resource registration.
- [x] Adapt existing model/parser behavior to explicit `ExecutionContext` containing pinned version, execution/attempt ID, model identity, immutable inputs and approved data references. Keep legacy schedule-latest APIs only for legacy callers.
- [x] Store node-authoritative results separately from the single-row legacy inference projection. Persist completed job events through outbox; schedule candidates through the engine's designated decision-node guard, not from the worker itself.
- [x] Run focused/full tests and a command/event simulator test using durable jobs. Final: 229 focused passes; full 257 passes / same ten baseline failures; independent bounded fix recheck clean; no real integration claim.

### Task 7: Workflow Builder and Dynamic Monitor Without Homepage Resizing

**Files:** Create the five frontend units from File Responsibilities, `ui/tests/workflow-graph.test.js` and `ui/tests/workflow-monitor.test.js`; modify `ui/src/{main,app-options}.js`, `ui/src/template.html`, `ui/src/styles.css`, `ui/package.json`, `ui/package-lock.json`.

**Consumes:** Workflow/type/template/version/binding/runtime APIs.
**Produces:** `workflowApi` CRUD/validation/publish/bind helpers; `normalizeWorkflowGraph(graph)`, `validateEditorGraph(graph)`, `runtimeToMonitor(runtime)` in focused helpers; registered `WorkflowBuilder` and `DynamicFlowPanel` components.

- [x] Add Node test runner scripts and failing pure-helper tests for duplicate instance IDs, dangling port connections, deletion of linked nodes, version/draft distinction, same-type runtime mapping and Chinese labels for waiting uploads/model start.
- [x] Run `npm test` to establish RED before implementing helpers/components.
- [x] Implement a separate `?view=workflow-builder` view using Vue Flow. Support add/delete/duplicate, drag/connect, typed port selection, config/device/policy editing, validation errors, revision-aware draft saving, publication, version selection and schedule binding.
- [x] Use explicit save operations and stable draft IDs; never mutate the pinned published version when editing. Surface API 409 revision conflicts and preserve unsaved work instead of silently reloading it.
- [x] Register components with the existing plain-JavaScript Vue application. Reuse theme state, apply UI-scoped styles, and use familiar icon controls with tooltips; do not introduce a new landing page.
- [x] Use new runtime nodes/edges for workflow-bound executions while retaining the original D3 graph for legacy runs. Default graph positions and homepage container geometry remain unchanged; custom graphs fit/pan/zoom within the existing viewport and status nodes scroll within the sidebar.
- [x] Run `npm test` and `npm run build`; browser-test node connections, persistence/reload, validation, publishing/binding, both themes and runtime status changes at desktop/mobile sizes.
- [x] Compare pre/post homepage element bounding boxes and screenshots, verify charts remain visible and text does not overlap, and confirm no orphaned refresh timers in builder mode.

Task 7 verification: 22 Node tests; 230 focused backend tests; full backend
258 passed / same ten known baseline failures (see execution ledger/log).
Playwright browser checks pass with entirely intercepted APIs. Desktop boxes
match the pre-edit baseline exactly. A scoped responsive correction for bound
workflows fixes the existing implicit-column canvas hiding/overlap below 1100px;
legacy mobile layout is untouched. Real Kafka/MySQL/hardware evidence is Task 8.

### Task 8: Local Setup, Real Integration and Whole-Change Review

**Files:** Create `compose.kafka.yml`, `backend/.env.kafka.example`, `backend/tests/test_workflow_mysql_integration.py`, `backend/tests/test_kafka_integration.py`, `backend/scripts/kafka_workflow_smoke.py`, and `docs/kafka-workflows.md`; modify `README.md`, `backend/README.md`, and execution ledger.

**Consumes:** All implemented public APIs/worker entry points.
**Produces:** An explicitly started, pinned official Kafka development service and repeatable non-hardware smoke test; documented existing-cluster and local-cluster operation.

- [ ] Write opt-in integration tests selected by dedicated MySQL/Kafka test configuration. Require a disposable MySQL database name and reject the application's production database; never truncate existing data. Missing prerequisites produce explicit skips.
- [ ] Pin a verified official Kafka image/version in Compose and configure reachable advertised listeners for host worker clients. Do not use `latest`, assume Docker availability, expose a development broker publicly, or start a container without reporting the action.
- [ ] Add an environment example with blank credential placeholders, explicit external-worker mode, bootstrap servers and role/executor settings; do not rewrite existing `.env` files.
- [ ] Implement a smoke script that creates an isolated simulator workflow/run, exercises command acceptance and correlated completion through real Kafka, checks final MySQL node states, and deletes only its isolated artifacts.
- [ ] Run real MySQL race tests for simultaneous final events, concurrent scheduler ticks and outbox claims; assert one model attempt/decision and stable command identity. No equipment capacity test.
- [ ] Run a real Kafka round trip, restart/duplicate delivery tests and invalid-message/DLQ test against explicitly enabled simulators. Report unrun cases explicitly if prerequisites are absent.
- [ ] Run bare `pytest`, `npm test`, `npm run build`, and desktop/mobile browser checks. Obtain a fresh-context whole-change review of safety, backward compatibility, transaction boundaries, offsets and UI geometry; fix material findings and rerun affected/full verification.
- [ ] Document additive initialization, gateway/template setup, publishing/binding, starting roles, pause/reconciliation, Kafka outage recovery and retained legacy behavior. Correct outdated SQLite-default operational guidance without touching legacy test fixtures.
- [ ] Start the frontend dev server on an available port after implementation and give the user its URL. Do not enable real-device command bindings or start hardware workflows for preview.

## Execution Ledger

| Task | Status | Evidence / Decision |
| --- | --- | --- |
| 1. Definitions and schema | Complete (foundation only) | 49 focused tests passed; full suite 77 passed / same 10 baseline failures; independent phase-1 review resolved |
| 2. Lightweight coordination | Complete (backend services) | 110 focused tests, 138 full-suite passes / same ten baseline failures; independent fix-pass recheck clean |
| 3. Scheduling isolation | Complete (backend integration) | 141 focused passes; full suite 169 passes / same ten baseline failures; independent bounded recheck clean |
| 4. APIs and HTTP results | Complete (backend API integration) | 189 focused passes; full 217 passes / same ten baseline failures; bounded review recheck clean |
| 5. Kafka messages | Backend transport/health implemented; real activation pending | Leased outbox/inbox/quarantine, recorded heartbeats; no real broker verification |
| 6. Durable executors | Complete (backend domain execution) | All six roles/jobs/tombstones/pinned adapters; 229 focused passes; full 257 passes / same ten baseline failures; bounded recheck clean |
| 7. Builder and monitor | Pending | Depends on APIs/runtime reads |
| 8. Integration and review | Pending | Real broker and disposable MySQL required for integration |

Record RED/GREEN commands/results and rulings in this table as work progresses.
Production implementation started with Task 1 after user authorization. The user
requested incremental delivery; the first verified foundation is documented in
`docs/workflow-foundation.md`. Lightweight runtime and Kafka services are now
implemented; scheduling/API/HTTP, domain execution and recorded heartbeat services
are also complete. UI and real service activation/integration remain pending. No
production initialization or hardware activation has been performed.

## Pre-Implementation Baseline

Command, run from `backend/` before production-code modifications:
`/opt/anaconda3/envs/dev/bin/python -m pytest`.
Result: **28 passed, 10 failed**, exit code 1, in 105.83 seconds.
Pytest also reported the pre-existing unset asyncio fixture loop-scope warning.

All failures are in `backend/tests/test_scheduler_feedback.py`:

1. `test_recommendation_engine_suggests_higher_temperature_when_metric_below_target`
2. `test_recommendation_api_and_patch_update_schedule`
3. `test_tizrnbv_recommendation_prefers_sputter_calibration_when_eds_drifts`
4. `test_tizrnbv_recommendation_uses_mat_model_first_batch_candidate`
5. `test_tizrnbv_feedback_enriches_workflow_progress`
6. `test_instrument_upload_service_auto_completes_round_when_all_data_ready`
7. `test_instrument_upload_api_auto_completes_round`
8. `test_edge_context_and_data_package_api`
9. `test_data_package_ingest_reads_live_manage_db_payload`
10. `test_build_run_view_refreshes_manage_db_snapshot`

Observed evidence includes empty recommendation lists, a calibration-action
expectation mismatch, immediate-next-round expectations, changed data-package
fields, and a Manage DB mock no longer intercepting the live fetch. These are
observations, not completed root-cause diagnoses. At execution time, reproduce
each category, compare the current public behavior with the test contract, and
record minimal justified fixes with their own RED/GREEN cycle. Do not silently
skip them, restore obsolete behavior, modify production credentials, or label the
suite passing while any unresolved failure remains.

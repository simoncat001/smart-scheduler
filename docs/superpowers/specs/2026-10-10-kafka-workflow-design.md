# Configurable Workflows and Kafka Messaging

## Approved Scope Amendment (2026-10-10)

The user explicitly approved lightweight workflow coordination plus Kafka and
removed resource occupancy control. This amendment supersedes resource capacity,
reservation, fencing and resource-registration requirements below. Equipment
availability and safe physical execution belong to the gateway, not this service.
Device nodes validate registered node/adapter types and gateway routing, without
querying resource tables. Legacy resource identifiers remain readable but unused.
Previously created resource tables are not dropped and their data is not modified.

Keep dependency advancement, parallel branches, validated outputs, pinned versions,
stable command IDs, outbox/inbox durability and exact attempt correlation. No
automatic execution retry on timeout, for device or compute nodes in this phase.
Timeout means needs_attention; a verified result may resolve it. Kafka delivery
acknowledgement is not executor acceptance. Gateways must reject expired commands
and durably deduplicate command_id before executing; no hardware driver is added.

The next incremental phase implements engine services, runtime reads, Kafka
publisher/event consumer and explicit worker roles. Existing API scheduling,
HTTP uploads, the builder and production activation remain separate integration
tasks. Do not enable bindings on the legacy scheduler until isolation is complete.

Date: 2026-10-10
Status: Approved by the user on 2026-10-10; implementation-plan review is the next stage.

## 1. Outcome and Constraints

Turn the fixed materials scheduler into a configurable node workflow system. Users
must be able to build serial and parallel workflows, reuse a node type more than
once, bind nodes to devices or workers, and exchange commands and results through
Kafka. The current materials experiment remains the default workflow template.

Keep FastAPI, SQLAlchemy, local MySQL, existing data references, and the current
homepage geometry and light/dark themes. Keep existing HTTP upload endpoints.
Do not install RabbitMQ, silently use SQLite for the new runtime, change device
settings, or start real experiments during development and tests.

Kafka deployment is not yet specified by the user. Provide configurable existing
cluster connectivity and an optional, explicitly started local development setup.
Use localhost:9092 only as a documented development default, not evidence of a
running broker. Real Kafka verification requires an available broker; report it
separately from fake-transport tests.

## 2. Current System and Compatibility Boundary

- `backend/app/materials.py` accepts `workflow_steps`, but progress depends on
  hard-coded instrument signals. Dispatch itself currently counts as preparation.
- `backend/app/services/scheduler.py` creates round executions, globally pauses
  other active schedules, and runs an in-process polling loop.
- `backend/app/models.py` uniquely identifies instrument state by execution and
  instrument type; it cannot represent two independent XRD node instances.
- `backend/app/services/acquisition.py` merges uploads and may submit successful
  round feedback as soon as the fixed instrument requirements are met.
- `backend/app/services/feedback.py` synchronously invokes recommendation logic
  and schedules the next candidate when the legacy closed-loop criteria pass.
- `ui/src/components/flow-monitor-panel.js` defines a fixed graph; the current
  homepage and prediction panels rely on existing read projections.

New workflow-bound executions use the new engine as their sole orchestration
authority. Executions without a workflow binding stay on the legacy path. Do not
retroactively emit hardware commands, reinterpret completed historical nodes, or
adopt an in-flight legacy execution. Migrating an existing schedule requires an
explicit binding update while it has no active execution.

Legacy upload and feedback processing must not auto-close a workflow-bound run or
launch its model inference. For those runs, reuse parsing and result storage, but
pass validated node results to the engine. Long parsing and inference happen
outside the engine's state-transition transaction.

## 3. Architecture and Ownership

```text
Workflow Editor / REST API
          |
Versioned definitions and node registry
          |
Workflow Engine <---- MySQL node state, attempts, resources, inbox
          |
MySQL transactional outbox -> Kafka Publisher -> Kafka command topics
                                                    |
                                        Device gateways / model workers
                                                    |
Workflow Engine <- Kafka Event Consumer <- Kafka event topic
          |
Read projections -> existing homepage and execution detail
```

- The API validates drafts, publishes versions, binds schedules, requests runs,
  and exposes runtime state. It does not own background consumers.
- The engine decides readiness, reserves resources, creates commands, applies
  events, advances dependencies, and coordinates round completion.
- The publisher sends committed outbox rows and records Kafka delivery outcomes.
- The event consumer validates messages and invokes the engine transactionally.
- Gateways durably accept commands, enforce device safety, execute asynchronously,
  and publish business acknowledgements and results.
- The model worker wraps existing model/recommendation functions behind the same
  command/result contract. Each inference node has one execution owner; API and
  legacy hooks cannot start that node independently.

Run background roles in explicit worker processes, outside FastAPI lifespan.
Database claims and uniqueness constraints must make multiple worker instances
safe. API restarts or reloads must not start duplicate schedulers.

## 4. Workflow Definition and Node Registry

A definition contains `schema_version`, `nodes`, `edges`, and UI positions.
Each node has a unique `id` within the version, a registered `type` and type
version, a display label, typed input/output ports, configuration, executor
binding, timeout, retry policy, and optional device resource requirements.
Edges identify source and destination nodes and their port bindings.

Node types and node instances are different identities. Two XRD nodes can share a
type and use separate IDs, devices, output ports, runtime records, and attempts.
Adding a device instance is configuration. Adding new domain execution behavior
requires an explicitly registered, tested adapter, not arbitrary uploaded code.

Initial node types:

| Type | Responsibility |
| --- | --- |
| `instrument_task` | Issue a device command and await validated results |
| `data_upload` | Await a correlated, validated data reference without starting hardware |
| `data_parse` | Run a registered parser in a worker |
| `model_inference` | Run a registered model and publish its result |
| `condition` | Select a route using an allowlisted field/operator/value rule |
| `join` | Await successful required inputs, including the materials all-three gate |
| `manual_approval` | Await an explicit, audited approval |

Use Pydantic for configuration/envelope validation and JSON Schema validation for
declared input/output payloads. Use NetworkX DAG validation/traversal rather than
an ad hoc graph parser. Reject duplicate IDs, missing endpoints, cycles, unknown
types/versions, invalid configurations, missing required port bindings, and
incompatible executor/resource bindings before publishing.

Conditions use a restricted comparison DSL, never Python/JavaScript `eval`.
They choose one named route, including an explicit default. Unselected edges are
inactive; a non-root node with all incoming edges inactive becomes `skipped`.
Joins wait for all active predecessors to succeed. A required input from a skipped
branch must have a declared default or be optional; otherwise validation rejects
the definition. The materials join explicitly requires all three results and
cannot treat a missing result as successful.

Published versions are immutable. Draft saves use a revision token to reject
lost updates. An execution pins one published version and its node type versions.
A new draft/version cannot modify a running execution. Closed-loop repetition
creates another round execution; there are no cycles inside a round's DAG.

## 5. MySQL Persistence

Keep `Schedule`, `Execution`, `CampaignRound`, existing data packages, inference
records, and legacy projections. `Execution` remains the round/correlation ID.
Use additive tables to avoid destructive changes to populated legacy tables:

| Table | Main invariant |
| --- | --- |
| `workflow_definitions` | Stable identity and editable draft with revision |
| `workflow_versions` | Unique definition/version, immutable graph and digest |
| `workflow_schedule_bindings` | Explicit published version bound to a schedule |
| `workflow_runs` | Unique execution ID, pinned version and aggregate state |
| `node_runs` | Unique workflow run/node ID, state and validated output reference |
| `node_attempts` | Unique node run/attempt, stable command ID and deadlines |
| `node_data_package_links` | Explicit node attempt/data package association |
| `device_resources` | Registered capacity and device/gateway ownership |
| `resource_reservations` | Capacity slot, owner attempt and fencing generation |
| `message_outbox` | Unique message ID, pending send, claim lease and backoff |
| `message_inbox` | Unique handler/message ID, payload digest and disposition |

Existing instrument and inference projections have one row per instrument type
or source execution. They are compatibility summaries, not authoritative runtime
storage for repeated nodes. A template may designate one node per legacy summary
slot (including the primary prediction node); all other instances retain their
independent attempts/results in the new runtime. Do not flatten two node outputs
into the same instrument key or overwrite another model node's result.

Use MySQL row locks/conditional updates and unique constraints for reservations,
attempt creation, event application, and workflow-version publication. Lock one
workflow run while advancing its parallel branches so simultaneous completion
events cannot schedule its join/model twice. Use a consistent lock order.

MySQL schema changes require an explicit, additive migration/initialization path.
`create_all` can create new tables but is not a migration mechanism for existing
columns. Do not clear application data or initialize a Git repository as part of
this work. The inspected workspace currently has no Git repository.

## 6. Execution and Device Safety

Node states: `waiting_dependencies`, `ready`, `dispatch_pending`, `dispatched`,
`accepted`, `running`, `succeeded`, `failed`, `timed_out`, `needs_attention`,
`cancel_requested`, `cancelled`, and `skipped`.

- Dependency readiness does not imply that a command was sent or that work began.
- Kafka delivery success moves publication state, not physical execution state.
- `accepted` is a durable business acknowledgement from the gateway/worker.
- `succeeded` requires the registered completion policy and validated outputs.
  Upload receipt alone is insufficient when parsing is required.
- New preparation nodes must not become successful merely because a round was
  dispatched. The default graph starts characterization after preparation succeeds.
- XRD, EDS, and nanoindentation are parallel. UI labels derive from their actual
  readiness/result state. Model inference displays waiting-to-start until its join
  succeeds, never a previous round's inference success.
- Preserve separate delivery, acceptance, execution, and result-validation
  deadlines. Record command expiry and reject stale attempts and old-round events.
- Transport retries reuse the same command ID. An explicit execution retry creates
  a new attempt and command ID, only when allowed by its safety policy.
- Pure computation may use bounded automatic retry. Hardware start commands
  default to manual/reconciliation retry. Timeout or lost heartbeat is uncertain
  device state, not proof of cancellation or permission to start again.
- Reclaiming a worker lease must not release a physical device reservation until
  the gateway confirms termination or an operator resolves it. Gateways enforce
  device ownership/fencing and must not overlap physical operations.
- Pause prevents new commands; cancellation remains requested until acknowledged.
  Continue recording results of already-running parallel nodes even if the run
  has failed, but do not start new downstream nodes.

For new workflows, allow multiple schedules subject to resource capacity and a
per-schedule active-run limit. Keep legacy scheduling semantics on the legacy path.

## 7. Kafka Contract and Routing

Use `confluent-kafka` in isolated worker roles. Topic names are provisioned from
registered gateways/executors, never arbitrary user-supplied routing strings.

| Topic | Key | Consumer ownership |
| --- | --- | --- |
| `scheduler.commands.<gateway_id>` | `device_id` | Equivalent replicas of that gateway |
| `scheduler.model.commands` | `node_run_id` | Equivalent model workers |
| `scheduler.parse.commands` | `node_run_id` | Equivalent parser workers |
| `scheduler.events` | `run_id` | One logical engine group; audit uses a separate group |
| `scheduler.dlq` | `message_id` | Operator inspection, never automatic hardware replay |

Different device gateways do not compete in one group for arbitrary commands.
The message key controls partition selection, not device ownership. Preserve
partitioning while device commands are in flight; capacity locks remain necessary
even when Kafka preserves partition order.

Every envelope carries `schema_version`, `message_id`, `message_type`, `run_id`,
`workflow_version_id`, `node_id`, `node_run_id`, `attempt`, `command_id`,
`gateway_id`/`device_id` when applicable, `occurred_at`, `expires_at` for commands,
and a typed `payload`. Events correlate to the same command/attempt and may carry
an executor sequence number. File-heavy results carry approved data references,
including existing `manage_db_record_id`, not raw files in Kafka.

Commands include execute, cancel, and query-status. Events include accepted,
started, progress, completed, failed, and cancelled. Validate ownership and the
legal state transition in addition to the envelope; late progress cannot overwrite
a terminal result. Duplicate IDs with different payloads are quarantined.

## 8. Reliable Delivery and Failure Handling

1. In one MySQL transaction, reserve/claim the node attempt, update its state,
   and insert its outgoing envelope into the outbox.
2. Publish only committed rows using `acks=all`, an idempotent producer, delivery
   callbacks, bounded timeouts, and configured retention/replication. A successful
   enqueue into the client buffer is not broker confirmation.
3. Mark sent after confirmed delivery. A crash before marking sent can cause
   republishing, so business `message_id`/`command_id` deduplication is mandatory
   even with producer idempotence.
4. The event consumer processes each partition in order, with automatic offset
   commit/store disabled. Persist inbox disposition, outputs, node transitions,
   downstream outbox rows, and projections atomically; then commit the processed
   offset. Never commit past an unfinished earlier message in the same partition.
5. A gateway/worker durably accepts a job before committing its command offset,
   then executes outside the poll loop. It persists results and its own outbound
   events so a crash does not silently lose an accepted job or completion message.
6. Database/network transients retry with bounded backoff without acknowledging
   unpersisted work. Permanent invalid messages are durably quarantined and get a
   DLQ outbox entry before their source offsets are committed. DLQ publication also
   survives a broker outage through the outbox.

Kafka transactions do not atomically include MySQL or physical device effects.
The contract is at-least-once delivery plus durable idempotent business handling,
not exactly-once hardware execution. Replaying audit events must not regenerate
start commands. Live event ingestion checks attempt ownership, expiry, state, and
deduplication. Operator retries are explicit, audited actions.

Retry delays are MySQL `next_attempt_at` deadlines, not an assumed Kafka delayed
queue feature. Broker failure leaves commands pending and UI reads available;
the system must never silently substitute an HTTP command or a simulated success.

Production uses TLS/SASL and topic ACLs. A gateway may consume its command topic
and publish authorized events, not consume another device's commands. Validate
data references and avoid logging credentials or Kafka authentication secrets.

## 9. HTTP, Model, and UI Integration

Add workflow draft/version/type APIs, schedule binding, runtime node-state APIs,
audited retry/approval/cancel operations, and messaging health visibility.
Existing upload/data-package APIs remain supported. Workflow-bound requests carry
`node_run_id` and `attempt`; an unqualified legacy upload may resolve only when
there is exactly one compatible waiting node within the explicitly correlated
execution/batch and component. Ambiguous same-type nodes return a conflict rather
than assigning a result to whichever node appears first or to the latest execution.

HTTP and Kafka converge on the same validated, idempotent result handler. Clients
provide a stable message/request ID; identical retries return the existing result,
while conflicting payload reuse is rejected. Preserve explicit attempt/data links
and existing Manage DB parsing rather than duplicating domain parsers in the engine.

The model worker calls existing model/recommendation functions only after required
results are valid, with an explicit source execution, node attempt, pinned model,
and input/output bindings. Refactor schedule-latest lookup behind an explicit
context-aware adapter; never let a concurrent new round change a job's inputs.
Only the template's designated closed-loop decision node may schedule the next
round. Other model nodes can produce independent results without modifying the
schedule. Model completion stores the recommendation, and the engine applies the
designated decision once per source round. The legacy feedback auto-inference
hook does not run for workflow-bound executions. Terminal workflow feedback uses
the existing health/campaign projection logic without launching inference again.
Each closed-loop version declares required result/terminal nodes; success closes
the round once all required active nodes succeed and the round's outputs validate.

Add a separate workflow-builder view using Vue Flow for adding/deleting/duplicating
nodes, port connections, configuration, device selection, validation, draft saving,
and version publication. Reuse the application's light/dark theme. Store UI
positions separately from executable semantics; keep the default template's
current graph positions.

The homepage retains its existing header, panel widths, heights, gaps, prediction
deck, and canvas viewport. Read runtime `nodes + edges` for new executions; keep
legacy rendering as fallback. Arbitrary graphs use the existing bounded viewport
with zoom/pan/fit; additional status nodes scroll inside the existing sidebar
instead of resizing the page. Do not combine an editor redesign with the homepage.

## 10. Delivery Boundaries and Acceptance

Deliver in three independently verifiable increments:

- Runtime foundation: versioned node definitions, registry, MySQL node/attempt
  state, default template, validation/build APIs, and legacy isolation.
- Kafka integration: publisher, event consumer, model/parser workers, gateway
  contract/reference adapter, outbox/inbox, health reporting, and optional local
  Kafka setup. Actual device control beyond a configured/test adapter requires its
  existing protocol and explicit device binding; do not invent hardware drivers.
- Builder and monitor integration: editing/publishing/binding workflows and
  dynamic runtime rendering inside the unchanged homepage geometry.

Required verification:

- Graph publication rejects cycles, duplicate IDs, invalid ports, and unknown
  types; two XRD instances remain independently correlated.
- Preparation does not succeed on dispatch; all three characterization nodes
  become ready together after preparation succeeds.
- Simultaneous final-result events create one model attempt and one next-round
  schedule. Output validation failure never opens the gate.
- Duplicate, reordered, expired, conflicting, and previous-attempt events cannot
  regress node state, repeat model execution, or start a second hardware operation.
- Crash tests cover commit-before-send, send-before-mark-sent, result-commit-before-
  offset-commit, gateway acceptance, lost completion publication, and rebalance.
- Real MySQL integration verifies locking/uniqueness and restart recovery; existing
  SQLite-based legacy tests do not prove MySQL concurrency correctness.
- A real Kafka smoke test sends a command to a simulator and receives correlated
  status/result events. No simulator sends real hardware commands. Missing broker
  availability is reported as an unrun integration test, not as a successful test.
- Existing scheduler/upload/model tests remain valid on the legacy path. Add
  workflow-bound API tests to prove that the legacy hooks cannot duplicate work.
- Frontend build and browser checks cover editor save/publish/errors, both themes,
  desktop/mobile overflow, default-page geometry, dynamic graph rendering, and
  persisted theme/version selection.

## 11. Primary References

- Kafka partitioning, consumer groups, and external-system delivery semantics:
  https://kafka.apache.org/40/design/design/
- Python Kafka client and manual offset management:
  https://docs.confluent.io/kafka-clients/python/current/overview.html
- NetworkX DAG validation and traversal:
  https://networkx.org/documentation/stable/reference/algorithms/dag.html
- Vue Flow custom nodes, edges, and editor interactions:
  https://vueflow.dev/guide/

## 12. Review and Next Stage

This document records the approved direction and implementation contract. The
user approved implementation according to this written specification on
2026-10-10. It does not claim that Kafka is installed or that business code has
changed. Prepare the implementation plan, review its increments/testing and
execution method, then implement. No automatic
startup, database mutation, dependency installation, or real device execution is
authorized by the existence of this document.

# Lightweight Workflow Coordination and Kafka: Phase 2

Phase 3 scheduling/isolation integration is now implemented. See
the newer [durable executor/heartbeat instructions](workflow-executors.md) for
phase-6 model/parser/simulator roles and implemented local cancellation tombstones.
Real gateway enforcement and real broker/MySQL checks remain prerequisites. See
[Workflow scheduling](workflow-scheduling.md) for the new binding/runtime API,
existing trigger/pause/resume integration and external-worker mode. Sections below
record the original phase-2 boundary; phase-3 instructions supersede its statements
that engine workers do not create schedule executions. Production activation,
domain executors remain separate prerequisites. Phase 4 now adds
[workflow APIs and node-aware HTTP uploads](workflow-api.md); its instructions
supersede the historical HTTP-upload boundary below.

## Implemented Boundary

This increment adds a flush-only coordinator and explicit Kafka publisher/event
consumer workers. The homepage, themes and existing legacy dispatcher are not
changed. This is not yet a drop-in replacement for legacy scheduling/upload APIs.
Production activation remains off until those integration guards are implemented.

- Publish device nodes using `gateway_id` and a registered `adapter_id`, with no
  device resource registration, availability checks, reservations or capacity locks.
- The receiver controls physical equipment safety and availability.
- Pin each run to a published version. Snapshot inputs/configuration/context in
  its attempt and command; later draft edits cannot change these snapshots.
- Preparation completion releases XRD, EDS and nanoindentation together. All
  three valid results are required before the join releases one model command.
- Internal joins pass matching input names to output names; conditions compare
  their configured `field` against a named input. Configure these ports explicitly.
  Invalid internal inputs/outputs produce `needs_attention`, not a fabricated result.
- Manual nodes await `WorkflowEngine.approve(...)`. Pause blocks new downstream
  commands, not receipt of results. It does not stop already queued/running jobs.
- Timeout records `needs_attention`; no node is automatically executed again,
  including compute nodes. RetryPolicy is retained in definitions for future
  explicit retry support, but device `max_attempts > 1` is rejected now.
- A valid late completion can resolve timeout uncertainty. Failure stops new
  downstream work while already-running siblings retain their own results.
- Terminal node results cannot be overwritten by late status/progress events.

There is no resource API, automatic equipment probing, hardware driver or hidden
fallback to HTTP commands. Only `WorkflowRun`/node state is finalized in this phase;
legacy execution feedback, campaign state and next-round scheduling are not yet
finalized by this engine. Public audited operation APIs, domain executors and
builder integration remain later work.

## Persistence and Initialization

Use local MySQL via the existing `SMART_CHEDULER_DATABASE_URL` configuration.
Do not rewrite application credentials or use its database as a disposable test DB.
Follow [additive initialization](workflow-foundation.md#mysql-initialization)
before starting workers. This work has not initialized your application database.
Existing resource tables/data are not dropped. SQLite is restricted to isolated
unit tests; production worker/initializer CLIs reject it.

Inbox message identity uses UTF-8 `VARBINARY(512)`, preserving case, accents and
trailing spaces independent of database collation. New schemas use this type.
Initializer/workers refuse an existing phase-1 `VARCHAR` inbox field; they do not
silently alter it. If you already initialized that schema, stop all writers, back
up the database, check existing data/charset and have the explicit migration
reviewed before applying it. For the original phase-1 `VARCHAR(128)` column:

```sql
ALTER TABLE workflow_message_inbox
  MODIFY message_id VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;
ALTER TABLE workflow_message_inbox
  MODIFY message_id VARBINARY(512) NOT NULL;
```

The first step ensures UTF-8 encoding before byte storage. MySQL DDL is not an
atomic application transaction. Prior collation-induced collisions cannot be
recovered by changing the type; audit existing inbox/quarantine records first.
No such migration was executed during implementation. See [MySQL binary string types](https://dev.mysql.com/doc/refman/8.4/en/binary-varbinary.html).

Services do not commit. The caller creates a **new workflow-owned execution**,
sets its `dispatch_payload.workflow_version_id` to the published version ID, and
starts it in the same transaction:

```python
engine = WorkflowEngine(session, settings.kafka.topic_prefix)
run = engine.start(new_execution, published_version, now)
```

Do not mark or adopt an existing legacy execution. The schedule integration that
will create these new owned executions is not implemented yet. Starting the
`engine` worker alone does not create executions or tick due legacy schedules.
It advances/monitors already-created workflow runs only. Runtime reads are
available through `workflow_runtime_read(session, execution_id)`; API routes follow.

## Kafka Configuration

Install dependencies from `backend/requirements.txt`. Kafka must be supplied
separately; this increment does not install/start a broker or containers.
Use environment variables with the existing intentional `SMART_CHEDULER` spelling:

```sh
export SMART_CHEDULER_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
export SMART_CHEDULER_KAFKA_TOPIC_PREFIX=scheduler
export SMART_CHEDULER_KAFKA_GROUP_ID=scheduler-engine
export SMART_CHEDULER_KAFKA_DELIVERY_TIMEOUT_MS=30000
export SMART_CHEDULER_KAFKA_OUTBOX_LEASE_SECONDS=60
export SMART_CHEDULER_KAFKA_POLL_INTERVAL_SECONDS=1
```

Provision topics explicitly on your cluster:

| Topic | Key | Receiver |
| --- | --- | --- |
| `scheduler.commands.<gateway_id>` | `node_run_id` | That gateway's equivalent replicas |
| `scheduler.model.commands` | `node_run_id` | Model executor |
| `scheduler.parse.commands` | `node_run_id` | Parser executor |
| `scheduler.events` | `run_id` | One logical coordinator consumer group |
| `scheduler.dlq` | Original message ID | Operator inspection only |

Different gateway services must not share a competing group for unrelated
gateway topics. Equivalent coordinator replicas must share the configured group.
Keys order messages per node/run, not equipment operations. Topic partitioning
does not provide equipment resource control. Keep topic prefix/group topology
stable while jobs are in flight. There is no automatic DLQ replay.

Local development defaults to PLAINTEXT. Production requires secured broker
listeners and least-privilege ACLs. Optional variables:
`SMART_CHEDULER_KAFKA_SECURITY_PROTOCOL` (`SSL` or `SASL_SSL`),
`SMART_CHEDULER_KAFKA_SASL_MECHANISM`, `SMART_CHEDULER_KAFKA_SASL_USERNAME`,
`SMART_CHEDULER_KAFKA_SASL_PASSWORD`, `SMART_CHEDULER_KAFKA_SSL_CA_LOCATION`.
Do not print these values or put real secrets in committed examples.

## Worker Commands

From `/Users/liye/Code/feed_back_sys/smart_scheduler/backend`, use separate processes:

```sh
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker engine
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker publisher
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker events
```

`--once` performs one cycle. It is not a broker health probe or a complete workflow
smoke test. Workers never start automatically in the FastAPI lifespan and never
initialize/reset tables. A failed worker cycle retains committed state; database
or offset failure does not acknowledge an unhandled message.

## Gateway Message Contract

Phase-4 cancellation requirement: `command.cancel` retains the targeted execute's
`command_id` and includes `payload.require_cancellation_tombstone: true`.
The gateway MUST persist a terminal cancellation record for this exact
run/version/node/attempt/command before acknowledging cancel or committing its
offset, even when no execute has arrived. Its execute acceptance path MUST check
that durable tombstone and refuse a late execute; restarted gateway replicas
must share the same durable command history. Do not delete tombstones while
their commands can still be accepted/replayed. Expired execute rejection remains
mandatory regardless of tombstone retention.

A scheduler publisher can suppress a pending command when its final check sees
cancellation, but another publisher may already be past that check or holding
Kafka-buffered bytes. Cancel can therefore overtake execute. No database/network
ordering in the coordinator proves physical device inactivity. Real gateways
without durable cancellation-before-execute handling are NOT safe to activate.
This is a required external gateway protocol; the durable executor phase and real
integration tests must implement/verify it before production hardware use.

Commands use schema version 1 and include canonical UUIDs for `run_id`,
`workflow_version_id`, `node_run_id` and `command_id`, plus instance `node_id`,
`attempt`, `gateway_id`, `adapter_id`, aware `occurred_at` and `expires_at`.
`message_id == command_id` for execute commands. Payload contains `node_type`,
`type_version`, `config`, immutable `inputs` and copied `execution_context`.
Deprecated resource/fencing fields are not needed for routing or execution.
Inline messages are bounded to 1 MB; large data belongs in approved external
data references, not raw files in Kafka.

The receiver must verify its gateway/adapter, reject expired commands and
**durably deduplicate `command_id` before execution**. It must persist acceptance
before committing its command offset. A duplicate command must not start a second
experiment. Scheduling service delivery retry uses exactly the same command ID;
producer idempotence alone does not cover restarts or database/broker boundaries.

Results copy all command correlation identities and use a **new stable event
message ID**, preserving that ID for retries of the same event. Supported event
types are `node.accepted`, `node.started`, `node.progress`, `node.completed`,
`node.failed` and `node.cancelled`. A business acceptance is distinct from broker
delivery confirmation. Completion payloads use explicitly declared output ports:

```json
{"outputs": {"sample": {"sample_id": "sample-a"}}}
```

For characterization, use `{"outputs":{"result":{...}}}`; model nodes use
`{"outputs":{"candidate":{...}}}`. Failed events use `{"error":"reason"}`.
Acceptance/start payloads may be empty; progress may carry `progress` and `note`.
Events do not need command expiry. Never associate a result with the latest round
or an instrument type alone: all instance/attempt identities must match.

## Delivery and Recovery

Attempt creation and its outbox command share a MySQL transaction. Publishers
commit a leased claim before networking, require the successful broker delivery
callback, then conditionally mark that same owner's claim sent. A crash after
broker acknowledgement but before DB marking may republish the same command;
gateway business deduplication is mandatory. Expired or terminal commands are
not replayed. Lease/backoff here governs messages, not physical equipment.

The event consumer processes one message at a time. Inbox reservation, validated
state changes and downstream outbox commands commit before its synchronous offset
commit. Failed DB/offset commits retain that same message for retry; it never
polls a later offset first. Duplicate event IDs with identical normalized payloads
have no repeated effects. Conflicting IDs and invalid envelopes/results are
persisted in quarantine together with a DLQ outbox record, before acknowledging
the source offset. DLQ raw previews are bounded/truncated; original digests remain.
DLQ may contain experiment data and needs restricted access.

In continuous CLI mode, actual Kafka exceptions and DB failures close/recreate
the consumer from committed broker offsets rather than staying stuck across a
rebalance. Inbox deduplication preserves state if the DB committed but the old
offset commit failed. `--once` reports failure instead of entering recovery loops.
Failed workflows still monitor active sibling attempt deadlines, without
reopening the run or issuing downstream commands.

This is at-least-once transport with business deduplication, not an atomic
MySQL/Kafka/device transaction. See [Confluent Python client documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
for delivery callbacks and manual offset management.

## Verification Boundary

Focused checks:

```sh
/opt/anaconda3/envs/dev/bin/python -m pytest tests/test_workflow_definition.py tests/test_workflow_runtime.py tests/test_kafka_messaging.py
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker --help
```

Unit tests use temporary databases and transport-boundary doubles. They exercise
coordinator behavior and crash/retry/offset ordering, not live MySQL row-lock races
or a broker round trip. At implementation time port 9092 had no listener and the
Docker daemon was unavailable; real Kafka/disposable-MySQL integration has not run.
No real device, production schema initialization or production worker was started.

The full legacy suite has ten pre-existing failures, listed in the implementation
plan. Do not report the full suite green while those failures remain.

Final phase-2 verification: 110 focused tests passed; the full suite reported
138 passed and the same ten baseline failures (106.02s). Independent review
confirmed the material fixes in a bounded recheck, including actual SDK exception
tests and schema/codec guards. Compilation and CLI help checks passed. Actual
MySQL races/migration and Kafka broker/outage/rebalance integration remain unrun.

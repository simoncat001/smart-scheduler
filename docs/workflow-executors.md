# Durable Domain Executors: Phase 6

Date: 2026-10-12. Backend only; homepage geometry/themes remain unchanged.
Runtime uses local MySQL. No equipment registration, capacity control or resource
reservations. No production DDL, environment rewrite, broker, worker or hardware
service was started during implementation.

## Durable Execution Path

The engine freezes execution/version/node/attempt/command IDs, declared port
schemas, executor kind, timeouts and round context. A role-specific Kafka consumer
reserves exact inbox identity and commits one `ExecutorJob` plus acceptance-event
outbox before its offset. Database failure prevents acknowledgement.

A separate computation loop claims work and commits that claim, then invokes
adapters without database transactions or Kafka polling locks open. Short lease
renewals protect result ownership. Finish requires the current owner token and
live lease; stale/cancelled owners cannot overwrite results. Output/event outbox
persists atomically. Lost publication never creates a new execution job.
Transient result commit errors retain already-computed output and retry persistence
under the same token, without recomputation or a false failed event. Cancellation,
expiry or another owner fences that retry. If the process itself dies before
durable persistence, device uncertainty still requires reconciliation.

Invalid routing/ports, expired commands, changed-body ID reuse and poison messages
are refused/quarantined. Consumer recovery rejoins committed offsets; durable
inbox/job IDs cover redelivery. No HTTP execution or simulated-success fallback.
Old commands lacking immutable port/kind/timeout descriptors are refused, not
filled from the latest definition. Reconcile before creating a new pinned round;
no backfill/adoption was performed.

## Pinned MAT Adapter

Built-in model ID: `mat-materials`. Version: `mat-sha256:<digest>`, over configured
model config and six existing JSON/CSV result artifacts. The worker hashes and
parses the same captured bytes, never the legacy mutable cache. Changed files fail
the pinned version instead of selecting a newer model. Retain version-specific
artifact directories while jobs run.

Existing MAT candidate/space/process functions receive explicit artifacts. This
selects from existing trained results, not online retraining or fresh global
Bayesian optimization. No shared model-file writes, latest-schedule query,
historical inference backfill or training download.

Inspect configured version without MySQL/Kafka connection or worker startup:

```sh
cd /Users/liye/Code/feed_back_sys/smart_scheduler/backend
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker model --describe-model
```

Use its ID/version in a draft before publishing. The template's `unconfigured`
version intentionally cannot be published. Worker artifact paths must match.
Input-free model nodes support initial selection. Feedback selection requires
named `xrd`, `eds`, `nanoindentation` inputs with phase, measured composition and
positive finite hardness. At target hardness, choose a nearby unused candidate;
otherwise reuse candidate order. Compact used-composition history comes from the
command, not a database query while the adapter runs.

## Parser and Simulator

`data_parse` / `manage_db` takes one declared package-reference input and one
output. Reference includes `instrument_code` and usable observation/Manage DB
reference. Run/batch/sample must match the command. Existing parser/validation
uses the worker's own settings outside state transactions; no latest-run lookup
or legacy feedback completion.

Simulator requires explicit opt-in and adapter `simulator`; `kafka_gateway` is
rejected. No real device driver is called. Default materials nodes get synthetic
preparation/XRD/EDS/hardness outputs tagged `simulated: true`. Declared schemas
still apply; incompatible output fails visibly. This proves no equipment behavior,
calibration or actual Kafka connection.

## Explicit Workers

After reviewed initialization/test-cluster setup, run roles explicitly. These are
instructions, not services started by this phase:

```sh
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker engine
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker publisher
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker events
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker model --executor-id model-worker
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker parser --executor-id parser-worker
/opt/anaconda3/envs/dev/bin/python -m app.workflows.worker simulator --executor-id device-gateway --enable-simulator
```

Domain executor ID must match the node's gateway ID; explicit
`SMART_CHEDULER_WORKER_EXECUTOR_ID` also supplies it. Equivalent replicas share
ID/group. Command groups differ from engine events. Compute topics are shared by
role; foreign logical gateway commands are rejected, not executed. Prefer one
logical gateway per compute role and one workflow namespace/prefix per MySQL DB.

`--once` handles one acceptance cycle and at most one job. Continuous mode keeps
polling separate from computation. Job lease defaults to 120 seconds, bounded
5-3600 by `--job-lease-seconds`. Leases protect result ownership, not equipment
occupancy. API startup does not launch domain workers.

## Cancel and Recover

Cancel durably stores a terminal command-ID tombstone before acknowledgement/
offset commit, even when execute has not arrived. Late execute is refused across
restart. In-flight compute may finish locally, but its token is invalidated and
cannot publish completion. Terminal jobs are not reclaimed. Actual gateways need
the [same contract](kafka-workflows.md#gateway-message-contract) plus physical
reconciliation before reporting cancellation of equipment already running.

An expired computational lease can recover the same pure job/command under a new
token, not a new workflow attempt or automatic node-timeout retry. Old holders
cannot renew/finish after expiry. Running device/simulator work after lease loss,
and accepted commands that expire before start, become `needs_attention` and emit
`node.needs_attention`; device work is never automatically replayed. A deadline
does not authorize another physical start.

## Designated Next-Round Plan

Structured proposals carry candidate/exhausted status, exact source execution,
pinned model ID/version, optional `next_payload`, summary/confidence. Other model
outputs remain ordinary data. Each model node retains separate output and
recommendation; none is flattened into the legacy single-row inference projection.

Only `decision_node_id`, after whole-run success, updates its schedule plan once.
It never dispatches equipment inside the event transaction. Pinned model config
`next_round_delay_seconds` defaults to 3600 (1-604800); a later normal tick creates
one next round under the existing exclusion guard. Paused schedules stay paused;
exhausted candidates pause auto scheduling. Failed/stale sources cannot replace
a later plan; round ordinals disambiguate database timestamp ties.

Operator payload edits during a run supersede its old proposal. The captured
schedule-payload digest no longer matches; recommendation becomes `superseded`
and the operator's payload remains. No legacy auto-inference/dispatch hook runs.

## Health and Verification

Workers record per-instance heartbeats. Messaging health reports fresh `online`,
stale/explicitly stopped `offline`, or `unknown` with no record, plus last-seen and
instances. This is freshness, not proof of device inactivity, live broker or job
success. Broker stays `unknown` (no probe). Heartbeats are excluded from message
backlog counts. No credentials/bootstrap addresses are exposed.

Final: 229 focused tests passed (9.87s); full suite 257 passed / same ten
pre-existing failures (118.89s), exit 1. Full evidence:
`.superpowers/sdd/2026-10-10-kafka-workflow-implementation/phase6-suite-final-verified.log`.
The full simulated graph confirms pinned MAT round completion and exactly one
delayed next round. Independent bounded recheck passed 11 targeted tests and 7
isolated checks; no remaining P1/P2 findings. The corrected fault test asserts
that a real OperationalError was injected before result commit.
Isolated database/transport checks do not prove real Kafka/MySQL races or physical
safety. Builder/monitor and real disposable MySQL/Kafka integration remain Tasks
7/8. No production equipment activation for preview.

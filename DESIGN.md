# Design

## Scenario

Image Task Runner accepts a batch of image-processing tasks represented as a directed acyclic graph (DAG). A task cannot run until every dependency is successful. For example:

`Resize -> Watermark`

and:

`Resize -> Compress -> Thumbnail`

or a thumbnail task can depend on both `Resize` and `Compress` when the application needs both prerequisites completed before thumbnail generation.

## Architecture

- **FastAPI** exposes the HTTP API.
- **SQLite** is the durable task store. It holds task state, dependency edges, retry counters, priorities, and timestamps.
- **Runner** is an in-process scheduler. It repeatedly:
  1. marks tasks with failed/blocked/cancelled dependencies as `BLOCKED`;
  2. claims ready tasks in priority order;
  3. launches at most `N` workers;
  4. retries failures with exponential delay.
- **Pillow** performs image transformations in a thread via `asyncio.to_thread`, keeping the FastAPI event loop responsive.
- **Atomic output** writes use a temporary file in the destination directory followed by `os.replace`.

## Dependency model

Dependencies are stored as edges in a separate table. Readiness is expressed as an SQL predicate: there must be no dependency whose state is anything other than `SUCCESS`. This avoids trying to maintain a fragile in-memory dependency counter.

Cycles are rejected before insertion using DFS over the submitted task graph. Unknown dependencies are also rejected.

## Concurrency

The scheduler claims only one task at a time and maintains an in-process set of active worker coroutines. It fills that set until its configured concurrency limit is reached. A task transitions to `RUNNING` before its worker starts, so the scheduler cannot claim the same task twice.

This guarantees the limit **inside one service process**. Running multiple service replicas would require a distributed lease/claim mechanism; SQLite plus this scheduler is deliberately not presented as a multi-node queue.

## Restart safety

A process can die while a task is `RUNNING`. On startup, all `RUNNING` tasks are reset to `WAITING`. They are then executed again from the beginning.

Outputs are never written directly to the final path. A worker writes to a temporary file and atomically replaces the final output only after a successful image save. Thus a restarted task sees either the old complete output or the new complete output, not a half-written file.

## Retry behavior

With base backoff `B`, retry number `r` waits:

`B * 2^(r-1)` seconds.

For example, with `B=0.25`, delays are 0.25s, 0.5s, 1s. `retry_count` increments when a retry is scheduled. Once the retry budget is exhausted, the task becomes `FAILED`.

## Cancellation

Cancellation traverses the dependency graph downstream and marks all `WAITING` and `RUNNING` tasks in that closure as `CANCELLED`. Dependents therefore cannot execute after a cancelled prerequisite.

## Improvement: priority

Priority was added because FIFO alone is poor for mixed workloads. Each task has `urgent`, `normal`, or `background` priority. The scheduler orders ready work by priority before creation time. This is intentionally simple: priority affects ordering among ready tasks but does not preempt a task already running.

## Image operation contract

- `resize`: `width`, `height`; preserves aspect ratio using Pillow `thumbnail`.
- `compress`: `quality`; JPEG quality is applied when the output is JPEG.
- `filter`: `filter` values `grayscale`, `blur`, `sharpen`, or `brightness`.
- `watermark`: `text`, optional `opacity`, `font_size`, and `font`.
- `thumbnail`: square output controlled by `size`.

This is an MVP contract. A production version should move file ingestion and output storage to object storage and should validate paths against an allowed workspace.

## Correctness Q&A

1. How do you make sure your concurrency limit is never exceeded, even when many tasks are submitted at the same moment? What could go wrong if you got this wrong?

The concurrency limit is enforced by the scheduler before a task is started.

The main enforcement is in Runner.\_loop() in app/main.py:

while len(self.tasks) < self.config.concurrency:
row = await self.store.claim_ready()
if not row:
break

    task = asyncio.create_task(self._run(row))
    self.tasks.add(task)

self.tasks represents the currently active worker coroutines. The scheduler will not claim another task once that count reaches the configured limit.

There is a second important protection in Store.claim_ready(). Selecting a task and changing it from WAITING to RUNNING happens under the store's asyncio.Lock and in one database transaction:

async with self.lock:
...
updated = self.conn.execute(
"""
UPDATE tasks
SET state = 'RUNNING', started_at = ?, error = NULL
WHERE id = ? AND state = 'WAITING'
""",
(now, row["id"]),
)

This means two scheduler claims cannot simultaneously claim the same task in this single-process service.

What could go wrong if this were implemented incorrectly?

If the limit were only checked when tasks were submitted, or if task claiming were not coordinated, two requests could both observe available capacity and start workers at the same time. For example, with N=2, five simultaneous submissions could accidentally start five image-processing operations.

That would cause:

CPU and memory usage to exceed the configured capacity.

Several large Pillow operations to compete for resources.

Poor latency for every task rather than controlled queueing.

Potential process instability or out-of-memory failures.

The service's advertised concurrency guarantee would be false.

Scope: this guarantee is for the current single-process scheduler. Running multiple independent Uvicorn worker processes would require a shared distributed concurrency mechanism; the current in-memory worker count is not sufficient for that deployment model.

2. If the service is killed while tasks are running, what exactly happens when it starts again? Say clearly whether any work could be lost or accidentally run twice.

The service deliberately uses restart-from-scratch / at-least-once execution semantics.

When a task is claimed, its database state becomes RUNNING. If the process is killed before the task finishes, the task remains RUNNING in SQLite because the process cannot complete the normal success/failure transition.

During application startup, Store.open() executes:

UPDATE tasks
SET
state = 'WAITING',
started_at = NULL,
error = 'worker restarted; task reset'
WHERE state = 'RUNNING'

Therefore, every task that was RUNNING when the process died is returned to WAITING and is eligible to run again from the beginning.

What is lost?

In-progress computation can be lost. For example, if Resize has processed 80% of an image in memory and the process is killed, that computation is discarded. The task starts again from the beginning after restart.

The service does not try to resume partially completed image processing.

Can a task run twice?

Yes. This is intentional and must be stated explicitly.

There is a small but real window between successfully writing an output and recording SUCCESS in SQLite. The image writer uses an atomic temporary-file + os.replace() sequence, but the database update is a separate operation.

For example:

Task is RUNNING.

Image output is successfully written and atomically replaced into its final path.

The process is killed before mark_success() commits SUCCESS.

On restart, the database still says RUNNING.

Startup resets it to WAITING.

The task runs again and overwrites the output atomically.

Therefore the system provides durable task state with restart-from-scratch, at-least-once execution, not exactly-once execution.

This tradeoff is deliberate because exactly-once semantics across a database transaction and filesystem output would require a substantially more complex design, such as durable job/output versioning or an external storage system with transactional coordination.

The atomic output write still matters: it prevents a killed process from exposing a partially written final image.

3. When several tasks are ready and one slot frees up, which one runs next? Explain your rule and give an example where it gives a poor result.

Ready tasks are ordered by:

Priority: urgent first, then normal, then background.

Creation time: earlier-created tasks win when priority is equal.

The rule is implemented in Store.claim_ready():

ORDER BY
CASE t.priority
WHEN 'urgent' THEN 0
WHEN 'normal' THEN 1
ELSE 2
END,
t.created_at
LIMIT 1

A task is considered ready only when it is WAITING, its retry backoff has expired, and all dependencies are SUCCESS.

Example

Suppose the concurrency limit is 2 and these tasks are waiting:

Task

Priority

Estimated work

Background thumbnail

background

1 second

Normal compression

normal

1 second

Urgent resize

urgent

60 seconds

When a slot becomes free, the urgent resize runs first even though it is much more expensive.

That can be a poor result if the actual business goal is minimizing average completion time. A short normal task could have completed immediately, while the long urgent task occupies a worker for 60 seconds.

Another weakness is starvation: if urgent tasks continuously arrive, background tasks may remain waiting indefinitely.

The priority feature is therefore a simple and predictable scheduling policy, not an optimal scheduling algorithm. A production system might use aging, deadlines, weighted fair scheduling, or separate worker pools to prevent starvation.

4. What is the one thing that must always be true for your service to be considered correct? Point to where in your code that is enforced.

The most important correctness invariant is:

A task must never execute until every one of its dependencies has successfully completed.

Without this invariant, the service could produce an output from incomplete or invalid input even though the dependency graph says the task is not ready.

This is enforced in Store.claim_ready() by the SQL condition:

AND NOT EXISTS (
SELECT 1
FROM dependencies d
JOIN tasks dep
ON dep.id = d.depends_on_id
WHERE d.task_id = t.id
AND dep.state <> 'SUCCESS'
)

A task can therefore only be claimed when no dependency exists whose state is anything other than SUCCESS.

Failed dependency propagation is separately enforced by Store.block_failed_dependents(), which changes waiting downstream tasks to BLOCKED when a dependency becomes FAILED, BLOCKED, or CANCELLED.

Circular dependency protection is enforced before insertion by validate_dag() using depth-first search. This prevents a graph from being accepted if traversal encounters a node already in the current visiting set.

The concurrency invariant is also critical: the scheduler must never have more than N active workers. That is enforced by Runner.\_loop() together with the serialized claim operation in Store.claim_ready().

Improvement: Priority Scheduling

The improvement added to the baseline design is task priority:

urgent

normal

background

Priority is persisted with the task and considered whenever the scheduler selects the next ready task. This allows latency-sensitive work to move ahead of lower-priority work without bypassing dependencies.

Importantly, priority does not override dependencies. An urgent task that depends on a background task cannot run until its dependency succeeds.

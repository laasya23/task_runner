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

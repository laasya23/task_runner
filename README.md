# Image Task Runner

A small FastAPI service for executing image-processing tasks as a dependency-aware DAG.

## Features

- Resize, compress, filter, watermark, and thumbnail operations using Pillow.
- Arbitrary task dependencies with submission-time cycle detection.
- Strict configurable concurrency limit.
- Priority scheduling: `urgent` > `normal` > `background`.
- Exponential retry backoff.
- Failed/blocked/cancelled dependencies prevent downstream execution.
- Cancellation cascades to all dependents.
- SQLite state persistence. Tasks found `RUNNING` after a process restart are reset to `WAITING` and rerun from scratch.
- Atomic output replacement via a temporary file, so a failed/restarted task does not expose a partial output.

## Setup

Python 3.11+ is recommended.

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell: .venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

## Run

```bash
uvicorn app.main:app --reload
```

Configuration is available through environment variables:

- `IMAGE_TASK_RUNNER_CONCURRENCY` — maximum simultaneous tasks; default `2`.
- `IMAGE_TASK_RUNNER_MAX_RETRIES` — default retry count; default `3`.
- `IMAGE_TASK_RUNNER_BACKOFF` — base backoff in seconds; default `0.25`.
- `IMAGE_TASK_RUNNER_DATA` — data directory; default `.image-task-runner`.
- `IMAGE_TASK_RUNNER_DB` — SQLite database path.

## API

### POST `/submit`

```json
{
  "tasks": [
    {
      "name": "resize",
      "operation": "resize",
      "input_path": "/tmp/source.jpg",
      "output_path": "/tmp/resized.jpg",
      "params": {"width": 1200, "height": 1200},
      "priority": "urgent"
    },
    {
      "name": "watermark",
      "operation": "watermark",
      "output_path": "/tmp/watermarked.jpg",
      "depends_on": ["resize"],
      "params": {"text": "CONFIDENTIAL"}
    }
  ]
}
```

When `input_path` is omitted, the first dependency's `output_path` is used. This makes simple pipelines concise. A task can depend on multiple tasks; all dependencies must succeed before it starts.

The response maps submitted names to generated task IDs:

```json
{"task_ids": {"resize": "...", "watermark": "..."}}
```

### GET `/status/{task_id}`

Returns state, retry count, max retries, error, output path, and priority.

States: `WAITING`, `RUNNING`, `SUCCESS`, `FAILED`, `BLOCKED`, `CANCELLED`.

### POST `/cancel/{task_id}`

Cancels the selected task and all of its dependents. A running image operation is not forcibly killed; its state is prevented from becoming `SUCCESS` only if cancellation races with completion. For production hard cancellation, use process isolation.

### GET `/stats`

Returns counts for running, waiting, successful, failed, blocked, and cancelled tasks plus the configured concurrency limit and current active workers.

## Test

```bash
pytest -q
```
#

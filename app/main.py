from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
from PIL import (
    Image,
    ImageEnhance,
    ImageFilter,
    ImageOps,
    ImageDraw,
    ImageFont,
)


# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(
    os.getenv("IMAGE_TASK_RUNNER_DATA", ".image-task-runner")
)

DB_PATH = Path(
    os.getenv(
        "IMAGE_TASK_RUNNER_DB",
        str(BASE_DIR / "tasks.db"),
    )
)

DEFAULT_CONCURRENCY = int(
    os.getenv("IMAGE_TASK_RUNNER_CONCURRENCY", "2")
)

DEFAULT_MAX_RETRIES = int(
    os.getenv("IMAGE_TASK_RUNNER_MAX_RETRIES", "3")
)

DEFAULT_BACKOFF = float(
    os.getenv("IMAGE_TASK_RUNNER_BACKOFF", "0.25")
)

POLL_INTERVAL = 0.05

PRIORITY = {
    "urgent": 0,
    "normal": 1,
    "background": 2,
}


# ============================================================
# Enums
# ============================================================

class Operation(str, Enum):
    resize = "resize"
    compress = "compress"
    filter = "filter"
    watermark = "watermark"
    thumbnail = "thumbnail"


class Priority(str, Enum):
    urgent = "urgent"
    normal = "normal"
    background = "background"


class TaskState(str, Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


# ============================================================
# API Models
# ============================================================

class TaskSpec(BaseModel):
    name: str = Field(min_length=1)

    operation: Operation

    input_path: str | None = None

    output_path: str

    params: dict[str, Any] = Field(default_factory=dict)

    depends_on: list[str] = Field(default_factory=list)

    priority: Priority = Priority.normal

    max_retries: int | None = Field(
        default=None,
        ge=0,
        le=20,
    )

    @field_validator("depends_on")
    @classmethod
    def unique_deps(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError(
                "depends_on contains duplicates"
            )

        return value


class SubmitRequest(BaseModel):
    tasks: list[TaskSpec] = Field(
        min_length=1,
        max_length=1000,
    )


class SubmitResponse(BaseModel):
    task_ids: dict[str, str]


@dataclass
class Config:
    concurrency: int = DEFAULT_CONCURRENCY
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff: float = DEFAULT_BACKOFF


# ============================================================
# SQLite Store
# ============================================================

class Store:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self.conn: sqlite3.Connection | None = None

    async def open(self):
        self.db_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
        )

        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                name TEXT NOT NULL,
                operation TEXT NOT NULL,
                input_path TEXT,
                output_path TEXT NOT NULL,
                params TEXT NOT NULL,
                priority TEXT NOT NULL,
                max_retries INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                next_run_at REAL NOT NULL DEFAULT 0,
                error TEXT,
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                UNIQUE(batch_id, name)
            );

            CREATE TABLE IF NOT EXISTS dependencies (
                task_id TEXT NOT NULL
                    REFERENCES tasks(id)
                    ON DELETE CASCADE,

                depends_on_id TEXT NOT NULL
                    REFERENCES tasks(id)
                    ON DELETE CASCADE,

                PRIMARY KEY(task_id, depends_on_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_ready
            ON tasks(state, next_run_at, priority);
            """
        )

        # Restart safety:
        #
        # If the process dies while a task is RUNNING,
        # there is no worker anymore.
        #
        # Reset the task to WAITING so it runs from scratch.
        #
        # Outputs are written atomically by ImageProcessor.
        self.conn.execute(
            """
            UPDATE tasks
            SET
                state = 'WAITING',
                started_at = NULL,
                error = 'worker restarted; task reset'
            WHERE state = 'RUNNING'
            """
        )

        self.conn.commit()

    async def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _execute(
        self,
        sql: str,
        args=(),
    ):
        assert self.conn is not None

        cursor = self.conn.execute(
            sql,
            args,
        )

        self.conn.commit()

        return cursor

    async def insert_batch(
        self,
        specs: list[TaskSpec],
    ) -> dict[str, str]:

        assert self.conn is not None

        async with self.lock:

            batch_id = str(uuid.uuid4())

            ids = {
                spec.name: str(uuid.uuid4())
                for spec in specs
            }

            now = time.time()

            try:

                for spec in specs:

                    self.conn.execute(
                        """
                        INSERT INTO tasks(
                            id,
                            batch_id,
                            name,
                            operation,
                            input_path,
                            output_path,
                            params,
                            priority,
                            max_retries,
                            state,
                            created_at
                        )
                        VALUES(
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            'WAITING',
                            ?
                        )
                        """,
                        (
                            ids[spec.name],
                            batch_id,
                            spec.name,
                            spec.operation.value,
                            spec.input_path,
                            spec.output_path,
                            json.dumps(spec.params),
                            spec.priority.value,
                            (
                                DEFAULT_MAX_RETRIES
                                if spec.max_retries is None
                                else spec.max_retries
                            ),
                            now,
                        ),
                    )

                for spec in specs:

                    for dependency in spec.depends_on:

                        self.conn.execute(
                            """
                            INSERT INTO dependencies(
                                task_id,
                                depends_on_id
                            )
                            VALUES(?, ?)
                            """,
                            (
                                ids[spec.name],
                                ids[dependency],
                            ),
                        )

                self.conn.commit()

            except Exception:
                self.conn.rollback()
                raise

            return ids

    async def get(
        self,
        task_id: str,
    ) -> sqlite3.Row | None:

        async with self.lock:

            assert self.conn is not None

            cursor = self.conn.execute(
                """
                SELECT *
                FROM tasks
                WHERE id = ?
                """,
                (task_id,),
            )

            return cursor.fetchone()

    async def get_deps(
        self,
        task_id: str,
    ) -> list[sqlite3.Row]:

        async with self.lock:

            assert self.conn is not None

            cursor = self.conn.execute(
                """
                SELECT t.*
                FROM tasks t
                JOIN dependencies d
                    ON d.depends_on_id = t.id
                WHERE d.task_id = ?
                """,
                (task_id,),
            )

            return cursor.fetchall()

    async def claim_ready(
        self,
    ) -> sqlite3.Row | None:

        async with self.lock:

            assert self.conn is not None

            now = time.time()

            # A task is runnable only when:
            #
            # 1. It is WAITING.
            # 2. Retry backoff has expired.
            # 3. Every dependency is SUCCESS.
            #
            # Priority is evaluated here so URGENT tasks are
            # selected before NORMAL and BACKGROUND tasks.

            row = self.conn.execute(
                """
                SELECT t.*
                FROM tasks t

                WHERE t.state = 'WAITING'

                AND t.next_run_at <= ?

                AND NOT EXISTS (
                    SELECT 1
                    FROM dependencies d

                    JOIN tasks dep
                        ON dep.id = d.depends_on_id

                    WHERE d.task_id = t.id

                    AND dep.state <> 'SUCCESS'
                )

                ORDER BY
                    CASE t.priority
                        WHEN 'urgent' THEN 0
                        WHEN 'normal' THEN 1
                        ELSE 2
                    END,

                    t.created_at

                LIMIT 1
                """,
                (now,),
            ).fetchone()

            if not row:
                return None

            updated = self.conn.execute(
                """
                UPDATE tasks

                SET
                    state = 'RUNNING',
                    started_at = ?,
                    error = NULL

                WHERE id = ?

                AND state = 'WAITING'
                """,
                (
                    now,
                    row["id"],
                ),
            )

            self.conn.commit()

            if not updated.rowcount:
                return None

            return row

    async def mark_success(
        self,
        task_id: str,
    ):

        async with self.lock:

            self._execute(
                """
                UPDATE tasks

                SET
                    state = 'SUCCESS',
                    finished_at = ?,
                    error = NULL

                WHERE id = ?

                AND state = 'RUNNING'
                """,
                (
                    time.time(),
                    task_id,
                ),
            )

    async def mark_failure_or_retry(
        self,
        task_id: str,
        error: str,
        backoff: float,
    ):

        async with self.lock:

            assert self.conn is not None

            row = self.conn.execute(
                """
                SELECT
                    retry_count,
                    max_retries,
                    state

                FROM tasks

                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()

            if not row:
                return

            if row["state"] != "RUNNING":
                return

            if row["retry_count"] < row["max_retries"]:

                retry = row["retry_count"] + 1

                # Exponential backoff:
                #
                # retry 1 = backoff * 1
                # retry 2 = backoff * 2
                # retry 3 = backoff * 4
                # retry 4 = backoff * 8

                delay = backoff * (
                    2 ** (retry - 1)
                )

                self._execute(
                    """
                    UPDATE tasks

                    SET
                        state = 'WAITING',
                        retry_count = ?,
                        next_run_at = ?,
                        error = ?,
                        started_at = NULL

                    WHERE id = ?
                    """,
                    (
                        retry,
                        time.time() + delay,
                        error,
                        task_id,
                    ),
                )

            else:

                self._execute(
                    """
                    UPDATE tasks

                    SET
                        state = 'FAILED',
                        finished_at = ?,
                        error = ?,
                        started_at = NULL

                    WHERE id = ?
                    """,
                    (
                        time.time(),
                        error,
                        task_id,
                    ),
                )

    async def cancel_cascade(
        self,
        task_id: str,
    ) -> int:

        async with self.lock:

            assert self.conn is not None

            row = self.conn.execute(
                """
                SELECT id
                FROM tasks
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()

            if not row:
                return 0

            # Start with the requested task.
            ids = {task_id}

            changed = True

            # Find every downstream dependent recursively.
            while changed:

                changed = False

                placeholders = ",".join(
                    "?" * len(ids)
                )

                rows = self.conn.execute(
                    f"""
                    SELECT task_id
                    FROM dependencies

                    WHERE depends_on_id IN (
                        {placeholders}
                    )
                    """,
                    tuple(ids),
                ).fetchall()

                for dependency in rows:

                    dependent_id = dependency[0]

                    if dependent_id not in ids:

                        ids.add(dependent_id)

                        changed = True

            placeholders = ",".join(
                "?" * len(ids)
            )

            cursor = self.conn.execute(
                f"""
                UPDATE tasks

                SET
                    state = 'CANCELLED',
                    finished_at = ?,
                    error = 'cancelled'

                WHERE id IN (
                    {placeholders}
                )

                AND state IN (
                    'WAITING',
                    'RUNNING'
                )
                """,
                (
                    time.time(),
                    *ids,
                ),
            )

            self.conn.commit()

            return cursor.rowcount

    async def block_failed_dependents(self):

        async with self.lock:

            assert self.conn is not None

            # Any WAITING task whose dependency has FAILED,
            # BLOCKED or CANCELLED becomes BLOCKED.

            self.conn.execute(
                """
                UPDATE tasks

                SET
                    state = 'BLOCKED',
                    finished_at = ?,
                    error =
                        'dependency failed or was blocked/cancelled'

                WHERE state = 'WAITING'

                AND id IN (

                    SELECT d.task_id

                    FROM dependencies d

                    JOIN tasks dep
                        ON dep.id = d.depends_on_id

                    WHERE dep.state IN (
                        'FAILED',
                        'BLOCKED',
                        'CANCELLED'
                    )
                )
                """,
                (time.time(),),
            )

            self.conn.commit()

    async def stats(self) -> dict[str, int]:

        async with self.lock:

            assert self.conn is not None

            rows = self.conn.execute(
                """
                SELECT
                    state,
                    COUNT(*) AS count

                FROM tasks

                GROUP BY state
                """
            ).fetchall()

            counts = {
                row["state"]: row["count"]
                for row in rows
            }

            return {
                "running": counts.get(
                    "RUNNING",
                    0,
                ),
                "waiting": counts.get(
                    "WAITING",
                    0,
                ),
                "success": counts.get(
                    "SUCCESS",
                    0,
                ),
                "failed": counts.get(
                    "FAILED",
                    0,
                ),
                "blocked": counts.get(
                    "BLOCKED",
                    0,
                ),
                "cancelled": counts.get(
                    "CANCELLED",
                    0,
                ),
            }


# ============================================================
# Image Processing
# ============================================================

class ImageProcessor:

    @staticmethod
    def _save_atomic(
        image: Image.Image,
        output: str,
        fmt: str | None = None,
        quality: int | None = None,
    ):

        target = Path(output)

        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        suffix = target.suffix or ".img"

        fd, temporary_path = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=suffix,
            dir=target.parent,
        )

        os.close(fd)

        try:

            save_format = (
                fmt
                or (
                    "JPEG"
                    if suffix.lower()
                    in {".jpg", ".jpeg"}
                    else "PNG"
                )
            )

            kwargs = {}

            if (
                quality is not None
                and save_format.upper() == "JPEG"
            ):

                kwargs["quality"] = quality
                kwargs["optimize"] = True

            # JPEG cannot store alpha.
            if (
                save_format.upper() == "JPEG"
                and image.mode in {
                    "RGBA",
                    "LA",
                    "P",
                }
            ):

                background = Image.new(
                    "RGB",
                    image.size,
                    "white",
                )

                if "A" in image.mode:

                    rgba = image.convert("RGBA")

                    background.paste(
                        rgba,
                        mask=rgba.getchannel("A"),
                    )

                else:

                    background.paste(
                        image.convert("RGB")
                    )

                image = background

            image.save(
                temporary_path,
                format=save_format,
                **kwargs,
            )

            # Atomic replacement.
            os.replace(
                temporary_path,
                target,
            )

        finally:

            if os.path.exists(temporary_path):

                os.unlink(
                    temporary_path
                )

    @classmethod
    def run(
        cls,
        operation: str,
        input_path: str,
        output_path: str,
        params: dict[str, Any],
    ):

        # Load a complete copy before processing.
        # This prevents us from holding the source file open
        # while writing the output.
        with Image.open(input_path) as source:

            image = source.copy()

        # ----------------------------------------------------
        # RESIZE
        # ----------------------------------------------------

        if operation == "resize":

            width = int(
                params.get(
                    "width",
                    image.width,
                )
            )

            height = int(
                params.get(
                    "height",
                    image.height,
                )
            )

            image.thumbnail(
                (
                    width,
                    height,
                ),
                Image.Resampling.LANCZOS,
            )

        # ----------------------------------------------------
        # COMPRESS
        # ----------------------------------------------------

        elif operation == "compress":

            quality = int(
                params.get(
                    "quality",
                    75,
                )
            )

            cls._save_atomic(
                image,
                output_path,
                quality=quality,
            )

            return

        # ----------------------------------------------------
        # FILTER
        # ----------------------------------------------------

        elif operation == "filter":

            filter_type = str(
                params.get(
                    "filter",
                    "grayscale",
                )
            )

            if filter_type == "grayscale":

                image = ImageOps.grayscale(
                    image
                )

            elif filter_type == "blur":

                radius = float(
                    params.get(
                        "radius",
                        2,
                    )
                )

                image = image.filter(
                    ImageFilter.GaussianBlur(
                        radius
                    )
                )

            elif filter_type == "sharpen":

                image = image.filter(
                    ImageFilter.SHARPEN
                )

            elif filter_type == "brightness":

                factor = float(
                    params.get(
                        "factor",
                        1.2,
                    )
                )

                image = ImageEnhance.Brightness(
                    image
                ).enhance(factor)

            else:

                raise ValueError(
                    f"unsupported filter: {filter_type}"
                )

        # ----------------------------------------------------
        # WATERMARK
        # ----------------------------------------------------

        elif operation == "watermark":

            text = str(
                params.get(
                    "text",
                    "Image Task Runner",
                )
            )

            opacity = int(
                params.get(
                    "opacity",
                    128,
                )
            )

            layer = Image.new(
                "RGBA",
                image.size,
                (0, 0, 0, 0),
            )

            draw = ImageDraw.Draw(
                layer
            )

            font_size = int(
                params.get(
                    "font_size",
                    max(
                        16,
                        image.width // 25,
                    ),
                )
            )

            try:

                font = ImageFont.truetype(
                    str(
                        params.get(
                            "font",
                            "DejaVuSans.ttf",
                        )
                    ),
                    font_size,
                )

            except OSError:

                font = ImageFont.load_default()

            bbox = draw.textbbox(
                (0, 0),
                text,
                font=font,
            )

            text_width = (
                bbox[2] - bbox[0]
            )

            text_height = (
                bbox[3] - bbox[1]
            )

            x = (
                image.width
                - text_width
                - 20
            )

            y = (
                image.height
                - text_height
                - 20
            )

            draw.text(
                (x, y),
                text,
                fill=(
                    255,
                    255,
                    255,
                    opacity,
                ),
                font=font,
            )

            image = Image.alpha_composite(
                image.convert("RGBA"),
                layer,
            )

        # ----------------------------------------------------
        # THUMBNAIL
        # ----------------------------------------------------

        elif operation == "thumbnail":

            size = int(
                params.get(
                    "size",
                    256,
                )
            )

            image = ImageOps.fit(
                image,
                (
                    size,
                    size,
                ),
                method=Image.Resampling.LANCZOS,
            )

        else:

            raise ValueError(
                f"unsupported operation: {operation}"
            )

        cls._save_atomic(
            image,
            output_path,
        )


# ============================================================
# Task Runner
# ============================================================

class Runner:

    def __init__(
        self,
        store: Store,
        config: Config,
    ):

        self.store = store
        self.config = config

        self.tasks: set[asyncio.Task] = set()

        self.stop_event = asyncio.Event()

        self.loop_task: asyncio.Task | None = None

    async def start(self):

        self.loop_task = asyncio.create_task(
            self._loop()
        )

    async def stop(self):

        self.stop_event.set()

        if self.loop_task:

            await self.loop_task

        if self.tasks:

            await asyncio.gather(
                *self.tasks,
                return_exceptions=True,
            )

    async def _loop(self):

        while not self.stop_event.is_set():

            # Propagate failed dependency states first.
            await self.store.block_failed_dependents()

            # Fill available worker slots.
            while (
                len(self.tasks)
                < self.config.concurrency
            ):

                row = await self.store.claim_ready()

                if not row:
                    break

                task = asyncio.create_task(
                    self._run(row)
                )

                self.tasks.add(task)

                task.add_done_callback(
                    self.tasks.discard
                )

            await asyncio.sleep(
                POLL_INTERVAL
            )

    async def _run(
        self,
        row: sqlite3.Row,
    ):

        task_id = row["id"]

        try:

            input_path = row["input_path"]

            # If no explicit input is provided,
            # use the first dependency's output.
            if not input_path:

                dependencies = (
                    await self.store.get_deps(
                        task_id
                    )
                )

                if dependencies:

                    input_path = dependencies[
                        0
                    ]["output_path"]

            if not input_path:

                raise ValueError(
                    "input_path is required "
                    "when the task has no dependency"
                )

            params = json.loads(
                row["params"]
            )

            # Pillow operations are synchronous.
            # Run them outside the event loop.
            await asyncio.to_thread(
                ImageProcessor.run,
                row["operation"],
                input_path,
                row["output_path"],
                params,
            )

            await self.store.mark_success(
                task_id
            )

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            await self.store.mark_failure_or_retry(
                task_id,
                f"{type(exc).__name__}: {exc}",
                self.config.backoff,
            )


# ============================================================
# Global Application State
# ============================================================

store = Store(DB_PATH)

runner = Runner(
    store,
    Config(),
)


# ============================================================
# FastAPI Lifecycle
# ============================================================

@asynccontextmanager
async def lifespan(
    app: FastAPI,
):

    await store.open()

    await runner.start()

    yield

    await runner.stop()

    await store.close()


app = FastAPI(
    title="Image Task Runner",
    version="1.0.0",
    lifespan=lifespan,
)


# ============================================================
# Root / Health Endpoint
# ============================================================

@app.get("/")
async def root():

    return {
        "service": "Image Task Runner",
        "status": "running",
        "docs": "/docs",
    }


# ============================================================
# Dependency Graph Validation
# ============================================================

def validate_dag(
    tasks: list[TaskSpec],
):

    names = [
        task.name
        for task in tasks
    ]

    # Duplicate names.
    if len(names) != len(set(names)):

        raise HTTPException(
            status_code=400,
            detail="task names must be unique",
        )

    known = set(names)

    graph = {
        task.name: task.depends_on
        for task in tasks
    }

    # Unknown dependencies.
    for name, dependencies in graph.items():

        missing = [
            dependency
            for dependency in dependencies
            if dependency not in known
        ]

        if missing:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"task {name} has unknown "
                    f"dependencies: {missing}"
                ),
            )

        # Self dependency.
        if name in dependencies:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"task {name} depends "
                    "on itself"
                ),
            )

    # Cycle detection using DFS.
    visiting: set[str] = set()

    visited: set[str] = set()

    def dfs(node: str):

        if node in visiting:

            raise HTTPException(
                status_code=400,
                detail="circular dependency detected",
            )

        if node in visited:
            return

        visiting.add(node)

        for dependency in graph[node]:

            dfs(dependency)

        visiting.remove(node)

        visited.add(node)

    for name in graph:

        dfs(name)


# ============================================================
# POST /submit
# ============================================================

@app.post(
    "/submit",
    response_model=SubmitResponse,
    status_code=202,
)
async def submit(
    request: SubmitRequest,
):

    validate_dag(
        request.tasks
    )

    try:

        task_ids = await store.insert_batch(
            request.tasks
        )

    except sqlite3.IntegrityError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    return {
        "task_ids": task_ids
    }


# ============================================================
# GET /status/{task_id}
# ============================================================

@app.get(
    "/status/{task_id}"
)
async def status(
    task_id: str,
):

    row = await store.get(
        task_id
    )

    if not row:

        raise HTTPException(
            status_code=404,
            detail="task not found",
        )

    return {
        "task_id": row["id"],
        "name": row["name"],
        "state": row["state"],
        "retry_count": row["retry_count"],
        "max_retries": row["max_retries"],
        "error": row["error"],
        "output_path": row["output_path"],
        "priority": row["priority"],
    }


# ============================================================
# POST /cancel/{task_id}
# ============================================================

@app.post(
    "/cancel/{task_id}"
)
async def cancel(
    task_id: str,
):

    row = await store.get(
        task_id
    )

    if not row:

        raise HTTPException(
            status_code=404,
            detail="task not found",
        )

    count = await store.cancel_cascade(
        task_id
    )

    return {
        "cancelled": count
    }


# ============================================================
# GET /stats
# ============================================================

@app.get(
    "/stats"
)
async def stats():

    result = await store.stats()

    result[
        "concurrency_limit"
    ] = runner.config.concurrency

    result[
        "active_workers"
    ] = len(runner.tasks)

    return result
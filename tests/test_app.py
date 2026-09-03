import asyncio
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from PIL import Image


def make_client(tmp_path, monkeypatch, concurrency=2, retries=1, backoff=0.01):
    monkeypatch.setenv("IMAGE_TASK_RUNNER_DATA", str(tmp_path / "data"))
    monkeypatch.setenv("IMAGE_TASK_RUNNER_DB", str(tmp_path / "data" / "tasks.db"))
    monkeypatch.setenv("IMAGE_TASK_RUNNER_CONCURRENCY", str(concurrency))
    monkeypatch.setenv("IMAGE_TASK_RUNNER_MAX_RETRIES", str(retries))
    monkeypatch.setenv("IMAGE_TASK_RUNNER_BACKOFF", str(backoff))
    import importlib
    import app.main as m
    importlib.reload(m)
    return TestClient(m.app), m


def image(tmp_path, name="in.png"):
    p = tmp_path / name
    Image.new("RGB", (100, 80), "white").save(p)
    return str(p)


def wait_for(client, task_id, states=("SUCCESS", "FAILED", "BLOCKED", "CANCELLED"), timeout=3):
    end = time.time() + timeout
    while time.time() < end:
        s = client.get(f"/status/{task_id}").json()
        if s["state"] in states:
            return s
        time.sleep(0.02)
    raise AssertionError(f"timed out: {s}")


def test_resize_and_status(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    out = str(tmp_path / "out.jpg")
    with c:
        r = c.post("/submit", json={"tasks":[{"name":"r","operation":"resize","input_path":src,"output_path":out,"params":{"width":40,"height":40}}]})
        tid = r.json()["task_ids"]["r"]
        s = wait_for(c, tid)
        assert s["state"] == "SUCCESS"
        assert s["retry_count"] == 0
        assert Image.open(out).size == (40, 32)


def test_cycle_rejected(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    with c:
        r = c.post("/submit", json={"tasks":[
            {"name":"a","operation":"resize","output_path":"a.png","depends_on":["b"]},
            {"name":"b","operation":"resize","output_path":"b.png","depends_on":["a"]}
        ]})
        assert r.status_code == 400
        assert "circular" in r.text.lower()


def test_failed_dependency_blocks_downstream(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch, retries=0)
    bad = str(tmp_path / "missing.png")
    out = str(tmp_path / "out.png")
    with c:
        r = c.post("/submit", json={"tasks":[
            {"name":"a","operation":"resize","input_path":bad,"output_path":out},
            {"name":"b","operation":"thumbnail","output_path":str(tmp_path/"thumb.png"),"depends_on":["a"]}
        ]})
        ids = r.json()["task_ids"]
        a = wait_for(c, ids["a"])
        b = wait_for(c, ids["b"])
        assert a["state"] == "FAILED"
        assert b["state"] == "BLOCKED"


def test_cancel_cascades(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    with c:
        r = c.post("/submit", json={"tasks":[
            {"name":"a","operation":"resize","input_path":src,"output_path":str(tmp_path/"a.png")},
            {"name":"b","operation":"thumbnail","output_path":str(tmp_path/"b.png"),"depends_on":["a"]}
        ]})
        ids = r.json()["task_ids"]
        assert c.post(f"/cancel/{ids['a']}").status_code == 200
        assert wait_for(c, ids["a"])["state"] == "CANCELLED"
        assert wait_for(c, ids["b"])["state"] == "CANCELLED"


def test_priority_and_concurrency_claiming(tmp_path, monkeypatch):
    c, m = make_client(tmp_path, monkeypatch, concurrency=1)
    src = image(tmp_path)
    with c:
        tasks=[]
        for i, pr in [("background","background"),("urgent","urgent"),("normal","normal")]:
            tasks.append({"name":i,"operation":"resize","input_path":src,"output_path":str(tmp_path/f"{i}.png"),"priority":pr})
        r=c.post("/submit",json={"tasks":tasks})
        ids=r.json()["task_ids"]
        # Give scheduler time to claim at least one; urgent should be claimed first.
        time.sleep(0.1)
        statuses=[c.get(f"/status/{ids[n]}").json()["state"] for n in ["background","urgent","normal"]]
        assert statuses[1] in {"RUNNING","SUCCESS"}
        for tid in ids.values(): wait_for(c,tid)
        assert c.get("/stats").json()["running"] == 0


def test_retry_count_increments(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch, retries=2, backoff=0.01)
    bad = str(tmp_path / "missing.png")
    with c:
        r=c.post("/submit",json={"tasks":[{"name":"x","operation":"resize","input_path":bad,"output_path":str(tmp_path/"x.png")} ]})
        tid=r.json()["task_ids"]["x"]
        s=wait_for(c,tid)
        assert s["state"]=="FAILED"
        assert s["retry_count"]==2


def test_unknown_task_and_stats(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    with c:
        assert c.get("/status/nope").status_code == 404
        s=c.get("/stats").json()
        assert s["running"] == 0
        assert s["waiting"] == 0

@pytest.mark.parametrize("operation,params,suffix", [
    ("compress", {"quality": 60}, ".jpg"),
    ("filter", {"filter": "grayscale"}, ".png"),
    ("watermark", {"text": "TEST", "font_size": 12}, ".png"),
    ("thumbnail", {"size": 32}, ".png"),
])
def test_remaining_image_operations(tmp_path, monkeypatch, operation, params, suffix):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    out = str(tmp_path / f"{operation}{suffix}")
    with c:
        r = c.post("/submit", json={"tasks":[{
            "name":"x", "operation":operation, "input_path":src,
            "output_path":out, "params":params
        }]})
        tid = r.json()["task_ids"]["x"]
        assert wait_for(c, tid)["state"] == "SUCCESS"
        assert Path(out).exists()


def test_restart_resets_running_to_waiting(tmp_path, monkeypatch):
    monkeypatch.setenv("IMAGE_TASK_RUNNER_DB", str(tmp_path / "restart.db"))
    import importlib
    import app.main as m
    importlib.reload(m)
    async def scenario():
        await m.store.open()
        spec = m.TaskSpec(name="x", operation=m.Operation.resize, input_path=image(tmp_path), output_path=str(tmp_path/"x.png"))
        ids = await m.store.insert_batch([spec])
        await m.store.claim_ready()
        assert (await m.store.get(ids["x"]))["state"] == "RUNNING"
        await m.store.close()
        fresh = m.Store(Path(tmp_path / "restart.db"))
        await fresh.open()
        row = await fresh.get(ids["x"])
        assert row["state"] == "WAITING"
        await fresh.close()
    asyncio.run(scenario())


def test_dependent_never_runs_before_dependency(tmp_path, monkeypatch):
    """A dependent task must not enter RUNNING until every dependency
    has succeeded. This is the core dependency-ordering guarantee."""
    c, _ = make_client(tmp_path, monkeypatch, concurrency=2, retries=0, backoff=0.01)
    src = image(tmp_path)
    with c:
        r = c.post("/submit", json={"tasks": [
            {"name": "a", "operation": "resize", "input_path": src,
             "output_path": str(tmp_path / "a.png"),
             "params": {"width": 40, "height": 40}},
            {"name": "b", "operation": "thumbnail",
             "output_path": str(tmp_path / "b.png"),
             "params": {"size": 16}, "depends_on": ["a"]},
        ]})
        ids = r.json()["task_ids"]
        # While 'a' has not yet succeeded, 'b' must stay WAITING/BLOCKED and
        # never RUNNING.
        deadline = time.time() + 3
        a_succeeded = False
        while time.time() < deadline:
            sa = c.get(f"/status/{ids['a']}").json()
            sb = c.get(f"/status/{ids['b']}").json()
            if sa["state"] == "SUCCESS":
                a_succeeded = True
            if not a_succeeded:
                assert sb["state"] in ("WAITING", "BLOCKED"), sb["state"]
            if a_succeeded and sb["state"] in ("SUCCESS", "BLOCKED", "CANCELLED"):
                break
            time.sleep(0.01)
        assert c.get(f"/status/{ids['a']}").json()["state"] == "SUCCESS"
        assert wait_for(c, ids["b"], timeout=5)["state"] == "SUCCESS"


def test_unknown_dependency_rejected(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    with c:
        r = c.post("/submit", json={"tasks": [
            {"name": "a", "operation": "resize", "input_path": src,
             "output_path": str(tmp_path / "a.png"), "depends_on": ["ghost"]}
        ]})
        assert r.status_code == 400
        assert "unknown" in r.text.lower()


def test_self_dependency_rejected(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    with c:
        r = c.post("/submit", json={"tasks": [
            {"name": "a", "operation": "resize", "input_path": src,
             "output_path": str(tmp_path / "a.png"), "depends_on": ["a"]}
        ]})
        assert r.status_code == 400
        assert "itself" in r.text.lower() or "circular" in r.text.lower()


def test_duplicate_task_names_rejected(tmp_path, monkeypatch):
    c, _ = make_client(tmp_path, monkeypatch)
    src = image(tmp_path)
    with c:
        r = c.post("/submit", json={"tasks": [
            {"name": "a", "operation": "resize", "input_path": src,
             "output_path": str(tmp_path / "a1.png")},
            {"name": "a", "operation": "resize", "input_path": src,
             "output_path": str(tmp_path / "a2.png")},
        ]})
        assert r.status_code == 400
        assert "unique" in r.text.lower()


def test_concurrency_never_exceeds_limit(tmp_path, monkeypatch):
    """Never more than N tasks running at once. Uses a deliberately slow
    image operation so the running window is wide enough to observe."""
    c, _ = make_client(tmp_path, monkeypatch, concurrency=2, retries=0, backoff=0.01)
    # A 2500x2500 blur takes ~200ms: wide enough to reliably observe running.
    src = str(tmp_path / "big.png")
    Image.new("RGB", (2500, 2500), "lime").save(src)
    tasks = [
        {"name": f"t{i}", "operation": "filter", "input_path": src,
         "output_path": str(tmp_path / f"o{i}.png"),
         "params": {"filter": "blur", "radius": 5}}
        for i in range(4)
    ]
    with c:
        r = c.post("/submit", json={"tasks": tasks})
        assert r.status_code == 202
        max_running = 0
        max_workers = 0
        deadline = time.time() + 6
        while time.time() < deadline:
            s = c.get("/stats").json()
            max_running = max(max_running, s["running"])
            max_workers = max(max_workers, s["active_workers"])
            if s["success"] == 4:
                break
            time.sleep(0.01)
        end = c.get("/stats").json()
        assert end["success"] == 4
        assert end["running"] == 0
        # Concurrency cap must never be exceeded (neither in DB nor in workers).
        assert max_running <= 2
        assert max_workers <= 2
        # The test is meaningful: we did observe active workers.
        assert max_running >= 1
        assert max_workers >= 1


def test_cancel_running_task_cascades(tmp_path, monkeypatch):
    """Cancelling a RUNNING task must cascade to its WAITING dependent so
    nothing waits forever."""
    c, _ = make_client(tmp_path, monkeypatch, concurrency=1, retries=0, backoff=0.01)
    src = str(tmp_path / "big.png")
    Image.new("RGB", (2500, 2500), "lime").save(src)
    with c:
        r = c.post("/submit", json={"tasks": [
            {"name": "a", "operation": "filter", "input_path": src,
             "output_path": str(tmp_path / "a.png"),
             "params": {"filter": "blur", "radius": 5}},
            {"name": "b", "operation": "thumbnail",
             "output_path": str(tmp_path / "b.png"),
             "params": {"size": 64}, "depends_on": ["a"]},
        ]})
        ids = r.json()["task_ids"]
        # Under concurrency=1, 'a' holds the only slot; 'b' must be WAITING.
        a = wait_for(c, ids["a"], states=("RUNNING", "SUCCESS", "FAILED"))
        assert a["state"] == "RUNNING"
        assert c.get(f"/status/{ids['b']}").json()["state"] == "WAITING"
        # Cancel the running task -> dependents must not execute.
        assert c.post(f"/cancel/{ids['a']}").status_code == 200
        assert wait_for(c, ids["a"], timeout=5)["state"] == "CANCELLED"
        assert wait_for(c, ids["b"], timeout=5)["state"] == "CANCELLED"

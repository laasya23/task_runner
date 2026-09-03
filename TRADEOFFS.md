# Tradeoffs

## 1. SQLite + in-process scheduler vs. Redis/Celery

**Chosen:** SQLite plus a lightweight scheduler.

**Why:** It keeps the service easy to run, inspect, test, and deploy as a single process. The task graph and retry state survive restarts.

**Cost:** It is not a proper distributed queue. Multiple replicas require a different claiming/lease strategy, and SQLite is not suitable for high-throughput distributed workers.

## 2. Reset `RUNNING` tasks on restart vs. checkpoint/resume

**Chosen:** restart the entire task from scratch.

**Why:** Image operations are relatively coarse and restarting avoids complicated partial-state recovery. Atomic output writes prevent corrupt final files.

**Cost:** A large image operation that was 95% complete is repeated after a crash.

## 3. Priority ordering vs. strict fairness

**Chosen:** urgent-first ordering, with creation time as the tie-breaker.

**Why:** It directly satisfies the latency requirement for urgent work and is trivial to reason about.

**Cost:** A constant stream of urgent tasks can starve background work. Production systems may need aging or weighted fair scheduling.

## 4. Local filesystem paths vs. object storage

**Chosen:** task specs reference local input/output paths.

**Why:** It makes the service genuinely runnable without cloud credentials and keeps the API small.

**Cost:** Paths are a deployment/security boundary. Exposing arbitrary paths to an untrusted client would be unsafe. A production deployment should use an upload endpoint plus an allowlisted workspace or object-storage keys such as S3/Blob URLs.

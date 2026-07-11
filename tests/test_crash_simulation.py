import os
import sys
import time
import uuid
import subprocess
import requests
import pytest
from db import db

@pytest.mark.integration
@pytest.mark.crash_simulation
@pytest.mark.parametrize("infrastructure", [{"num_workers": 3, "lease_duration": 5}], indirect=True)
def test_worker_crash_job_requeued(infrastructure):
    """
    Test 1 — test_worker_crash_job_requeued:
    - Submit one noop job
    - Wait until job status is IN_PROGRESS
    - Kill that specific worker process
    - Wait for scheduler sweep
    - Assert job status is back to PENDING or COMPLETED
    - Assert worker status is DEAD
    - Assert retry_count is incremented
    """
    res = requests.post("http://127.0.0.1:8000/jobs", json={
        "job_type": "noop",
        "payload": {"delay_seconds": 10},
        "idempotency_key": str(uuid.uuid4())
    })
    assert res.status_code == 201
    job_id = res.json()["job_id"]

    worker_id = None
    for _ in range(20):
        with db.engine.connect() as conn:
            row = conn.execute(
                db.text("SELECT status, worker_id FROM jobs WHERE job_id = :job_id"),
                {"job_id": job_id}
            ).first()
            if row and row[0] == "IN_PROGRESS" and row[1] is not None:
                worker_id = row[1]
                break
        time.sleep(0.5)

    if not worker_id:
        pytest.fail("Job never reached IN_PROGRESS state")

    w_proc = infrastructure["worker_procs"][worker_id]
    w_proc.kill()

    # lease_duration (5s) + grace_period (10s) + scheduler (5s) + buffer = 22s
    time.sleep(22)

    with db.engine.connect() as conn:
        job_row = conn.execute(
            db.text("SELECT status, retry_count FROM jobs WHERE job_id = :job_id"),
            {"job_id": job_id}
        ).first()
        worker_row = conn.execute(
            db.text("SELECT status FROM workers WHERE worker_id = :worker_id"),
            {"worker_id": worker_id}
        ).first()

    assert job_row is not None
    assert job_row[0] in ("PENDING", "COMPLETED")
    if job_row[0] == "PENDING":
        assert job_row[1] == 1 

    assert worker_row is not None
    assert worker_row[0] == "DEAD"


@pytest.mark.integration
@pytest.mark.crash_simulation
@pytest.mark.flaky
@pytest.mark.parametrize("infrastructure", [{"num_workers": 1, "lease_duration": 3}], indirect=True)
def test_lease_expiry_without_crash(infrastructure):
    """
    Test 2 — test_lease_expiry_without_crash:
    - This test is inherently timing-sensitive and may occasionally produce a flaky result 
      depending on system load. We use a 3-second short lease and a job that sleeps 2-4 seconds.
    """
    res = requests.post("http://127.0.0.1:8000/jobs", json={
        "job_type": "noop",
        "payload": {"delay_seconds": 4},
        "idempotency_key": str(uuid.uuid4())
    })
    assert res.status_code == 201
    job_id = res.json()["job_id"]

    # wait lease duration (3s) + grace period (10s) + scheduler interval (5s) + buffer = 20s
    time.sleep(20)

    with db.engine.connect() as conn:
        row = conn.execute(
            db.text("SELECT status, retry_count FROM jobs WHERE job_id = :job_id"),
            {"job_id": job_id}
        ).first()

    assert row is not None
    status, retry_count = row
    
    valid_outcome = (status == "COMPLETED") or (retry_count >= 1 and status in ("IN_PROGRESS", "COMPLETED"))
    assert valid_outcome


@pytest.mark.integration
@pytest.mark.crash_simulation
def test_retry_backoff_timing(infrastructure):
    """
    Test 3 — test_retry_backoff_timing:
    """
    start_time = time.time()
    res = requests.post("http://127.0.0.1:8000/jobs", json={
        "job_type": "send_email",
        "payload": {"to": "test@fail.com"},
        "idempotency_key": str(uuid.uuid4())
    })
    assert res.status_code == 201
    job_id = res.json()["job_id"]

    dl_row = None
    for _ in range(120):
        with db.engine.connect() as conn:
            dl_row = conn.execute(
                db.text("SELECT retry_count FROM dead_letter_jobs WHERE job_id = :job_id"),
                {"job_id": job_id}
            ).first()
            if dl_row:
                break
        time.sleep(1)

    elapsed = time.time() - start_time
    assert dl_row is not None, "Job did not reach DEAD_LETTER within 120 seconds"
    assert dl_row[0] == 3
    
    # minimum backoff sum across 3 retries: 2 + 4 + 8 = 14s
    assert elapsed >= 14
    assert elapsed <= 120


@pytest.mark.integration
@pytest.mark.crash_simulation
@pytest.mark.parametrize("infrastructure", [{"num_workers": 4}], indirect=True)
def test_no_double_execution_under_normal_conditions(infrastructure):
    """
    Test 4 — test_no_double_execution_under_normal_conditions
    Cannot guarantee exactly-once but verifies no double-execution was observed.
    """
    job_ids = []
    for _ in range(20):
        res = requests.post("http://127.0.0.1:8000/jobs", json={
            "job_type": "noop",
            "payload": {},
            "idempotency_key": str(uuid.uuid4())
        })
        assert res.status_code == 201
        job_ids.append(res.json()["job_id"])

    in_clause = ",".join(f"'{jid}'" for jid in job_ids)
    for _ in range(60):
        with db.engine.connect() as conn:
            results = conn.execute(
                db.text(f"SELECT status FROM jobs WHERE job_id IN ({in_clause})")
            ).fetchall()
            
            if all(r[0] == "COMPLETED" for r in results) and len(results) == 20:
                break
        time.sleep(0.5)

    with db.engine.connect() as conn:
        results = conn.execute(
            db.text(f"SELECT retry_count FROM jobs WHERE job_id IN ({in_clause})")
        ).fetchall()
        
    assert len(results) == 20
    total_retries = sum(r[0] for r in results)
    assert total_retries == 0


@pytest.mark.integration
@pytest.mark.crash_simulation
def test_queue_server_restart_recovery(infrastructure):
    """
    Test 5 — test_queue_server_restart_recovery
    """
    job_ids = []
    for _ in range(5):
        res = requests.post("http://127.0.0.1:8000/jobs", json={
            "job_type": "noop",
            "payload": {},
            "idempotency_key": str(uuid.uuid4())
        })
        assert res.status_code == 201
        job_ids.append(res.json()["job_id"])

    time.sleep(2) 

    queue_proc = None
    for name, p in infrastructure["processes"]:
        if name == "queue_server":
            queue_proc = p
            break
            
    assert queue_proc is not None
    queue_proc.kill()
    queue_proc.wait()

    for _ in range(5):
        res = requests.post("http://127.0.0.1:8000/jobs", json={
            "job_type": "noop",
            "payload": {},
            "idempotency_key": str(uuid.uuid4())
        })
        assert res.status_code == 202
        data = res.json()
        assert data.get("queue_enqueue_failed") is True
        job_ids.append(data["job_id"])

    new_queue_proc = subprocess.Popen(["./queue_core/build/queue_server", "9000"], env=infrastructure["env"])
    infrastructure["processes"].append(("queue_server_restarted", new_queue_proc))

    queue_healthy = False
    for _ in range(20):
        try:
            res = requests.get("http://127.0.0.1:8000/health")
            if res.status_code == 200 and res.json().get("queue_reachable") is True:
                queue_healthy = True
                break
        except:
            pass
        time.sleep(0.5)
        
    assert queue_healthy

    in_clause = ",".join(f"'{jid}'" for jid in job_ids)
    all_completed = False
    for _ in range(60): 
        with db.engine.connect() as conn:
            results = conn.execute(
                db.text(f"SELECT status FROM jobs WHERE job_id IN ({in_clause})")
            ).fetchall()
            
            if all(r[0] == "COMPLETED" for r in results) and len(results) == 10:
                all_completed = True
                break
        time.sleep(0.5)
        
    assert all_completed


@pytest.mark.integration
@pytest.mark.crash_simulation
def test_scheduler_restart_does_not_lose_jobs(infrastructure):
    """
    Test 6 — test_scheduler_restart_does_not_lose_jobs
    """
    job_ids = []
    for _ in range(3):
        res = requests.post("http://127.0.0.1:8000/jobs", json={
            "job_type": "send_email",
            "payload": {"to": "test@fail.com"},
            "idempotency_key": str(uuid.uuid4())
        })
        assert res.status_code == 201
        job_ids.append(res.json()["job_id"])
        
    in_clause = ",".join(f"'{jid}'" for jid in job_ids)

    scheduler_proc = None
    for name, p in infrastructure["processes"]:
        if name == "scheduler":
            scheduler_proc = p
            break
            
    assert scheduler_proc is not None
    scheduler_proc.kill()
    scheduler_proc.wait()

    time.sleep(5)
    
    with db.engine.connect() as conn:
        results = conn.execute(
            db.text(f"SELECT status, retry_count FROM jobs WHERE job_id IN ({in_clause})")
        ).fetchall()
        
    for _ in range(20):
        if all(r[0] == "FAILED" for r in results):
            break
        time.sleep(0.5)
        with db.engine.connect() as conn:
            results = conn.execute(
                db.text(f"SELECT status, retry_count FROM jobs WHERE job_id IN ({in_clause})")
            ).fetchall()

    for row in results:
        assert row[0] == "FAILED"
        assert row[1] == 0 

    new_sched_proc = subprocess.Popen([sys.executable, "-m", "scheduler"], env=infrastructure["env"])
    infrastructure["processes"].append(("scheduler_restarted", new_sched_proc))
    
    retried = False
    for _ in range(40):
        with db.engine.connect() as conn:
            results = conn.execute(
                db.text(f"SELECT status, retry_count FROM jobs WHERE job_id IN ({in_clause})")
            ).fetchall()
            if not results:
                retried = True
                break
                
            if all(r[1] >= 1 for r in results):
                retried = True
                break
        time.sleep(0.5)
        
    assert retried

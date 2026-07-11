import os
import tempfile

temp_db_fd, temp_db_path = tempfile.mkstemp()
os.close(temp_db_fd)
os.environ["DATABASE_URL"] = f"sqlite:///{temp_db_path}"

import uuid
import json
import threading
import pytest
from sqlalchemy import text
import sys

# Ensure db can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db.db import (
    engine, init_db, insert_job, get_job, update_job_status,
    get_next_runnable_job, find_expired_leases, register_worker,
    mark_dead_workers
)

@pytest.fixture(autouse=True)
def setup_database():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dead_letter_jobs"))
        conn.execute(text("DROP TABLE IF EXISTS jobs"))
        conn.execute(text("DROP TABLE IF EXISTS workers"))
    init_db()
    yield

def test_insert_and_get_job():
    job_id = str(uuid.uuid4())
    job, is_dup = insert_job(job_id, "test_task", '{"foo": "bar"}')
    assert not is_dup
    assert job["job_id"] == job_id
    
    fetched = get_job(job_id)
    assert fetched is not None
    assert fetched["job_id"] == job_id

def test_insert_duplicate_idempotency_key():
    job_id1 = str(uuid.uuid4())
    job_id2 = str(uuid.uuid4())
    ik = "ik_123"
    
    job1, is_dup1 = insert_job(job_id1, "test", '{}', idempotency_key=ik)
    assert not is_dup1
    
    job2, is_dup2 = insert_job(job_id2, "test", '{}', idempotency_key=ik)
    assert is_dup2
    assert job2["job_id"] == job_id1 # Should return the first job

def test_invalid_status_raises():
    job_id = str(uuid.uuid4())
    insert_job(job_id, "test", '{}')
    
    with pytest.raises(ValueError, match="Invalid status"):
        update_job_status(job_id, "INVALID_STATUS")

def test_get_job_not_found():
    assert get_job(str(uuid.uuid4())) is None

def test_get_next_runnable_job_respects_time():
    # Insert pending job in the future
    job_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO jobs (job_id, job_type, payload, status, scheduled_time)
            VALUES (:id, 'test', '{}', 'PENDING', datetime(CURRENT_TIMESTAMP, '+1 hour'))
        """), {"id": job_id})
        
    job = get_next_runnable_job()
    assert job is None # None because it's scheduled in future
    
    # Insert runnable job
    job_id2 = str(uuid.uuid4())
    insert_job(job_id2, "test", '{}')
    job = get_next_runnable_job()
    assert job is not None
    assert job["job_id"] == job_id2

def test_get_next_runnable_job_concurrency():
    # Insert multiple jobs
    job_id1 = str(uuid.uuid4())
    job_id2 = str(uuid.uuid4())
    insert_job(job_id1, "test", '{}')
    insert_job(job_id2, "test", '{}')
    
    results = []
    
    def worker():
        res = get_next_runnable_job()
        if res:
            results.append(res["job_id"])
            
    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Assert both workers got different jobs
    assert len(results) == 2
    assert set(results) == {job_id1, job_id2}

def test_find_expired_leases():
    job_id = str(uuid.uuid4())
    insert_job(job_id, "test", '{}')
    
    # Update to IN_PROGRESS with expired lease
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE jobs 
            SET status = 'IN_PROGRESS', lease_expires_at = datetime(CURRENT_TIMESTAMP, '-1 hour')
            WHERE job_id = :id
        """), {"id": job_id})
        
    expired = find_expired_leases()
    assert len(expired) == 1
    assert expired[0]["job_id"] == job_id

def test_find_expired_leases_with_dead_worker():
    # Insert worker and job
    w1 = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    register_worker(w1, "token1")
    insert_job(job_id, "test", '{}')
    
    # Assign job to worker but lease is in future
    update_job_status(job_id, "IN_PROGRESS", worker_id=w1, lease_expires_at="2099-01-01 00:00:00")
    
    # Mark worker as DEAD
    with engine.begin() as conn:
        conn.execute(text("UPDATE workers SET status = 'DEAD' WHERE worker_id = :w1"), {"w1": w1})
        
    expired = find_expired_leases()
    assert len(expired) == 1
    assert expired[0]["job_id"] == job_id

def test_mark_dead_workers():
    w1 = str(uuid.uuid4())
    w2 = str(uuid.uuid4())
    register_worker(w1, "token1")
    register_worker(w2, "token2")
    
    # Update w1's heartbeat to be old
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE workers 
            SET last_heartbeat = datetime(CURRENT_TIMESTAMP, '-60 seconds')
            WHERE worker_id = :w1
        """), {"w1": w1})
        
    dead = mark_dead_workers(30)
    assert len(dead) == 1
    assert dead[0] == w1

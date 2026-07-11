import os
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///asyncflow.db")

# For SQLite, connection pooling is handled differently. We use NullPool.
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, poolclass=NullPool)
else:
    engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=5)

def init_db() -> None:
    """Initialize the database by executing schema.sql."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    with engine.begin() as conn:
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))

def insert_job(job_id: str, job_type: str, payload: str, idempotency_key: Optional[str] = None, max_retries: int = 3) -> Tuple[Dict[str, Any], bool]:
    """
    Inserts a job into the jobs table.
    Returns a tuple of (job_dict, is_duplicate).
    If idempotency_key already exists, returns the existing job and True.
    Raises ValueError if job_type is empty or payload is invalid JSON.
    """
    if not job_type or not isinstance(job_type, str):
        raise ValueError("job_type must be a non-empty string")
    
    try:
        json.loads(payload)
    except (TypeError, ValueError):
        raise ValueError("payload must be a valid JSON string")
        
    stmt = text("""
        INSERT INTO jobs (job_id, job_type, payload, idempotency_key, max_retries)
        VALUES (:job_id, :job_type, :payload, :idempotency_key, :max_retries)
        RETURNING *
    """)
    
    try:
        with engine.begin() as conn:
            result = conn.execute(stmt, {
                "job_id": job_id,
                "job_type": job_type,
                "payload": payload,
                "idempotency_key": idempotency_key,
                "max_retries": max_retries
            }).mappings().first()
            return dict(result), False
    except IntegrityError:
        if idempotency_key:
            with engine.connect() as conn:
                existing = conn.execute(
                    text("SELECT * FROM jobs WHERE idempotency_key = :ik"), 
                    {"ik": idempotency_key}
                ).mappings().first()
                if existing:
                    return dict(existing), True
        raise

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a job by ID. Returns None if not found."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM jobs WHERE job_id = :job_id"),
            {"job_id": job_id}
        ).mappings().first()
        return dict(result) if result else None

def update_job_status(job_id: str, new_status: str, result: Optional[str] = None, worker_id: Optional[str] = None, lease_expires_at: Optional[str] = None) -> bool:
    """
    Update a job's status and optional fields.
    Returns False if job_id does not exist, True otherwise.
    Raises ValueError if new_status is invalid.
    """
    valid_statuses = {'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'DEAD_LETTER'}
    if new_status not in valid_statuses:
        raise ValueError(f"Invalid status: {new_status}. Must be one of {valid_statuses}")
        
    stmt = text("""
        UPDATE jobs
        SET status = :status,
            result = coalesce(:result, result),
            worker_id = coalesce(:worker_id, worker_id),
            lease_expires_at = coalesce(:lease_expires_at, lease_expires_at),
            updated_at = CURRENT_TIMESTAMP
        WHERE job_id = :job_id
    """)
    
    with engine.begin() as conn:
        res = conn.execute(stmt, {
            "status": new_status,
            "result": result,
            "worker_id": worker_id,
            "lease_expires_at": lease_expires_at,
            "job_id": job_id
        })
        return res.rowcount > 0

def get_next_runnable_job() -> Optional[Dict[str, Any]]:
    """
    Retrieve and lock the next runnable job.
    """
    is_sqlite = engine.url.drivername == "sqlite"
    
    with engine.begin() as conn:
        if is_sqlite:
            # SQLite does not support FOR UPDATE SKIP LOCKED.
            # We use an atomic UPDATE ... RETURNING as an alternative to prevent race conditions.
            stmt = text("""
                UPDATE jobs
                SET status = 'IN_PROGRESS',
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = (
                    SELECT job_id FROM jobs 
                    WHERE status = 'PENDING' 
                      AND scheduled_time <= CURRENT_TIMESTAMP 
                    ORDER BY scheduled_time ASC 
                    LIMIT 1
                )
                RETURNING *;
            """)
            res = conn.execute(stmt).mappings().first()
            return dict(res) if res else None
        else:
            select_stmt = text("""
                SELECT job_id FROM jobs
                WHERE status = 'PENDING'
                  AND scheduled_time <= CURRENT_TIMESTAMP
                ORDER BY scheduled_time ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """)
            row = conn.execute(select_stmt).first()
            if not row:
                return None
                
            update_stmt = text("""
                UPDATE jobs
                SET status = 'IN_PROGRESS',
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = :job_id
                RETURNING *
            """)
            res = conn.execute(update_stmt, {"job_id": row[0]}).mappings().first()
            return dict(res) if res else None

def find_expired_leases() -> List[Dict[str, Any]]:
    """Return all jobs where status is 'IN_PROGRESS' and either lease_expires_at is in the past, or the assigned worker is DEAD."""
    with engine.connect() as conn:
        res = conn.execute(text("""
            SELECT j.* FROM jobs j
            LEFT JOIN workers w ON j.worker_id = w.worker_id
            WHERE j.status = 'IN_PROGRESS' 
              AND (j.lease_expires_at < CURRENT_TIMESTAMP OR w.status = 'DEAD')
        """)).mappings().all()
        return [dict(r) for r in res]

def requeue_job(job_id: str) -> bool:
    """Set job back to PENDING, clear lease and worker, increment retry count."""
    stmt = text("""
        UPDATE jobs
        SET status = 'PENDING',
            lease_expires_at = NULL,
            worker_id = NULL,
            retry_count = retry_count + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE job_id = :job_id
    """)
    with engine.begin() as conn:
        res = conn.execute(stmt, {"job_id": job_id})
        return res.rowcount > 0

def move_to_dead_letter(job_id: str, failure_reason: str) -> bool:
    """
    Move a job to the dead letter queue.
    We copy the job row into dead_letter_jobs and delete it from jobs.
    """
    with engine.begin() as conn:
        job = conn.execute(text("SELECT * FROM jobs WHERE job_id = :job_id"), {"job_id": job_id}).mappings().first()
        if not job:
            return False
            
        insert_stmt = text("""
            INSERT INTO dead_letter_jobs 
            (job_id, job_type, payload, status, idempotency_key, retry_count, max_retries, result, created_at, updated_at, scheduled_time, lease_expires_at, worker_id, failure_reason)
            VALUES 
            (:job_id, :job_type, :payload, 'DEAD_LETTER', :idempotency_key, :retry_count, :max_retries, :result, :created_at, CURRENT_TIMESTAMP, :scheduled_time, :lease_expires_at, :worker_id, :failure_reason)
        """)
        
        # Merge job dict with failure_reason
        params = dict(job)
        params["failure_reason"] = failure_reason
        conn.execute(insert_stmt, params)
        
        conn.execute(text("DELETE FROM jobs WHERE job_id = :job_id"), {"job_id": job_id})
        return True

def register_worker(worker_id: str, auth_token: str) -> Dict[str, Any]:
    """Insert or update a worker row, set status to ACTIVE, last_heartbeat to now."""
    is_sqlite = engine.url.drivername == "sqlite"
    
    with engine.begin() as conn:
        if is_sqlite:
            stmt = text("""
                INSERT INTO workers (worker_id, auth_token, status, last_heartbeat)
                VALUES (:worker_id, :auth_token, 'ACTIVE', CURRENT_TIMESTAMP)
                ON CONFLICT(worker_id) DO UPDATE SET 
                    auth_token = excluded.auth_token,
                    status = 'ACTIVE',
                    last_heartbeat = CURRENT_TIMESTAMP
                RETURNING *
            """)
        else:
            # Postgres upsert
            stmt = text("""
                INSERT INTO workers (worker_id, auth_token, status, last_heartbeat)
                VALUES (:worker_id, :auth_token, 'ACTIVE', CURRENT_TIMESTAMP)
                ON CONFLICT(worker_id) DO UPDATE SET 
                    auth_token = EXCLUDED.auth_token,
                    status = 'ACTIVE',
                    last_heartbeat = CURRENT_TIMESTAMP
                RETURNING *
            """)
        res = conn.execute(stmt, {"worker_id": worker_id, "auth_token": auth_token}).mappings().first()
        return dict(res)

def update_worker_heartbeat(worker_id: str) -> bool:
    """Update the heartbeat for a worker."""
    stmt = text("""
        UPDATE workers
        SET last_heartbeat = CURRENT_TIMESTAMP
        WHERE worker_id = :worker_id
    """)
    with engine.begin() as conn:
        res = conn.execute(stmt, {"worker_id": worker_id})
        return res.rowcount > 0

def mark_dead_workers(heartbeat_timeout_seconds: int) -> List[str]:
    """
    Find workers where last_heartbeat < now - timeout, set status to DEAD.
    Returns list of affected worker_ids.
    """
    with engine.begin() as conn:
        is_sqlite = engine.url.drivername == "sqlite"
        if is_sqlite:
            # SQLite modifier: CURRENT_TIMESTAMP, '-X seconds'
            stmt = text(f"""
                UPDATE workers
                SET status = 'DEAD'
                WHERE status = 'ACTIVE' 
                  AND last_heartbeat < datetime(CURRENT_TIMESTAMP, '-{heartbeat_timeout_seconds} seconds')
                RETURNING worker_id
            """)
            res = conn.execute(stmt).fetchall()
            return [row[0] for row in res]
        else:
            # Postgres logic
            stmt = text(f"""
                UPDATE workers
                SET status = 'DEAD'
                WHERE status = 'ACTIVE'
                  AND last_heartbeat < NOW() - INTERVAL '{heartbeat_timeout_seconds} seconds'
                RETURNING worker_id
            """)
            res = conn.execute(stmt).fetchall()
            return [row[0] for row in res]

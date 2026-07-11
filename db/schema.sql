CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    status TEXT DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE', 'DEAD')),
    auth_token TEXT NOT NULL,
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    current_job_id TEXT,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'DEAD_LETTER')),
    idempotency_key TEXT UNIQUE,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    result TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lease_expires_at TIMESTAMP,
    worker_id TEXT,
    FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'DEAD_LETTER')),
    idempotency_key TEXT UNIQUE,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    result TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lease_expires_at TIMESTAMP,
    worker_id TEXT,
    moved_to_dlq_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    failure_reason TEXT,
    FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_scheduled_time ON jobs(status, scheduled_time);
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_workers_status_last_heartbeat ON workers(status, last_heartbeat);

# AsyncFlow

AsyncFlow is a lightweight, asynchronous job queue system designed for high concurrency and robust error recovery.

## Architecture Overview

The system consists of four primary components: a C++ TCP Queue Server, a Producer API, worker processes, and a centralized Scheduler. The Producer API (written in FastAPI) exposes HTTP endpoints for job submission and status monitoring. When a job is submitted, it is persisted to a relational database (SQLite or PostgreSQL) using SQLAlchemy. The Producer then notifies the C++ Queue Server via a custom TCP protocol to immediately wake up an available worker. Workers continuously poll the C++ Queue Server for tokens; when a token is received, the worker retrieves the next pending job from the database using a lease mechanism to guarantee at-least-once delivery. The Scheduler runs as a separate background process, periodically sweeping the database to requeue expired leases, retry failed jobs with exponential backoff, move persistently failing jobs to a dead-letter queue, and reconcile any jobs that were missed by the real-time TCP notifications.

## Prerequisites

### System Requirements
- Ubuntu/Debian (Primary target platform)
- Python 3.10+
- CMake 3.10+
- g++ (supporting C++17)

### Installation

1. **Install System Dependencies**
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv cmake g++ make
```

2. **Create and Activate a Virtual Environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install Python Dependencies**
Create a `requirements.txt` in the project root if not already present, containing:
```text
fastapi==0.103.1
uvicorn==0.23.2
sqlalchemy==2.0.21
pydantic==2.4.2
requests==2.31.0
pytest==7.4.2
```
Then run:
```bash
pip install -r requirements.txt
```

4. **Compile the C++ Queue Server**
From the project root:
```bash
mkdir -p queue_core/build
cd queue_core/build
cmake ..
make
cd ../..
```

## Quick Start

Run each of these commands in a separate terminal window, ensuring the virtual environment is activated in each.

1. **Start the C++ Queue Server**
```bash
export QUEUE_HOST=127.0.0.1
export QUEUE_PORT=8080
export QUEUE_AUTH_TOKEN=supersecret_token_123
./queue_core/build/queue_server
```

2. **Start the Producer API**
```bash
export DATABASE_URL=sqlite:///asyncflow.db
export QUEUE_HOST=127.0.0.1
export QUEUE_PORT=8080
export QUEUE_AUTH_TOKEN=supersecret_token_123
python -m uvicorn producer.main:app --port 8000
```

3. **Start Workers (Repeat for N workers)**
```bash
export DATABASE_URL=sqlite:///asyncflow.db
export QUEUE_HOST=127.0.0.1
export QUEUE_PORT=8080
export QUEUE_AUTH_TOKEN=supersecret_token_123
export WORKER_ID=worker-1
python -m worker
```

4. **Start the Scheduler**
```bash
export DATABASE_URL=sqlite:///asyncflow.db
export SCHEDULER_INTERVAL=5
python -m scheduler
```

### Running the Demo
A programmatic demonstration script is provided to test the entire system end-to-end:
```bash
python -m scripts.demo --workers 4 --jobs 100 --lease-seconds 30
```

## Running Tests

- **Unit Tests Only:**
```bash
pytest tests/ -m "not integration and not crash_simulation"
```

- **Integration Tests:**
```bash
pytest tests/ -m "integration"
```

- **Crash Simulation Tests:**
*Warning: Run these in isolation as they involve intentionally killing processes and may leave dangling state if interrupted.*
```bash
pytest tests/test_crash_simulation.py -v
```

## Design Decisions

- **C++ TCP Server:** Instead of embedding the queue in Python, a dedicated C++ TCP server is used to handle high-throughput, low-latency socket connections and token distribution. This avoids Python's GIL bottlenecks for network I/O and provides a strict separation of concerns between state storage (database) and signaling (TCP).
- **At-Least-Once Delivery:** The system guarantees jobs will be processed at least once. If a worker crashes after completing the job but before updating the database, the lease will expire and the job will be retried. Exactly-once is impossible in distributed systems without two-phase commits; idempotency must be handled at the application layer.
- **Lease-based Processing:** Instead of simply deleting a job upon dequeue, jobs are marked `IN_PROGRESS` with a `lease_expires_at` timestamp. This prevents job loss if a worker crashes mid-execution.
- **Failure Handling and Retries:** When a job fails, the worker marks it as `FAILED` in the database and sends an `ACK` to the queue server to remove the lease. This transfers ownership of the retry process to the central Scheduler, which uses an exponential backoff strategy. The Scheduler periodically sweeps for retryable `FAILED` jobs, increments their retry count, schedules them for execution based on the backoff delay, and sends an `ENQUEUE` command back to the C++ server. This ensures robust distributed backoff logic and prevents tight requeue loops.
- **Separate Scheduler Process:** The Scheduler runs as an independent process rather than a thread inside a worker. This prevents duplicate sweeping logic, avoids tying scheduler health to worker stability, and allows independent scaling and deployment of the reconciliation layer.
- **SQLite vs PostgreSQL:** SQLite is used by default for ease of setup and local development. For production or high worker counts (>10), PostgreSQL is recommended to avoid database locking contention. Switching is simply a matter of changing the `DATABASE_URL` environment variable; the SQLAlchemy schema is fully compatible.

## Known Limitations

- **In-Memory Token Registry:** The queue server stores connected worker state in memory. If the C++ server restarts, workers must detect the broken pipe and re-register.
- **SQLite Write Contention:** SQLite's single-writer model can become a bottleneck under very high worker concurrency. PostgreSQL's `FOR UPDATE SKIP LOCKED` handles this significantly better at scale.
- **No Exactly-Once Guarantees:** Duplicate execution is possible at the lease boundary or during network partitions. Job handlers must be idempotent.
- **FIFO Only:** The current implementation does not support priority queues; jobs are processed strictly based on `scheduled_time`.
- **Reconciliation Sweep Cost:** The scheduler's reconciliation pass currently scans all pending jobs. At extremely high scale, this full table scan could become expensive and may require more granular indexing or batching.

## Interview Talking Points

- **At-least-once vs exactly-once tradeoff:** Why we chose at-least-once delivery to ensure no job is ever lost, and how it pushes the requirement for idempotency to the job handlers.
- **Lease mechanisms:** How leasing prevents job loss when a worker is OOM-killed, hardware fails, or network partitions occur mid-execution.
- **Exponential backoff:** Why it is critical for transient failures (like API rate limits) and how it prevents retry storms from bringing down dependent services.
- **Idempotency keys:** Their dual role in preventing duplicate submissions at the Producer level and ensuring safe retries at the worker handler level.
- **Composite indexing:** How the index on `(status, scheduled_time)` enables efficient polling and rapid identification of the next runnable job.
- **Locking strategies:** The difference between SQLite's single-writer locking and PostgreSQL's row-level `FOR UPDATE SKIP LOCKED`, and why the latter is essential for high-throughput concurrent dequeueing.
- **Dead-letter queues (DLQ):** The operational importance of a DLQ for isolating poison pills, enabling manual debugging, triggering alerts, and allowing safe replay of fixed jobs.

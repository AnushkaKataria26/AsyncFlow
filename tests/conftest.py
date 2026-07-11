import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import time
import uuid
import subprocess
import requests
import pytest
from db import db

@pytest.fixture(scope="function")
def infrastructure(request):
    params = getattr(request, "param", {})
    num_workers = params.get("num_workers", 3)
    lease_duration = params.get("lease_duration", None)

    db_path = f"asyncflow_test_{uuid.uuid4().hex}.db"
    env = os.environ.copy()
    env["DATABASE_URL"] = f"sqlite:///{db_path}"
    env["QUEUE_HOST"] = "127.0.0.1"
    env["QUEUE_PORT"] = "9000"
    
    if lease_duration is not None:
        env["LEASE_DURATION_SECONDS"] = str(lease_duration)
        env["HEARTBEAT_INTERVAL_SECONDS"] = str(max(1, lease_duration // 2))
        env["DEAD_WORKER_TIMEOUT_SECONDS"] = str(max(2, lease_duration))

    env["PYTHONPATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    db.DATABASE_URL = env["DATABASE_URL"]
    db.engine = db.create_engine(db.DATABASE_URL, poolclass=db.NullPool)
    db.init_db()

    processes = []
    
    # Start queue server
    queue_proc = subprocess.Popen(["./queue_core/build/queue_server", "9000"], env=env)
    processes.append(("queue_server", queue_proc))
    time.sleep(1)

    # Start Producer API
    producer_proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "producer.main:app", "--port", "8000"], env=env)
    processes.append(("producer", producer_proc))

    # Start Scheduler
    scheduler_proc = subprocess.Popen([sys.executable, "-m", "scheduler"], env=env)
    processes.append(("scheduler", scheduler_proc))

    # Wait for producer /health
    for _ in range(50):
        try:
            res = requests.get("http://127.0.0.1:8000/health", timeout=1)
            if res.status_code == 200:
                break
        except:
            pass
        time.sleep(0.1)
    else:
        pytest.fail("Producer API failed to become healthy within 5 seconds")

    # Start workers
    worker_pids = []
    worker_procs = {}
    for i in range(num_workers):
        w_env = env.copy()
        w_id = f"test_worker_{i}_{uuid.uuid4().hex}"
        w_env["WORKER_ID"] = w_id
        w_proc = subprocess.Popen([sys.executable, "-m", "worker"], env=w_env)
        processes.append((f"worker_{i}", w_proc))
        worker_procs[w_id] = w_proc
        worker_pids.append(w_proc.pid)

    infra = {
        "db_path": db_path,
        "producer_url": "http://127.0.0.1:8000",
        "queue_host": "127.0.0.1",
        "queue_port": 9000,
        "worker_pids": worker_pids,
        "worker_procs": worker_procs,
        "processes": processes,
        "env": env
    }

    try:
        yield infra
    finally:
        # Teardown
        for name, p in processes:
            if p.poll() is None:
                p.terminate()
                
        for name, p in processes:
            if p.poll() is None:
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()

        # Close db connections before removing file
        db.engine.dispose()
        
        if os.path.exists(db_path):
            os.remove(db_path)

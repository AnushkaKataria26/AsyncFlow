import os
import sys
import uuid
from unittest import mock
import pytest
from fastapi.testclient import TestClient

# Ensure sys.path includes the project root to import producer.main
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from producer.main import app
from db import db
from shared import queue_client

client = TestClient(app)

# Use in-memory SQLite for testing DB by default if not set
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"

@pytest.fixture(autouse=True)
def setup_db():
    db.init_db()
    # No teardown needed for in-memory, but we should clear tables if file-based
    yield
    with db.engine.begin() as conn:
        conn.execute(db.text("DELETE FROM jobs"))
        conn.execute(db.text("DELETE FROM workers"))
        conn.execute(db.text("DELETE FROM dead_letter_jobs"))

@mock.patch("shared.queue_client.send_command")
def test_create_job_success(mock_send):
    mock_send.return_value = "OK"
    response = client.post("/jobs", json={
        "job_type": "test_job",
        "payload": {"key": "value"}
    })
    assert response.status_code == 201
    data = response.json()
    assert "job_id" in data
    assert data["status"] == "PENDING"
    assert data["duplicate"] is False
    mock_send.assert_called_once()

@mock.patch("shared.queue_client.send_command")
def test_create_job_idempotency(mock_send):
    mock_send.return_value = "OK"
    ik = str(uuid.uuid4())
    req = {
        "job_type": "test_job",
        "payload": {"key": "value"},
        "idempotency_key": ik
    }
    
    # First request
    response1 = client.post("/jobs", json=req)
    assert response1.status_code == 201
    data1 = response1.json()
    assert data1["duplicate"] is False
    
    # Second request
    response2 = client.post("/jobs", json=req)
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["duplicate"] is True
    assert data1["job_id"] == data2["job_id"]

def test_create_job_invalid_type():
    response = client.post("/jobs", json={
        "job_type": "", # empty
        "payload": {}
    })
    assert response.status_code == 422

def test_create_job_invalid_retries():
    response = client.post("/jobs", json={
        "job_type": "test_job",
        "payload": {},
        "max_retries": 11 # max is 10
    })
    assert response.status_code == 422
    
    response = client.post("/jobs", json={
        "job_type": "test_job",
        "payload": {},
        "max_retries": -1
    })
    assert response.status_code == 422

@mock.patch("shared.queue_client.send_command")
def test_get_job(mock_send):
    mock_send.return_value = "OK"
    # Create first
    create_res = client.post("/jobs", json={"job_type": "test_job", "payload": {}})
    job_id = create_res.json()["job_id"]
    
    # Get
    response = client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == job_id
    assert data["job_type"] == "test_job"
    assert "lease_expires_at" not in data # Default verbose=False

    # Get verbose
    response = client.get(f"/jobs/{job_id}?verbose=true")
    assert response.status_code == 200
    assert "lease_expires_at" in response.json()

def test_get_nonexistent_job():
    response = client.get(f"/jobs/{str(uuid.uuid4())}")
    assert response.status_code == 404

@mock.patch("shared.queue_client.send_command")
def test_health_check_queue_down(mock_send):
    # Simulate queue being down
    mock_send.side_effect = queue_client.QueueUnavailableError("Down")
    
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["queue_reachable"] is False
    assert data["db_reachable"] is True

@mock.patch("shared.queue_client.send_command")
def test_queue_enqueue_failure_returns_202(mock_send):
    mock_send.side_effect = queue_client.QueueUnavailableError("Down")
    response = client.post("/jobs", json={
        "job_type": "test_job",
        "payload": {}
    })
    assert response.status_code == 202
    data = response.json()
    assert data["queue_enqueue_failed"] is True
    assert data["status"] == "PENDING"

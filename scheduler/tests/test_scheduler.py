import pytest
from unittest.mock import patch, MagicMock, call
import threading
from datetime import datetime, timedelta, timezone

from scheduler.scheduler_config import SchedulerConfig
from scheduler.scheduler import Scheduler
from shared.queue_client import QueueUnavailableError

@pytest.fixture
def config():
    return SchedulerConfig(
        scheduler_interval_seconds=0.1,
        lease_grace_period_seconds=0,
        dead_worker_timeout_seconds=30,
        max_requeue_batch_size=100,
        queue_host="localhost",
        queue_port=9000
    )

@pytest.fixture
def scheduler(config):
    return Scheduler(config)

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_expired_lease_requeued(mock_send_command, mock_db, scheduler):
    # find_expired_leases returns one job with retry_count=0, max_retries=3
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_db.find_expired_leases.return_value = [{
        "job_id": "job_1",
        "worker_id": "worker_1",
        "lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
        "retry_count": 0,
        "max_retries": 3
    }]
    mock_db.mark_dead_workers.return_value = []
    mock_db.get_retryable_failed_jobs.return_value = []
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.requeue_job.assert_called_once_with("job_1")
    mock_send_command.assert_called_once_with("ENQUEUE job_1", "localhost", 9000)
    assert scheduler.total_requeues == 1

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_expired_lease_at_max_retries_goes_to_dlq(mock_send_command, mock_db, scheduler):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_db.find_expired_leases.return_value = [{
        "job_id": "job_1",
        "worker_id": "worker_1",
        "lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
        "retry_count": 3,
        "max_retries": 3
    }]
    mock_db.mark_dead_workers.return_value = []
    mock_db.get_retryable_failed_jobs.return_value = []
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.requeue_job.assert_not_called()
    mock_send_command.assert_not_called()
    mock_db.move_to_dead_letter.assert_called_once_with("job_1", "Lease expired after max retries exhausted")
    assert scheduler.total_moved_to_dlq == 1

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_failed_job_retry_backoff_not_yet_due(mock_send_command, mock_db, scheduler):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_db.find_expired_leases.return_value = []
    mock_db.mark_dead_workers.return_value = []
    
    # updated_at 1 sec ago, retry_count=1 -> backoff is 2s, not yet due
    mock_db.get_retryable_failed_jobs.return_value = [{
        "job_id": "job_failed_1",
        "updated_at": (now - timedelta(seconds=1)).isoformat(),
        "retry_count": 1,
        "max_retries": 3
    }]
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.requeue_job.assert_not_called()
    mock_send_command.assert_not_called()

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_failed_job_retry_backoff_due(mock_send_command, mock_db, scheduler):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_db.find_expired_leases.return_value = []
    mock_db.mark_dead_workers.return_value = []
    
    # updated_at 5 secs ago, retry_count=1 -> backoff is 2s, so due
    mock_db.get_retryable_failed_jobs.return_value = [{
        "job_id": "job_failed_1",
        "updated_at": (now - timedelta(seconds=5)).isoformat(),
        "retry_count": 1,
        "max_retries": 3
    }]
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.requeue_job.assert_called_once_with("job_failed_1")
    mock_send_command.assert_called_once_with("ENQUEUE job_failed_1", "localhost", 9000)
    assert scheduler.total_retries_scheduled == 1

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_failed_job_moves_to_dlq_when_exhausted(mock_send_command, mock_db, scheduler):
    mock_db.find_expired_leases.return_value = []
    mock_db.mark_dead_workers.return_value = []
    mock_db.get_retryable_failed_jobs.return_value = []
    
    mock_db.get_dlq_candidates.return_value = [{
        "job_id": "job_dlq_1"
    }]
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.move_to_dead_letter.assert_called_once_with("job_dlq_1", "Max retries exceeded")
    assert scheduler.total_moved_to_dlq == 1

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_reconciliation_enqueues_pending_jobs(mock_send_command, mock_db, scheduler):
    mock_db.find_expired_leases.return_value = []
    mock_db.mark_dead_workers.return_value = []
    mock_db.get_retryable_failed_jobs.return_value = []
    mock_db.get_dlq_candidates.return_value = []
    
    mock_db.get_pending_jobs.return_value = [
        {"job_id": "pending_1"},
        {"job_id": "pending_2"}
    ]
    
    # Simulate queue server returning DUPLICATE for one of them
    mock_send_command.side_effect = ["OK", "DUPLICATE"]
    
    scheduler._sweep()
    
    assert mock_send_command.call_count == 2
    mock_send_command.assert_has_calls([
        call("ENQUEUE pending_1", "localhost", 9000),
        call("ENQUEUE pending_2", "localhost", 9000)
    ])

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_dead_worker_detection(mock_send_command, mock_db, scheduler):
    mock_db.find_expired_leases.return_value = []
    mock_db.mark_dead_workers.return_value = ["worker_abc"]
    mock_db.get_retryable_failed_jobs.return_value = []
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler._sweep()
    
    mock_db.mark_dead_workers.assert_called_once_with(30)
    assert scheduler.total_dead_workers_detected == 1

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_sweep_continues_after_partial_failure(mock_send_command, mock_db, scheduler):
    mock_db.mark_dead_workers.side_effect = Exception("DB connection lost")
    
    # Run the scheduler in a thread, it should log exception and wait
    # We will let it run for a tiny bit, then stop it
    
    t = threading.Thread(target=scheduler.run)
    t.start()
    
    import time
    time.sleep(0.05)
    scheduler.stop()
    t.join(timeout=1.0)
    
    assert not t.is_alive()
    # It should have called mark_dead_workers and caught the exception
    assert mock_db.mark_dead_workers.called

@patch("scheduler.scheduler.db")
@patch("scheduler.scheduler.send_command")
def test_batch_size_limit(mock_send_command, mock_db, scheduler):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    # Generate 200 jobs
    jobs = []
    for i in range(200):
        jobs.append({
            "job_id": f"job_{i}",
            "worker_id": "w1",
            "lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
            "retry_count": 0,
            "max_retries": 3
        })
    mock_db.find_expired_leases.return_value = jobs
    mock_db.mark_dead_workers.return_value = []
    mock_db.get_retryable_failed_jobs.return_value = []
    mock_db.get_dlq_candidates.return_value = []
    mock_db.get_pending_jobs.return_value = []
    
    scheduler.config.max_requeue_batch_size = 100
    
    scheduler._sweep()
    
    assert mock_db.requeue_job.call_count == 100
    assert mock_send_command.call_count == 100
    assert scheduler.total_requeues == 100
